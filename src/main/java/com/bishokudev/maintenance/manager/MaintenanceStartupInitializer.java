package com.bishokudev.maintenance.manager;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.model.MaintenanceState;
import com.bishokudev.maintenance.model.TransitionReport;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates startup readiness probe enforcement and queue consumer pausing
 * when the application is configured to boot directly into maintenance mode.
 * <p>
 * Specifically:
 * <ol>
 *   <li>On {@link ApplicationStartedEvent}, if startup maintenance is enabled,
 *       managed queue consumers (Kafka, RabbitMQ) are stopped/paused and tracked
 *       in their respective lifecycle managers for subsequent dynamic resumption.</li>
 *   <li>On {@link AvailabilityChangeEvent} with {@link ReadinessState#ACCEPTING_TRAFFIC},
 *       intercepts the event and immediately calls {@link KubernetesReadinessManager#refuseTraffic()}
 *       to keep Kubernetes readiness probes unready (HTTP 503 / OUT_OF_SERVICE) from boot.</li>
 *   <li>Distinguishes between cold boot active maintenance and dynamic exit via Actuator:
 *       when the phase is {@code "EXITING"} or {@code "INACTIVE"}, {@code ACCEPTING_TRAFFIC} is
 *       permitted to stand so that dynamic exit cleanly restores readiness to 200 OK.</li>
 *   <li>Enforces zero drain delay on boot by directly publishing {@link ReadinessState#REFUSING_TRAFFIC}
 *       without calling {@link TrafficDrainHandler#refuseTrafficAndDrain(java.time.Duration)}.</li>
 * </ol>
 */
public class MaintenanceStartupInitializer implements ApplicationListener<ApplicationEvent>, Ordered {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceStartupInitializer.class);

    private final MaintenanceProperties properties;
    private final MaintenanceState state;
    private final KubernetesReadinessManager readinessManager;
    private final QueueMaintenanceManager queueManager;
    private final @Nullable ApplicationEventPublisher eventPublisher;
    private final AtomicBoolean queuesPausedOnStartup = new AtomicBoolean(false);
    private final AtomicBoolean startupEventPublished = new AtomicBoolean(false);
    private volatile Map<String, Object> startupQueueReport = Map.of();

    /**
     * Creates a new initializer with default null event publisher.
     *
     * @param properties       maintenance configuration properties
     * @param state            maintenance state model
     * @param readinessManager Kubernetes readiness probe manager
     * @param queueManager     queue consumer lifecycle manager
     */
    public MaintenanceStartupInitializer(
            MaintenanceProperties properties,
            MaintenanceState state,
            KubernetesReadinessManager readinessManager,
            QueueMaintenanceManager queueManager) {
        this(properties, state, readinessManager, queueManager, null);
    }

    /**
     * Creates a new initializer with full dependencies.
     *
     * @param properties       maintenance configuration properties
     * @param state            maintenance state model
     * @param readinessManager Kubernetes readiness probe manager
     * @param queueManager     queue consumer lifecycle manager
     * @param eventPublisher   optional Spring application event publisher
     */
    public MaintenanceStartupInitializer(
            MaintenanceProperties properties,
            MaintenanceState state,
            KubernetesReadinessManager readinessManager,
            QueueMaintenanceManager queueManager,
            @Nullable ApplicationEventPublisher eventPublisher) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.state = Objects.requireNonNull(state, "state must not be null");
        this.readinessManager = Objects.requireNonNull(readinessManager, "readinessManager must not be null");
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ApplicationStartedEvent) {
            onApplicationStarted();
        } else if (event instanceof ApplicationReadyEvent) {
            onApplicationReady();
        } else if (event instanceof AvailabilityChangeEvent<?> availabilityEvent) {
            onAvailabilityChange(availabilityEvent);
        }
    }

    /**
     * Executes immediately upon context refresh completion, before command-line/application runners.
     */
    void onApplicationStarted() {
        if (!properties.isStartInMaintenance()) {
            return;
        }
        String phase = state.getPhase();
        if ("EXITING".equals(phase) || "INACTIVE".equals(phase)) {
            log.debug("Skipping onApplicationStarted: maintenance phase is [{}]", phase);
            return;
        }
        if (!state.isMaintenanceActive() && !"FAILED".equals(phase)) {
            return;
        }
        pauseQueuesOnStartup();
        publishStartupEventOnStartup();
        enforceReadinessRefusal();
    }

    /**
     * Fallback execution in case {@link ApplicationStartedEvent} was skipped or not fired.
     */
    void onApplicationReady() {
        if (!properties.isStartInMaintenance()) {
            return;
        }
        String phase = state.getPhase();
        if ("EXITING".equals(phase) || "INACTIVE".equals(phase)) {
            log.debug("Skipping onApplicationReady: maintenance phase is [{}]", phase);
            return;
        }
        if (!state.isMaintenanceActive() && !"FAILED".equals(phase)) {
            return;
        }
        pauseQueuesOnStartup();
        publishStartupEventOnStartup();
        enforceReadinessRefusal();
    }

    /**
     * Intercepts availability change events to enforce readiness refusal while maintenance mode is active.
     */
    void onAvailabilityChange(AvailabilityChangeEvent<?> event) {
        if (event.getState() == ReadinessState.ACCEPTING_TRAFFIC) {
            String phase = state.getPhase();
            // Crucial edge case: do NOT intercept or re-refuse traffic when exiting or inactive!
            if ("EXITING".equals(phase) || "INACTIVE".equals(phase)) {
                log.debug("Readiness ACCEPTING_TRAFFIC permitted: maintenance phase is [{}]", phase);
                return;
            }
            if (state.isMaintenanceActive() || "FAILED".equals(phase)) {
                log.info("Intercepted ACCEPTING_TRAFFIC while maintenance phase is [{}]: re-enforcing REFUSING_TRAFFIC", phase);
                enforceReadinessRefusal();
            }
        }
    }

    /**
     * Pauses Kafka listener containers and stops RabbitMQ listener containers, populating their
     * {@code owned} collection so that Actuator exit can resume them later.
     */
    private void pauseQueuesOnStartup() {
        if (!properties.getQueues().isEnabled()) {
            log.info("Queue maintenance is disabled by configuration; skipping startup queue pausing");
            return;
        }
        if (queuesPausedOnStartup.compareAndSet(false, true)) {
            log.info("Application starting in maintenance mode: pausing message listener containers");
            Map<String, Object> queueReport;
            try {
                queueReport = queueManager.stopConsumers();
                this.startupQueueReport = queueReport;
                log.info("Startup queue pausing completed: {}", queueReport);
            } catch (Exception ex) {
                log.error("Failed to pause message consumers on startup", ex);
                queueReport = Map.of("error", ex.toString());
                this.startupQueueReport = queueReport;
            }
        }
    }

    /**
     * Publishes startup {@link MaintenanceModeChangedEvent} once, decoupled from queue pausing.
     */
    private void publishStartupEventOnStartup() {
        if (!properties.getEvents().isEnabled() || eventPublisher == null) {
            return;
        }
        if (startupEventPublished.compareAndSet(false, true)) {
            Map<String, Object> queueReport;
            if (!properties.getQueues().isEnabled()) {
                queueReport = Map.of("enabled", false);
            } else if (startupQueueReport != null && !startupQueueReport.isEmpty()) {
                queueReport = startupQueueReport;
            } else {
                queueReport = Map.of("status", "queues_enabled");
            }
            publishStartupEvent(queueReport);
        }
    }

    /**
     * Sets Kubernetes readiness state to REFUSING_TRAFFIC with zero drain delay.
     */
    private void enforceReadinessRefusal() {
        if (!properties.getReadiness().isEnabled()) {
            log.debug("Readiness management is disabled by configuration");
            return;
        }
        try {
            readinessManager.refuseTraffic();
        } catch (Exception ex) {
            log.error("Failed to enforce REFUSING_TRAFFIC readiness state on startup", ex);
        }
    }

    /**
     * Publishes {@link MaintenanceModeChangedEvent} if event publishing is enabled.
     */
    private void publishStartupEvent(Map<String, Object> queueReport) {
        if (eventPublisher != null && properties.getEvents().isEnabled()) {
            try {
                TransitionReport report = TransitionReport.builder()
                        .readiness(properties.getReadiness().isEnabled() ? "REFUSING_TRAFFIC" : "DISABLED")
                        .queues(queueReport)
                        .build();
                eventPublisher.publishEvent(new MaintenanceModeChangedEvent(
                        this, true, state.getLastChanged(), state.getReason(), report));
            } catch (Exception ex) {
                log.error("Failed to publish startup MaintenanceModeChangedEvent", ex);
            }
        }
    }

    /**
     * Returns {@code true} if message queues were paused on startup.
     *
     * @return {@code true} if queues were paused on startup
     */
    public boolean isQueuesPausedOnStartup() {
        return queuesPausedOnStartup.get();
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
