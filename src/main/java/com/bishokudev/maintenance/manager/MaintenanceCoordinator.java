package com.bishokudev.maintenance.manager;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.model.HookExecutionReport;
import com.bishokudev.maintenance.model.MaintenanceState;
import com.bishokudev.maintenance.model.TransitionReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates the maintenance mode transition workflows.
 * <p>
 * This class is a <em>pure orchestrator</em>: it delegates low-level concerns to
 * dedicated single-responsibility components:
 * <ul>
 *   <li>{@link TrafficDrainHandler} — Kubernetes readiness transitions and drain delay</li>
 *   <li>{@link MaintenanceHookExecutor} — ordered hook execution with per-hook timeout</li>
 *   <li>{@link QueueMaintenanceManager} — message broker consumer lifecycle</li>
 * </ul>
 * <p>
 * Individual operations (Readiness, Queues, Hooks, Events) can be toggled via
 * {@link MaintenanceProperties}.
 * <p>
 * <strong>Entering Maintenance:</strong>
 * <ol>
 *   <li>Idempotency check: if already active, no-op.</li>
 *   <li>Kubernetes Readiness &rarr; {@code REFUSING_TRAFFIC} + drain delay (if enabled).</li>
 *   <li>Pause Queue Listeners (Kafka &amp; RabbitMQ) (if enabled).</li>
 *   <li>Invoke {@link MaintenanceHook}s (ordered, with timeout) (if enabled).</li>
 *   <li>Update {@link MaintenanceState}.</li>
 *   <li>Publish {@link MaintenanceModeChangedEvent} (if enabled).</li>
 * </ol>
 * <p>
 * <strong>Exiting Maintenance:</strong>
 * <ol>
 *   <li>Idempotency check: if already inactive, no-op.</li>
 *   <li>Resume Queue Listeners (if enabled).</li>
 *   <li>Invoke {@link MaintenanceHook}s (ordered, with timeout) (if enabled).</li>
 *   <li>Kubernetes Readiness &rarr; {@code ACCEPTING_TRAFFIC} (if enabled).</li>
 *   <li>Update {@link MaintenanceState}.</li>
 *   <li>Publish {@link MaintenanceModeChangedEvent} (if enabled).</li>
 * </ol>
 */
public class MaintenanceCoordinator {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceCoordinator.class);

    private final MaintenanceState state;
    private final TrafficDrainHandler drainHandler;
    private final QueueMaintenanceManager queueManager;
    private final MaintenanceHookExecutor hookExecutor;
    private final ApplicationEventPublisher eventPublisher;
    private final MaintenanceProperties properties;

    public MaintenanceCoordinator(MaintenanceState state,
                                  TrafficDrainHandler drainHandler,
                                  QueueMaintenanceManager queueManager,
                                  MaintenanceHookExecutor hookExecutor,
                                  ApplicationEventPublisher eventPublisher,
                                  MaintenanceProperties properties) {
        this.state = Objects.requireNonNull(state, "state must not be null");
        this.drainHandler = Objects.requireNonNull(drainHandler, "drainHandler must not be null");
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.hookExecutor = Objects.requireNonNull(hookExecutor, "hookExecutor must not be null");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    /**
     * Executes the maintenance transition.
     *
     * @param enable target maintenance state
     * @param reason optional reason for the change
     * @return true if state changed; false if already in the requested state (idempotent)
     */
    public synchronized boolean setMaintenanceMode(boolean enable, String reason) {
        return enable ? enterMaintenance(reason) : exitMaintenance(reason);
    }

    private boolean enterMaintenance(String reason) {
        if (state.isMaintenanceActive()) {
            log.info("Application is already in maintenance mode. Ignoring request (idempotent).");
            return false;
        }

        log.info("Entering maintenance mode. Reason: '{}'", reason);
        long startNanos = System.nanoTime();
        TransitionReport.Builder report = TransitionReport.builder();

        // 1. Kubernetes Readiness: refuse traffic + drain delay (if enabled)
        applyReadiness(report, true);

        // 2. Stop queue listeners (if enabled)
        applyQueues(report, true);

        // 3. Execute hooks (if enabled)
        applyHooks(report, true);

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        report.transitionDurationMs(durationMs);

        TransitionReport transitionReport = report.build();
        finalizeTransition(true, reason, transitionReport);

        log.info("Successfully entered maintenance mode (took {}ms).", durationMs);
        return true;
    }

    private boolean exitMaintenance(String reason) {
        if (!state.isMaintenanceActive()) {
            log.info("Application is not in maintenance mode. Ignoring request (idempotent).");
            return false;
        }

        log.info("Exiting maintenance mode. Reason: '{}'", reason);
        long startNanos = System.nanoTime();
        TransitionReport.Builder report = TransitionReport.builder();

        // 1. Resume queue listeners (if enabled)
        applyQueues(report, false);

        // 2. Execute hooks (if enabled)
        applyHooks(report, false);

        // 3. Kubernetes Readiness: accept traffic (if enabled)
        applyReadiness(report, false);

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        report.transitionDurationMs(durationMs);

        TransitionReport transitionReport = report.build();
        finalizeTransition(false, reason, transitionReport);

        log.info("Successfully exited maintenance mode (took {}ms).", durationMs);
        return true;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Step delegates (single level of abstraction)
    // ──────────────────────────────────────────────────────────────────────

    private void applyReadiness(TransitionReport.Builder report, boolean entering) {
        if (!properties.getReadiness().isEnabled()) {
            log.debug("Kubernetes readiness transition is disabled via configuration.");
            report.readiness("DISABLED");
            return;
        }

        if (entering) {
            TrafficDrainHandler.Result result = drainHandler.refuseTrafficAndDrain(properties.getDrainDelay());
            report.readiness(result.readiness())
                    .readinessError(result.error());
            if (result.drainDelayMs() != null) {
                report.drainDelayMs(result.drainDelayMs());
            }
            report.drainDelayInterrupted(result.interrupted());
        } else {
            TrafficDrainHandler.Result result = drainHandler.acceptTraffic();
            report.readiness(result.readiness())
                    .readinessError(result.error());
        }
    }

    private void applyQueues(TransitionReport.Builder report, boolean entering) {
        if (!properties.getQueues().isEnabled()) {
            log.debug("Queue listener {} is disabled via configuration.", entering ? "pause" : "resume");
            report.queues(Map.of("enabled", false));
            return;
        }

        try {
            Map<String, Object> queueReport = entering
                    ? queueManager.stopConsumers()
                    : queueManager.startConsumers();
            report.queues(queueReport);
        } catch (Exception ex) {
            log.error("Failed to {} queue listeners", entering ? "stop" : "start", ex);
            report.queueError(ex.getMessage());
        }
    }

    private void applyHooks(TransitionReport.Builder report, boolean entering) {
        if (!properties.getHooks().isEnabled()) {
            log.debug("MaintenanceHook execution is disabled via configuration.");
            report.hooks(null);
            return;
        }

        HookExecutionReport hookReport = hookExecutor.execute(entering);
        report.hooks(hookReport);
    }

    private void finalizeTransition(boolean entering, String reason, TransitionReport transitionReport) {
        Map<String, Object> detailsMap = transitionReport.toMap();
        state.transition(entering, reason, detailsMap);
        Instant transitionTime = state.getLastChanged();

        if (properties.getEvents().isEnabled()) {
            publishEvent(entering, transitionTime, reason, transitionReport);
        } else {
            log.debug("MaintenanceModeChangedEvent publishing is disabled via configuration.");
        }
    }

    private void publishEvent(boolean entering, Instant timestamp, String reason,
                              TransitionReport transitionReport) {
        try {
            eventPublisher.publishEvent(
                    new MaintenanceModeChangedEvent(this, entering, timestamp, reason, transitionReport));
        } catch (Exception ex) {
            log.error("Failed to publish MaintenanceModeChangedEvent", ex);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Public accessors
    // ──────────────────────────────────────────────────────────────────────

    /**
     * @return the current maintenance state bean
     */
    public MaintenanceState getState() {
        return state;
    }

    /**
     * @return unmodifiable, ordered list of registered hooks
     */
    public List<MaintenanceHook> getHooks() {
        return hookExecutor.getHooks();
    }

    /**
     * @return the current maintenance properties configuration
     */
    public MaintenanceProperties getProperties() {
        return properties;
    }
}
