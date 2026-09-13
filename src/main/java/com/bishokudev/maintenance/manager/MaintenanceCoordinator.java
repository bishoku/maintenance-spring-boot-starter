package com.bishokudev.maintenance.manager;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Orchestrates the maintenance mode transition workflows.
 * <p>
 * <strong>Entering Maintenance:</strong>
 * <ol>
 *   <li>Idempotency check: if already active, no-op.</li>
 *   <li>Kubernetes Readiness &rarr; {@code REFUSING_TRAFFIC}.</li>
 *   <li>Drain delay (configurable) to allow Kubernetes to update endpoints.</li>
 *   <li>Pause Queue Listeners (Kafka &amp; RabbitMQ).</li>
 *   <li>Invoke {@link MaintenanceHook}s (ordered, with timeout).</li>
 *   <li>Update {@link MaintenanceState}.</li>
 *   <li>Publish {@link MaintenanceModeChangedEvent}.</li>
 * </ol>
 * <p>
 * <strong>Exiting Maintenance:</strong>
 * <ol>
 *   <li>Idempotency check: if already inactive, no-op.</li>
 *   <li>Resume Queue Listeners.</li>
 *   <li>Invoke {@link MaintenanceHook}s (ordered, with timeout).</li>
 *   <li>Kubernetes Readiness &rarr; {@code ACCEPTING_TRAFFIC}.</li>
 *   <li>Update {@link MaintenanceState}.</li>
 *   <li>Publish {@link MaintenanceModeChangedEvent}.</li>
 * </ol>
 */
public class MaintenanceCoordinator {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceCoordinator.class);

    private final MaintenanceState state;
    private final KubernetesReadinessManager readinessManager;
    private final QueueMaintenanceManager queueManager;
    private final ApplicationEventPublisher eventPublisher;
    private final List<MaintenanceHook> hooks;
    private final Duration drainDelay;
    private final Duration hookTimeout;
    private final ExecutorService hookExecutor;

    public MaintenanceCoordinator(MaintenanceState state,
                                  KubernetesReadinessManager readinessManager,
                                  QueueMaintenanceManager queueManager,
                                  ApplicationEventPublisher eventPublisher,
                                  List<MaintenanceHook> hooks,
                                  MaintenanceProperties properties) {
        this.state = Objects.requireNonNull(state, "state must not be null");
        this.readinessManager = Objects.requireNonNull(readinessManager, "readinessManager must not be null");
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
        Objects.requireNonNull(properties, "properties must not be null");

        List<MaintenanceHook> sortedHooks = hooks != null ? new ArrayList<>(hooks) : new ArrayList<>();
        AnnotationAwareOrderComparator.sort(sortedHooks);
        this.hooks = Collections.unmodifiableList(sortedHooks);

        this.drainDelay = properties.getDrainDelay();
        this.hookTimeout = properties.getHookTimeout();
        this.hookExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "maintenance-hook-executor");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Executes the maintenance transition.
     *
     * @param enable target maintenance state
     * @param reason optional reason for the change
     * @return true if state changed; false if already in the requested state (idempotent)
     */
    public synchronized boolean setMaintenanceMode(boolean enable, String reason) {
        if (enable) {
            return enterMaintenance(reason);
        } else {
            return exitMaintenance(reason);
        }
    }

    private boolean enterMaintenance(String reason) {
        if (state.isMaintenanceActive()) {
            log.info("Application is already in maintenance mode. Ignoring request (idempotent).");
            return false;
        }

        log.info("Entering maintenance mode. Reason: '{}'", reason);
        Map<String, Object> details = new LinkedHashMap<>();

        // 1. Kubernetes Readiness: refuse traffic first
        try {
            readinessManager.refuseTraffic();
            details.put("readiness", "REFUSING_TRAFFIC");
        } catch (Exception ex) {
            log.error("Failed to transition Kubernetes readiness to REFUSING_TRAFFIC", ex);
            details.put("readinessError", ex.getMessage());
        }

        // 2. Drain delay: allow K8s to update endpoints
        if (drainDelay != null && !drainDelay.isZero() && !drainDelay.isNegative()) {
            try {
                log.info("Waiting {}ms for Kubernetes endpoint drain...", drainDelay.toMillis());
                Thread.sleep(drainDelay.toMillis());
                details.put("drainDelayMs", drainDelay.toMillis());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.warn("Drain delay interrupted", ex);
                details.put("drainDelayInterrupted", true);
            }
        }

        // 3. Stop queue listeners
        try {
            Map<String, Object> queueReport = queueManager.stopConsumers();
            details.put("queues", queueReport);
        } catch (Exception ex) {
            log.error("Failed to stop queue listeners", ex);
            details.put("queueError", ex.getMessage());
        }

        // 4. Execute hooks with timeout
        Map<String, Object> hookReport = executeHooks(true);
        details.put("hooks", hookReport);

        // 5. Update state atomically
        state.transition(true, reason, details);
        Instant transitionTime = state.getLastChanged();

        // 6. Publish domain event
        publishEvent(true, transitionTime, reason, details);

        log.info("Successfully entered maintenance mode.");
        return true;
    }

    private boolean exitMaintenance(String reason) {
        if (!state.isMaintenanceActive()) {
            log.info("Application is not in maintenance mode. Ignoring request (idempotent).");
            return false;
        }

        log.info("Exiting maintenance mode. Reason: '{}'", reason);
        Map<String, Object> details = new LinkedHashMap<>();

        // 1. Resume queue listeners
        try {
            Map<String, Object> queueReport = queueManager.startConsumers();
            details.put("queues", queueReport);
        } catch (Exception ex) {
            log.error("Failed to start queue listeners", ex);
            details.put("queueError", ex.getMessage());
        }

        // 2. Execute hooks with timeout
        Map<String, Object> hookReport = executeHooks(false);
        details.put("hooks", hookReport);

        // 3. Kubernetes Readiness: accept traffic
        try {
            readinessManager.acceptTraffic();
            details.put("readiness", "ACCEPTING_TRAFFIC");
        } catch (Exception ex) {
            log.error("Failed to transition Kubernetes readiness to ACCEPTING_TRAFFIC", ex);
            details.put("readinessError", ex.getMessage());
        }

        // 4. Update state atomically
        state.transition(false, reason, details);
        Instant transitionTime = state.getLastChanged();

        // 5. Publish domain event
        publishEvent(false, transitionTime, reason, details);

        log.info("Successfully exited maintenance mode.");
        return true;
    }

    /**
     * Executes all hooks with a per-hook timeout. Returns a detailed report.
     */
    private Map<String, Object> executeHooks(boolean entering) {
        Map<String, Object> report = new LinkedHashMap<>();
        int succeeded = 0;
        int failed = 0;
        int timedOut = 0;
        List<String> failures = new ArrayList<>();

        for (MaintenanceHook hook : hooks) {
            String hookName = hook.getClass().getName();
            try {
                Future<?> future = hookExecutor.submit(() -> {
                    if (entering) {
                        hook.onEnterMaintenance();
                    } else {
                        hook.onExitMaintenance();
                    }
                });

                long timeoutMs = hookTimeout != null ? hookTimeout.toMillis() : 30_000L;
                future.get(timeoutMs, TimeUnit.MILLISECONDS);
                succeeded++;
            } catch (TimeoutException ex) {
                timedOut++;
                String msg = String.format("Hook [%s] timed out after %dms", hookName, hookTimeout.toMillis());
                log.error(msg);
                failures.add(msg);
            } catch (Exception ex) {
                failed++;
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                String msg = String.format("Hook [%s] failed: %s", hookName, cause.getMessage());
                log.error("Error executing MaintenanceHook [{}]", hookName, cause);
                failures.add(msg);
            }
        }

        report.put("total", hooks.size());
        report.put("succeeded", succeeded);
        report.put("failed", failed);
        report.put("timedOut", timedOut);
        if (!failures.isEmpty()) {
            report.put("partialFailure", true);
            report.put("failures", List.copyOf(failures));
        }
        return report;
    }

    private void publishEvent(boolean entering, Instant timestamp, String reason, Map<String, Object> details) {
        try {
            eventPublisher.publishEvent(
                    new MaintenanceModeChangedEvent(this, entering, timestamp, reason, details));
        } catch (Exception ex) {
            log.error("Failed to publish MaintenanceModeChangedEvent", ex);
        }
    }

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
        return hooks;
    }
}
