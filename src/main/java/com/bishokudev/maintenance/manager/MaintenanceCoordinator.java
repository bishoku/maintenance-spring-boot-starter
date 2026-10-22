package com.bishokudev.maintenance.manager;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.model.MaintenanceState;
import com.bishokudev.maintenance.model.TransitionReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Serializes pod-local transitions. Critical failure retains readiness refusal where possible and permits retry. */
public class MaintenanceCoordinator {
    private static final Logger log = LoggerFactory.getLogger(MaintenanceCoordinator.class);
    private final MaintenanceState state;
    private final TrafficDrainHandler drainHandler;
    private final QueueMaintenanceManager queueManager;
    private final MaintenanceHookExecutor hookExecutor;
    private final ApplicationEventPublisher eventPublisher;
    private final MaintenanceProperties properties;
    private boolean transitioning;

    public MaintenanceCoordinator(MaintenanceState state, TrafficDrainHandler drainHandler,
            QueueMaintenanceManager queueManager, MaintenanceHookExecutor hookExecutor,
            ApplicationEventPublisher eventPublisher, MaintenanceProperties properties) {
        this.state = Objects.requireNonNull(state);
        this.drainHandler = Objects.requireNonNull(drainHandler);
        this.queueManager = Objects.requireNonNull(queueManager);
        this.hookExecutor = Objects.requireNonNull(hookExecutor);
        this.eventPublisher = Objects.requireNonNull(eventPublisher);
        this.properties = Objects.requireNonNull(properties);
    }

    /** Returns true only on a completed transition; inspect phase/details for failure or no-op. */
    public synchronized boolean setMaintenanceMode(boolean enable, String reason) {
        if (transitioning) throw new IllegalStateException("Reentrant maintenance transition");
        if ((enable && state.getPhase().equals("ACTIVE")) || (!enable && state.getPhase().equals("INACTIVE"))) return false;
        transitioning = true;
        boolean previouslyActive = state.isMaintenanceActive();
        long started = System.nanoTime();
        TransitionReport.Builder report = TransitionReport.builder();
        state.update(previouslyActive, enable ? "ENTERING" : "EXITING", reason, Map.of());
        boolean success = false;
        try {
            if (!hookExecutor.isIdle()) throw new IllegalStateException("Previous hook still executing; retry after it terminates");
            if (enable) {
                readiness(report, true);
                requireSuccess(report);
                queues(report, true);
                requireSuccess(report);
                hooks(report, true);
                requireSuccess(report);
            } else {
                // Preparation finishes before consumers resume and readiness is restored.
                hooks(report, false);
                requireSuccess(report);
                queues(report, false);
                requireSuccess(report);
                readiness(report, false);
                requireSuccess(report);
            }
            success = true;
        } catch (Exception ex) {
            if (!report.build().partialFailure()) report.readinessError(ex.toString());
            log.warn("Maintenance {} failed; readiness acceptance is withheld", enable ? "entry" : "exit", ex);
            if (!enable) {
                report.queueError("Exit failed: " + ex);
                // Best-effort compensation if some consumers resumed before another step failed.
                if (properties.getQueues().isEnabled()) {
                    try {
                        Map<String, Object> outcomes = new LinkedHashMap<>();
                        if (report.build().queues() != null) outcomes.putAll(report.build().queues());
                        outcomes.put("compensation", queueManager.stopConsumers());
                        report.queues(outcomes);
                    }
                    catch (Exception compensation) { report.queueError(compensation.toString()); }
                }
                if (properties.getReadiness().isEnabled()) drainHandler.refuseTrafficAndDrain(java.time.Duration.ZERO);
            }
        } finally {
            report.transitionDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            TransitionReport completed = report.build();
            state.update(success ? enable : previouslyActive, success ? (enable ? "ACTIVE" : "INACTIVE") : "FAILED",
                    reason, completed.toMap());
            if (properties.getEvents().isEnabled()) {
                try { eventPublisher.publishEvent(new MaintenanceModeChangedEvent(this, enable, state.getLastChanged(), reason, completed)); }
                catch (Exception ex) { log.error("Maintenance event listener failed", ex); }
            }
            transitioning = false;
        }
        return success;
    }
    private static void requireSuccess(TransitionReport.Builder report) {
        if (report.build().partialFailure()) throw new IllegalStateException("Critical maintenance step failed");
    }
    private void readiness(TransitionReport.Builder report, boolean enter) {
        if (!properties.getReadiness().isEnabled()) { report.readiness("DISABLED"); return; }
        TrafficDrainHandler.Result result = enter ? drainHandler.refuseTrafficAndDrain(properties.getDrainDelay()) : drainHandler.acceptTraffic();
        report.readiness(result.readiness()).readinessError(result.error()).drainDelayInterrupted(result.interrupted());
        if (result.drainDelayMs() != null) report.drainDelayMs(result.drainDelayMs());
    }
    private void queues(TransitionReport.Builder report, boolean enter) {
        if (!properties.getQueues().isEnabled()) { report.queues(Map.of("enabled", false)); return; }
        try { report.queues(enter ? queueManager.stopConsumers() : queueManager.startConsumers()); }
        catch (Exception ex) { report.queueError(ex.toString()); }
    }
    private void hooks(TransitionReport.Builder report, boolean enter) {
        if (properties.getHooks().isEnabled()) report.hooks(hookExecutor.execute(enter));
    }
    public MaintenanceState getState() { return state; }
    public List<MaintenanceHook> getHooks() { return hookExecutor.getHooks(); }
    public MaintenanceProperties getProperties() { return properties; }
}
