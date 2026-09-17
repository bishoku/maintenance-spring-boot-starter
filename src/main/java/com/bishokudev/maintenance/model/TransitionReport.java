package com.bishokudev.maintenance.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable, type-safe report capturing the full details of a maintenance mode transition.
 * <p>
 * Replaces the legacy hand-built {@code Map<String, Object>} that previously flowed
 * through the coordinator, event, and metrics systems. Provides compile-time safety
 * for all consumers and a backward-compatible {@link #toMap()} for JSON serialization.
 *
 * @param readiness             readiness state description (e.g. "REFUSING_TRAFFIC", "ACCEPTING_TRAFFIC", "DISABLED")
 * @param readinessError        error message if readiness transition failed; {@code null} otherwise
 * @param drainDelayMs          drain delay applied in milliseconds; {@code null} if not applicable
 * @param drainDelayInterrupted {@code true} if the drain delay sleep was interrupted
 * @param queues                queue manager report (from {@code QueueMaintenanceManager}); {@code null} if disabled
 * @param queueError            error message if queue start/stop failed; {@code null} otherwise
 * @param hooks                 hook execution report; {@code null} if hooks are disabled
 * @param transitionDurationMs  total wall-clock duration of the transition in milliseconds
 */
public record TransitionReport(
        String readiness,
        String readinessError,
        Long drainDelayMs,
        boolean drainDelayInterrupted,
        Map<String, Object> queues,
        String queueError,
        HookExecutionReport hooks,
        long transitionDurationMs
) {
    public TransitionReport {
        queues = queues != null ? Map.copyOf(queues) : null;
    }

    /**
     * Returns {@code true} if any component experienced a failure during the transition.
     */
    public boolean partialFailure() {
        return readinessError != null
                || queueError != null
                || (hooks != null && hooks.partialFailure());
    }

    /**
     * Converts this report to a {@code Map<String, Object>} structure identical to the
     * legacy hand-built map, ensuring backward-compatible JSON serialization for
     * {@link com.bishokudev.maintenance.actuator.MaintenanceStatusResponse} and
     * {@link com.bishokudev.maintenance.event.MaintenanceModeChangedEvent}.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();

        // Readiness
        if (readinessError != null) {
            map.put("readinessError", readinessError);
        }
        if (readiness != null) {
            map.put("readiness", readiness);
        }

        // Drain delay
        if (drainDelayMs != null) {
            map.put("drainDelayMs", drainDelayMs);
        }
        if (drainDelayInterrupted) {
            map.put("drainDelayInterrupted", true);
        }

        // Queues
        if (queues != null) {
            map.put("queues", queues);
        }
        if (queueError != null) {
            map.put("queueError", queueError);
        }

        // Hooks
        if (hooks != null) {
            map.put("hooks", hooks.toMap());
        }

        // Transition duration
        map.put("transitionDurationMs", transitionDurationMs);

        return Collections.unmodifiableMap(map);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Builder for step-by-step construction during transition workflow
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Mutable builder for constructing a {@code TransitionReport} step by step
     * during the transition workflow.
     */
    public static class Builder {
        private String readiness;
        private String readinessError;
        private Long drainDelayMs;
        private boolean drainDelayInterrupted;
        private Map<String, Object> queues;
        private String queueError;
        private HookExecutionReport hooks;
        private long transitionDurationMs;

        public Builder readiness(String readiness) {
            this.readiness = readiness;
            return this;
        }

        public Builder readinessError(String readinessError) {
            this.readinessError = readinessError;
            return this;
        }

        public Builder drainDelayMs(long drainDelayMs) {
            this.drainDelayMs = drainDelayMs;
            return this;
        }

        public Builder drainDelayInterrupted(boolean interrupted) {
            this.drainDelayInterrupted = interrupted;
            return this;
        }

        public Builder queues(Map<String, Object> queues) {
            this.queues = queues;
            return this;
        }

        public Builder queueError(String queueError) {
            this.queueError = queueError;
            return this;
        }

        public Builder hooks(HookExecutionReport hooks) {
            this.hooks = hooks;
            return this;
        }

        public Builder transitionDurationMs(long transitionDurationMs) {
            this.transitionDurationMs = transitionDurationMs;
            return this;
        }

        public TransitionReport build() {
            return new TransitionReport(
                    readiness, readinessError, drainDelayMs, drainDelayInterrupted,
                    queues, queueError, hooks, transitionDurationMs
            );
        }
    }

    /**
     * Creates a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }
}
