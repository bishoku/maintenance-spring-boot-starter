package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * Encapsulates Kubernetes readiness probe transitions and the drain delay sleep,
 * keeping low-level {@link Thread#sleep} and interrupt handling out of the coordinator.
 */
public class TrafficDrainHandler {

    private static final Logger log = LoggerFactory.getLogger(TrafficDrainHandler.class);

    private final KubernetesReadinessManager readinessManager;

    public TrafficDrainHandler(KubernetesReadinessManager readinessManager) {
        this.readinessManager = Objects.requireNonNull(readinessManager, "readinessManager must not be null");
    }

    /**
     * Result of a traffic drain operation.
     *
     * @param readiness   readiness state string (e.g. "REFUSING_TRAFFIC", "ACCEPTING_TRAFFIC")
     * @param error       error message if the readiness transition failed; {@code null} otherwise
     * @param drainDelayMs drain delay applied in milliseconds; {@code null} if not applicable
     * @param interrupted {@code true} if the drain delay sleep was interrupted
     */
    public record Result(String readiness, String error, Long drainDelayMs, boolean interrupted) {

        /** Result for when readiness management is disabled via configuration. */
        public static Result disabled() {
            return new Result("DISABLED", null, null, false);
        }
    }

    /**
     * Transitions to {@code REFUSING_TRAFFIC} and then sleeps for the drain delay
     * to allow Kubernetes to propagate endpoint changes.
     *
     * @param drainDelay the drain delay duration
     * @return result capturing readiness state, errors, and delay information
     */
    public Result refuseTrafficAndDrain(Duration drainDelay) {
        String readiness = null;
        String error = null;
        Long delayMs = null;
        boolean interrupted = false;

        try {
            readinessManager.refuseTraffic();
            readiness = "REFUSING_TRAFFIC";
        } catch (Exception ex) {
            log.error("Failed to transition Kubernetes readiness to REFUSING_TRAFFIC", ex);
            error = ex.getMessage();
        }

        if (drainDelay != null && !drainDelay.isZero() && !drainDelay.isNegative()) {
            try {
                log.info("Waiting {}ms for Kubernetes endpoint drain...", drainDelay.toMillis());
                Thread.sleep(drainDelay.toMillis());
                delayMs = drainDelay.toMillis();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.warn("Drain delay interrupted", ex);
                interrupted = true;
            }
        }

        return new Result(readiness, error, delayMs, interrupted);
    }

    /**
     * Transitions to {@code ACCEPTING_TRAFFIC}.
     *
     * @return result capturing readiness state and any errors
     */
    public Result acceptTraffic() {
        try {
            readinessManager.acceptTraffic();
            return new Result("ACCEPTING_TRAFFIC", null, null, false);
        } catch (Exception ex) {
            log.error("Failed to transition Kubernetes readiness to ACCEPTING_TRAFFIC", ex);
            return new Result(null, ex.getMessage(), null, false);
        }
    }
}
