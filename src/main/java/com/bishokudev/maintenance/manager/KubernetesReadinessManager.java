package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Objects;

/**
 * Manages Kubernetes readiness state by emitting {@link AvailabilityChangeEvent}s.
 * Transitioning to {@link ReadinessState#REFUSING_TRAFFIC} causes Kubernetes readiness probes
 * to fail, removing the pod from service endpoints.
 */
public class KubernetesReadinessManager {

    private static final Logger log = LoggerFactory.getLogger(KubernetesReadinessManager.class);

    private final ApplicationEventPublisher eventPublisher;

    public KubernetesReadinessManager(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
    }

    /**
     * Publishes REFUSING_TRAFFIC readiness state.
     */
    public void refuseTraffic() {
        log.info("Transitioning Kubernetes readiness state to REFUSING_TRAFFIC");
        AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.REFUSING_TRAFFIC);
    }

    /**
     * Publishes ACCEPTING_TRAFFIC readiness state.
     */
    public void acceptTraffic() {
        log.info("Transitioning Kubernetes readiness state to ACCEPTING_TRAFFIC");
        AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.ACCEPTING_TRAFFIC);
    }
}
