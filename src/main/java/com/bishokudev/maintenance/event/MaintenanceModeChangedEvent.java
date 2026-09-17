package com.bishokudev.maintenance.event;

import com.bishokudev.maintenance.model.TransitionReport;
import org.springframework.context.ApplicationEvent;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable domain application event published whenever maintenance mode state transitions.
 * Consuming applications can listen to this event using {@code @EventListener}.
 */
public class MaintenanceModeChangedEvent extends ApplicationEvent {

    private final boolean entering;
    private final Instant transitionTimestamp;
    private final String reason;
    private final TransitionReport report;
    private final Map<String, Object> details;

    /**
     * Constructs a new event with a type-safe {@link TransitionReport}.
     *
     * @param source              the event source
     * @param entering            {@code true} if entering maintenance; {@code false} if exiting
     * @param transitionTimestamp timestamp of when the transition completed
     * @param reason              reason string provided for the transition
     * @param report              type-safe transition report
     */
    public MaintenanceModeChangedEvent(Object source, boolean entering, Instant transitionTimestamp,
                                       String reason, TransitionReport report) {
        super(source);
        this.entering = entering;
        this.transitionTimestamp = Objects.requireNonNull(transitionTimestamp, "transitionTimestamp must not be null");
        this.reason = reason != null ? reason : "";
        this.report = report;
        // Backward-compatible details map
        this.details = report != null ? report.toMap()
                : Collections.emptyMap();
    }

    /**
     * Constructs a new event with a raw details map (backward-compatible constructor).
     *
     * @param source              the event source
     * @param entering            {@code true} if entering maintenance; {@code false} if exiting
     * @param transitionTimestamp timestamp of when the transition completed
     * @param reason              reason string provided for the transition
     * @param details             transition details as a map
     */
    public MaintenanceModeChangedEvent(Object source, boolean entering, Instant transitionTimestamp,
                                       String reason, Map<String, Object> details) {
        super(source);
        this.entering = entering;
        this.transitionTimestamp = Objects.requireNonNull(transitionTimestamp, "transitionTimestamp must not be null");
        this.reason = reason != null ? reason : "";
        this.report = null;
        this.details = details != null
                ? Collections.unmodifiableMap(new LinkedHashMap<>(details))
                : Collections.emptyMap();
    }

    /** @return {@code true} if transitioning into maintenance mode; {@code false} if exiting */
    public boolean isEntering() {
        return entering;
    }

    /** @return timestamp of when the transition completed */
    public Instant getTransitionTimestamp() {
        return transitionTimestamp;
    }

    /** @return reason string provided for the transition */
    public String getReason() {
        return reason;
    }

    /**
     * Returns the type-safe transition report, or {@code null} if the event was
     * constructed with a raw details map.
     *
     * @return the transition report, may be {@code null}
     */
    public TransitionReport getReport() {
        return report;
    }

    /** @return unmodifiable defensive copy of component transition details */
    public Map<String, Object> getDetails() {
        return details;
    }

    @Override
    public String toString() {
        return "MaintenanceModeChangedEvent{" +
                "entering=" + entering +
                ", transitionTimestamp=" + transitionTimestamp +
                ", reason='" + reason + '\'' +
                ", details=" + details +
                '}';
    }
}
