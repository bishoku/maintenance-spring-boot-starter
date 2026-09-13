package com.bishokudev.maintenance.event;

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
    private final Map<String, Object> details;

    public MaintenanceModeChangedEvent(Object source, boolean entering, Instant transitionTimestamp,
                                       String reason, Map<String, Object> details) {
        super(source);
        this.entering = entering;
        this.transitionTimestamp = Objects.requireNonNull(transitionTimestamp, "transitionTimestamp must not be null");
        this.reason = reason != null ? reason : "";
        // Defensive copy to guarantee true immutability
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
