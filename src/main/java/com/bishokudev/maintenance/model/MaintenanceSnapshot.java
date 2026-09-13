package com.bishokudev.maintenance.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable, thread-safe snapshot of the maintenance mode state at a point in time.
 * Used internally by {@link MaintenanceState} to guarantee atomic reads across all fields.
 */
public record MaintenanceSnapshot(
        boolean active,
        Instant lastChanged,
        String reason,
        Map<String, Object> details
) {
    public MaintenanceSnapshot {
        lastChanged = lastChanged != null ? lastChanged : Instant.now();
        reason = reason != null ? reason : "";
        details = details != null
                ? Collections.unmodifiableMap(new LinkedHashMap<>(details))
                : Collections.emptyMap();
    }

    /**
     * Creates a default inactive snapshot.
     */
    public static MaintenanceSnapshot inactive() {
        return new MaintenanceSnapshot(false, Instant.now(), "", Collections.emptyMap());
    }
}
