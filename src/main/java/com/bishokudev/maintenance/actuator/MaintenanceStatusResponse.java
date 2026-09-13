package com.bishokudev.maintenance.actuator;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * DTO representing the response from the {@code /actuator/maintenance} endpoint.
 */
public record MaintenanceStatusResponse(
        boolean active,
        Instant lastChanged,
        String reason,
        Map<String, Object> details
) {
    public MaintenanceStatusResponse {
        details = details != null ? Collections.unmodifiableMap(details) : Collections.emptyMap();
        reason = reason != null ? reason : "";
    }
}
