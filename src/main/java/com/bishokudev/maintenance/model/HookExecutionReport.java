package com.bishokudev.maintenance.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable, type-safe report summarizing the execution of all
 * {@link com.bishokudev.maintenance.hook.MaintenanceHook} callbacks during a single transition.
 *
 * @param total          number of registered hooks
 * @param succeeded      hooks that completed successfully
 * @param failed         hooks that threw an exception
 * @param timedOut       hooks that exceeded the configured timeout
 * @param executions     per-hook execution details (ordered)
 * @param failures       human-readable failure messages (empty if none)
 * @param partialFailure {@code true} if at least one hook failed or timed out
 */
public record HookExecutionReport(
        int total,
        int succeeded,
        int failed,
        int timedOut,
        List<HookExecutionDetail> executions,
        List<String> failures,
        boolean partialFailure
) {
    public HookExecutionReport {
        executions = executions != null ? List.copyOf(executions) : List.of();
        failures = failures != null ? List.copyOf(failures) : List.of();
    }

    /**
     * Creates a report representing a disabled hooks configuration.
     */
    public static HookExecutionReport disabled() {
        return new HookExecutionReport(0, 0, 0, 0, List.of(), List.of(), false);
    }

    /**
     * Converts this report to a {@code Map<String, Object>} structure identical to the
     * legacy hand-built map, ensuring backward-compatible JSON serialization.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("total", total);
        map.put("succeeded", succeeded);
        map.put("failed", failed);
        map.put("timedOut", timedOut);
        map.put("executions", executions.stream()
                .map(e -> Map.<String, Object>of("name", e.name(), "status", e.status(), "durationMs", e.durationMs()))
                .toList());
        if (partialFailure) {
            map.put("partialFailure", true);
            map.put("failures", List.copyOf(failures));
        }
        return Collections.unmodifiableMap(map);
    }
}
