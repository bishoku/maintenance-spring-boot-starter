package com.bishokudev.maintenance.model;

/**
 * Immutable detail of a single {@link com.bishokudev.maintenance.hook.MaintenanceHook} execution.
 *
 * @param name       fully-qualified class name of the hook
 * @param status     execution outcome: {@code "success"}, {@code "failed"}, or {@code "timed_out"}
 * @param durationMs wall-clock execution time in milliseconds
 */
public record HookExecutionDetail(String name, String status, long durationMs) {
}
