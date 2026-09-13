package com.bishokudev.maintenance.hook;

/**
 * Extension interface that application developers can implement in their Spring beans.
 * Registered hooks are automatically collected and executed in order during maintenance transitions.
 */
public interface MaintenanceHook {

    /**
     * Invoked when the application enters maintenance mode.
     * Guaranteed to run after ingress traffic is refused and message queues are paused.
     */
    default void onEnterMaintenance() {
    }

    /**
     * Invoked when the application exits maintenance mode.
     * Invoked before readiness probes resume accepting external traffic.
     */
    default void onExitMaintenance() {
    }
}
