package com.bishokudev.maintenance.hook;

/**
 * Extension interface that application developers can implement in their Spring beans.
 * Registered hooks are collected and executed in order during maintenance transitions.
 * Hooks must be bounded, interruptible and idempotent: failed transitions may be retried.
 * Do not call the coordinator from a hook.
 */
public interface MaintenanceHook {

    /**
     * Invoked when the application enters maintenance mode.
     * Runs after enabled readiness/grace-period and broker suspension steps succeed.
     */
    default void onEnterMaintenance() {
    }

    /**
     * Invoked when the application exits maintenance mode.
     * Preparation runs before consumers and HTTP traffic resume. Failure withholds readiness acceptance.
     */
    default void onExitMaintenance() {
    }
}
