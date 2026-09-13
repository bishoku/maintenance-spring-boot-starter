package com.bishokudev.maintenance.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for the maintenance mode starter.
 *
 * <pre>
 * maintenance:
 *   drain-delay: 5s
 *   hook-timeout: 30s
 * </pre>
 */
@ConfigurationProperties(prefix = "maintenance")
public class MaintenanceProperties {

    /**
     * Delay between setting readiness to REFUSING_TRAFFIC and stopping queue consumers.
     * Allows Kubernetes time to remove the pod from service endpoints before in-flight
     * requests are affected.
     */
    private Duration drainDelay = Duration.ofSeconds(5);

    /**
     * Maximum time to wait for each {@code MaintenanceHook} to complete before timing out.
     * A hook that exceeds this duration will be interrupted and logged as failed.
     */
    private Duration hookTimeout = Duration.ofSeconds(30);

    public Duration getDrainDelay() {
        return drainDelay;
    }

    public void setDrainDelay(Duration drainDelay) {
        this.drainDelay = drainDelay;
    }

    public Duration getHookTimeout() {
        return hookTimeout;
    }

    public void setHookTimeout(Duration hookTimeout) {
        this.hookTimeout = hookTimeout;
    }
}
