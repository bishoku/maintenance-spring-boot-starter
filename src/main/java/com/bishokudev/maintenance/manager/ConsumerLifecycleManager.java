package com.bishokudev.maintenance.manager;

import java.util.Map;

/**
 * Strategy interface for managing the lifecycle of message queue consumers.
 * <p>
 * Implementations handle a specific broker (Kafka, RabbitMQ, etc.) and are
 * conditionally registered only when the broker's classes are on the classpath.
 */
public interface ConsumerLifecycleManager {

    /**
     * @return human-readable name of the managed broker (e.g. "kafka", "rabbit")
     */
    String name();

    /**
     * Stops all managed consumers for this broker.
     *
     * @return report map with details about what was stopped
     */
    Map<String, Object> stopConsumers();

    /**
     * Starts all managed consumers for this broker.
     *
     * @return report map with details about what was started
     */
    Map<String, Object> startConsumers();
}
