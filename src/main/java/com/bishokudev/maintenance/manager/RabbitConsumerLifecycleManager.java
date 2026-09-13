package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Manages RabbitMQ consumer lifecycle via {@link RabbitListenerEndpointRegistry}.
 * Only instantiated when {@code spring-rabbit} is on the classpath.
 */
public class RabbitConsumerLifecycleManager implements ConsumerLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(RabbitConsumerLifecycleManager.class);

    private final RabbitListenerEndpointRegistry registry;

    public RabbitConsumerLifecycleManager(RabbitListenerEndpointRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "RabbitListenerEndpointRegistry must not be null");
    }

    @Override
    public String name() {
        return "rabbit";
    }

    @Override
    public Map<String, Object> stopConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        if (registry.isRunning()) {
            log.info("Stopping RabbitListenerEndpointRegistry ({} containers)...",
                    registry.getListenerContainerIds().size());
            registry.stop();
            report.put("rabbitStopped", true);
            report.put("containersAffected", registry.getListenerContainerIds().size());
        } else {
            log.debug("RabbitListenerEndpointRegistry is already stopped.");
            report.put("rabbitStopped", false);
            report.put("reason", "already stopped");
        }
        return report;
    }

    @Override
    public Map<String, Object> startConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        if (!registry.isRunning()) {
            log.info("Starting RabbitListenerEndpointRegistry ({} containers)...",
                    registry.getListenerContainerIds().size());
            registry.start();
            report.put("rabbitStarted", true);
            report.put("containersAffected", registry.getListenerContainerIds().size());
        } else {
            log.debug("RabbitListenerEndpointRegistry is already running.");
            report.put("rabbitStarted", false);
            report.put("reason", "already running");
        }
        return report;
    }
}
