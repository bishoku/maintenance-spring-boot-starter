package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.ObjectProvider;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages RabbitMQ consumer lifecycle via {@link RabbitListenerEndpointRegistry}.
 * Uses {@link ObjectProvider} to defer registry resolution and avoid auto-configuration order sensitivity.
 */
public class RabbitConsumerLifecycleManager implements ConsumerLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(RabbitConsumerLifecycleManager.class);

    private final ObjectProvider<RabbitListenerEndpointRegistry> registryProvider;

    public RabbitConsumerLifecycleManager(ObjectProvider<RabbitListenerEndpointRegistry> registryProvider) {
        this.registryProvider = Objects.requireNonNull(registryProvider, "registryProvider must not be null");
    }

    @Override
    public String name() {
        return "rabbit";
    }

    @Override
    public Map<String, Object> stopConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        AtomicBoolean found = new AtomicBoolean(false);

        registryProvider.ifAvailable(registry -> {
            found.set(true);
            if (registry.isRunning()) {
                int count = registry.getListenerContainerIds() != null ? registry.getListenerContainerIds().size() : 0;
                log.info("Stopping RabbitListenerEndpointRegistry ({} containers)...", count);
                registry.stop();
                report.put("rabbitStopped", true);
                report.put("containersAffected", count);
            } else {
                log.debug("RabbitListenerEndpointRegistry is already stopped.");
                report.put("rabbitStopped", false);
                report.put("reason", "already stopped");
            }
        });

        if (!found.get()) {
            report.put("rabbitAvailable", false);
            report.put("reason", "no RabbitListenerEndpointRegistry bean in context");
        }

        return report;
    }

    @Override
    public Map<String, Object> startConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        AtomicBoolean found = new AtomicBoolean(false);

        registryProvider.ifAvailable(registry -> {
            found.set(true);
            if (!registry.isRunning()) {
                int count = registry.getListenerContainerIds() != null ? registry.getListenerContainerIds().size() : 0;
                log.info("Starting RabbitListenerEndpointRegistry ({} containers)...", count);
                registry.start();
                report.put("rabbitStarted", true);
                report.put("containersAffected", count);
            } else {
                log.debug("RabbitListenerEndpointRegistry is already running.");
                report.put("rabbitStarted", false);
                report.put("reason", "already running");
            }
        });

        if (!found.get()) {
            report.put("rabbitAvailable", false);
            report.put("reason", "no RabbitListenerEndpointRegistry bean in context");
        }

        return report;
    }
}
