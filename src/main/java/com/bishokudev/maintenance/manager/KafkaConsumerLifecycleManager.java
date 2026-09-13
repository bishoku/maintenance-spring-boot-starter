package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages Kafka consumer lifecycle via {@link KafkaListenerEndpointRegistry}.
 * Uses {@link ObjectProvider} to defer registry resolution and avoid auto-configuration order sensitivity.
 */
public class KafkaConsumerLifecycleManager implements ConsumerLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLifecycleManager.class);

    private final ObjectProvider<KafkaListenerEndpointRegistry> registryProvider;

    public KafkaConsumerLifecycleManager(ObjectProvider<KafkaListenerEndpointRegistry> registryProvider) {
        this.registryProvider = Objects.requireNonNull(registryProvider, "registryProvider must not be null");
    }

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public Map<String, Object> stopConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        AtomicBoolean found = new AtomicBoolean(false);

        registryProvider.ifAvailable(registry -> {
            found.set(true);
            if (registry.isRunning()) {
                int count = registry.getListenerContainerIds() != null ? registry.getListenerContainerIds().size() : 0;
                log.info("Stopping KafkaListenerEndpointRegistry ({} containers)...", count);
                registry.stop();
                report.put("kafkaStopped", true);
                report.put("containersAffected", count);
            } else {
                log.debug("KafkaListenerEndpointRegistry is already stopped.");
                report.put("kafkaStopped", false);
                report.put("reason", "already stopped");
            }
        });

        if (!found.get()) {
            report.put("kafkaAvailable", false);
            report.put("reason", "no KafkaListenerEndpointRegistry bean in context");
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
                log.info("Starting KafkaListenerEndpointRegistry ({} containers)...", count);
                registry.start();
                report.put("kafkaStarted", true);
                report.put("containersAffected", count);
            } else {
                log.debug("KafkaListenerEndpointRegistry is already running.");
                report.put("kafkaStarted", false);
                report.put("reason", "already running");
            }
        });

        if (!found.get()) {
            report.put("kafkaAvailable", false);
            report.put("reason", "no KafkaListenerEndpointRegistry bean in context");
        }

        return report;
    }
}
