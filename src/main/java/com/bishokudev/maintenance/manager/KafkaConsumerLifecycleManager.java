package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Manages Kafka consumer lifecycle via {@link KafkaListenerEndpointRegistry}.
 * Only instantiated when {@code spring-kafka} is on the classpath.
 */
public class KafkaConsumerLifecycleManager implements ConsumerLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLifecycleManager.class);

    private final KafkaListenerEndpointRegistry registry;

    public KafkaConsumerLifecycleManager(KafkaListenerEndpointRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "KafkaListenerEndpointRegistry must not be null");
    }

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public Map<String, Object> stopConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        if (registry.isRunning()) {
            log.info("Stopping KafkaListenerEndpointRegistry ({} containers)...",
                    registry.getListenerContainerIds().size());
            registry.stop();
            report.put("kafkaStopped", true);
            report.put("containersAffected", registry.getListenerContainerIds().size());
        } else {
            log.debug("KafkaListenerEndpointRegistry is already stopped.");
            report.put("kafkaStopped", false);
            report.put("reason", "already stopped");
        }
        return report;
    }

    @Override
    public Map<String, Object> startConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        if (!registry.isRunning()) {
            log.info("Starting KafkaListenerEndpointRegistry ({} containers)...",
                    registry.getListenerContainerIds().size());
            registry.start();
            report.put("kafkaStarted", true);
            report.put("containersAffected", registry.getListenerContainerIds().size());
        } else {
            log.debug("KafkaListenerEndpointRegistry is already running.");
            report.put("kafkaStarted", false);
            report.put("reason", "already running");
        }
        return report;
    }
}
