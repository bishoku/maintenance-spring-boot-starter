package com.bishokudev.maintenance.manager;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import java.time.Duration;
import java.util.*;

/** Pauses running containers without leaving their consumer groups. */
public class KafkaConsumerLifecycleManager implements ConsumerLifecycleManager {
    private final ObjectProvider<KafkaListenerEndpointRegistry> registryProvider;
    private final Duration timeout;
    private final Set<MessageListenerContainer> owned = new LinkedHashSet<>();
    public KafkaConsumerLifecycleManager(ObjectProvider<KafkaListenerEndpointRegistry> provider) { this(provider, Duration.ofSeconds(30)); }
    public KafkaConsumerLifecycleManager(ObjectProvider<KafkaListenerEndpointRegistry> provider, Duration timeout) {
        registryProvider = Objects.requireNonNull(provider); this.timeout = timeout;
    }
    @Override public String name() { return "kafka"; }
    @Override public synchronized Map<String,Object> stopConsumers() {
        KafkaListenerEndpointRegistry registry = registryProvider.getIfAvailable();
        if (registry == null) return Map.of("kafkaAvailable", false);
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            if (container.isRunning() && !container.isPauseRequested()) {
                owned.add(container);
                container.pause();
            }
        }
        // Includes externally paused containers: requested is not the same as acknowledged.
        await(() -> registry.getListenerContainers().stream().allMatch(this::isQuiescent));
        return Map.of("kafkaPaused", true, "containersAffected", owned.size());
    }
    @Override public synchronized Map<String,Object> startConsumers() {
        int count = owned.size();
        for (MessageListenerContainer container : owned) {
            if (!container.isRunning()) throw new IllegalStateException("Maintenance-owned Kafka container stopped externally");
            container.resume();
        }
        await(() -> owned.stream().noneMatch(this::isAnyPaused));
        owned.clear();
        return Map.of("kafkaResumed", true, "containersAffected", count);
    }
    private boolean isAnyPaused(MessageListenerContainer container) {
        if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrent) {
            return concurrent.getContainers().stream().anyMatch(this::isAnyPaused);
        }
        return container.isContainerPaused();
    }
    private boolean isQuiescent(MessageListenerContainer container) {
        if (!container.isRunning() || container.isContainerPaused()) return true;
        if (!container.isPauseRequested()) return false;
        if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrent) {
            return concurrent.getContainers().stream().allMatch(this::isQuiescent);
        }
        // Spring only acknowledges pause when partitions are assigned. An unassigned
        // consumer has no records to drain; keep its pause request for future assignments.
        var assigned = container.getAssignedPartitions();
        return assigned != null && assigned.isEmpty();
    }
    private void await(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new IllegalStateException("Kafka pause/resume acknowledgement timed out");
            try { Thread.sleep(10); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException("Kafka lifecycle interrupted", ex); }
        }
    }
}
