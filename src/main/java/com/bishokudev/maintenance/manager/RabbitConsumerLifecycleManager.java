package com.bishokudev.maintenance.manager;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Stops and restores only the containers owned by this maintenance window. */
public class RabbitConsumerLifecycleManager implements ConsumerLifecycleManager {
    private final ObjectProvider<RabbitListenerEndpointRegistry> registryProvider;
    private final Duration timeout;
    private final Map<MessageListenerContainer, CountDownLatch> owned = new LinkedHashMap<>();
    private final Set<MessageListenerContainer> startupOwned = new LinkedHashSet<>();
    public RabbitConsumerLifecycleManager(ObjectProvider<RabbitListenerEndpointRegistry> provider) { this(provider, Duration.ofSeconds(30)); }
    public RabbitConsumerLifecycleManager(ObjectProvider<RabbitListenerEndpointRegistry> provider, Duration timeout) {
        registryProvider = Objects.requireNonNull(provider); this.timeout = timeout;
    }
    @Override public String name() { return "rabbit"; }
    /** Defers an otherwise automatic initial start; explicitly disabled listeners remain user-owned. */
    public synchronized void deferStartup(MessageListenerContainer container) {
        if (!container.isAutoStartup()) return;
        if (container.isRunning()) throw new IllegalStateException("Rabbit factory started a container before startup maintenance could defer it");
        container.setAutoStartup(false);
        if (container.isAutoStartup()) throw new IllegalStateException("Rabbit container does not support deferring autoStartup");
        startupOwned.add(container);
    }
    @Override public synchronized Map<String,Object> stopConsumers() {
        RabbitListenerEndpointRegistry registry = registryProvider.getIfAvailable();
        if (registry == null) return Map.of("rabbitAvailable", false);
        long deadline = System.nanoTime() + timeout.toNanos();
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            if (container.isRunning() && (!owned.containsKey(container) || owned.get(container).getCount() == 0)) {
                CountDownLatch stopped = new CountDownLatch(1);
                owned.put(container, stopped);
                // Keep ownership even if stop throws: an unacknowledged stop must not
                // become a successful retry merely because isRunning() became false.
                container.stop(stopped::countDown);
            }
        }
        for (CountDownLatch stopped : owned.values()) {
            try {
                if (!stopped.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                    throw new IllegalStateException("Rabbit stop acknowledgement timed out");
            } catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException("Rabbit stop interrupted", ex); }
        }
        return Map.of("rabbitStopped", true, "containersAffected", owned.size(), "startupDeferred", startupOwned.size());
    }
    @Override public synchronized Map<String,Object> startConsumers() {
        if (owned.values().stream().anyMatch(latch -> latch.getCount() != 0))
            throw new IllegalStateException("Rabbit stop still in progress");
        int count = owned.size() + startupOwned.size();
        for (MessageListenerContainer container : startupOwned) {
            container.setAutoStartup(true);
            container.start();
            if (!container.isRunning()) throw new IllegalStateException("Maintenance-owned Rabbit container failed to start");
        }
        for (MessageListenerContainer container : owned.keySet()) container.start();
        if (owned.keySet().stream().anyMatch(c -> !c.isRunning())) throw new IllegalStateException("Rabbit restart failed");
        owned.clear();
        startupOwned.clear();
        return Map.of("rabbitStarted", true, "containersAffected", count);
    }
}
