package com.bishokudev.maintenance.e2e;

import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KafkaConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.RabbitConsumerLifecycleManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration and cross-feature tests focusing on listener container lifecycle on startup
 * and dynamic exit across message brokers (Kafka, RabbitMQ) - Tiers 2 & 3.
 */
public class StartupMaintenanceQueueLifecycleTest {

    @Test
    @DisplayName("Tier 3: Kafka consumer container is paused on startup stopConsumers and resumed on startConsumers")
    @SuppressWarnings("unchecked")
    void tier3_kafkaContainerPauseAndResumeLifecycle() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer activeContainer = mock(MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(activeContainer));

        AtomicBoolean paused = new AtomicBoolean(false);
        when(activeContainer.isRunning()).thenReturn(true);
        when(activeContainer.isPauseRequested()).thenAnswer(inv -> paused.get());
        when(activeContainer.isContainerPaused()).thenAnswer(inv -> paused.get());

        doAnswer(inv -> {
            paused.set(true);
            return null;
        }).when(activeContainer).pause();

        doAnswer(inv -> {
            paused.set(false);
            return null;
        }).when(activeContainer).resume();

        KafkaConsumerLifecycleManager kafkaManager =
                new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(200));

        // Startup simulation: stopConsumers() pauses active containers
        Map<String, Object> stopResult = kafkaManager.stopConsumers();
        assertThat(stopResult).containsEntry("kafkaPaused", true);
        assertThat(stopResult).containsEntry("containersAffected", 1);
        verify(activeContainer, times(1)).pause();
        assertThat(paused.get()).isTrue();

        // Dynamic exit simulation: startConsumers() resumes owned containers
        Map<String, Object> startResult = kafkaManager.startConsumers();
        assertThat(startResult).containsEntry("kafkaResumed", true);
        assertThat(startResult).containsEntry("containersAffected", 1);
        verify(activeContainer, times(1)).resume();
        assertThat(paused.get()).isFalse();
    }

    @Test
    @DisplayName("Tier 3: RabbitMQ consumer container is stopped on startup stopConsumers and restarted on startConsumers")
    @SuppressWarnings("unchecked")
    void tier3_rabbitContainerStopAndStartLifecycle() {
        ObjectProvider<org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry registry =
                mock(org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry.class);
        org.springframework.amqp.rabbit.listener.MessageListenerContainer rabbitContainer =
                mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(rabbitContainer));

        AtomicBoolean running = new AtomicBoolean(true);
        when(rabbitContainer.isRunning()).thenAnswer(inv -> running.get());

        doAnswer(inv -> {
            running.set(false);
            Runnable callback = inv.getArgument(0);
            if (callback != null) callback.run();
            return null;
        }).when(rabbitContainer).stop(any(Runnable.class));

        doAnswer(inv -> {
            running.set(true);
            return null;
        }).when(rabbitContainer).start();

        RabbitConsumerLifecycleManager rabbitManager =
                new RabbitConsumerLifecycleManager(provider, Duration.ofMillis(200));

        // Startup simulation: stop consumers
        Map<String, Object> stopResult = rabbitManager.stopConsumers();
        assertThat(stopResult).containsEntry("rabbitStopped", true);
        assertThat(stopResult).containsEntry("containersAffected", 1);
        verify(rabbitContainer, times(1)).stop(any(Runnable.class));
        assertThat(running.get()).isFalse();

        // Dynamic exit simulation: restart consumers
        Map<String, Object> startResult = rabbitManager.startConsumers();
        assertThat(startResult).containsEntry("rabbitStarted", true);
        assertThat(startResult).containsEntry("containersAffected", 1);
        verify(rabbitContainer, times(1)).start();
        assertThat(running.get()).isTrue();
    }

    @Test
    @DisplayName("Tier 3: Multi-broker lifecycle orchestration delegates to all registered managers on startup and exit")
    void tier3_multiBrokerOrchestration() {
        ConsumerLifecycleManager kafka = mock(ConsumerLifecycleManager.class);
        when(kafka.name()).thenReturn("kafka");
        when(kafka.stopConsumers()).thenReturn(Map.of("kafkaStopped", true));
        when(kafka.startConsumers()).thenReturn(Map.of("kafkaResumed", true));

        ConsumerLifecycleManager rabbit = mock(ConsumerLifecycleManager.class);
        when(rabbit.name()).thenReturn("rabbit");
        when(rabbit.stopConsumers()).thenReturn(Map.of("rabbitStopped", true));
        when(rabbit.startConsumers()).thenReturn(Map.of("rabbitStarted", true));

        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of(kafka, rabbit));

        // Startup pause
        Map<String, Object> stopReport = queueManager.stopConsumers();
        verify(kafka, times(1)).stopConsumers();
        verify(rabbit, times(1)).stopConsumers();
        assertThat(stopReport).containsKeys("kafka", "rabbit");

        // Dynamic exit resume
        Map<String, Object> startReport = queueManager.startConsumers();
        verify(kafka, times(1)).startConsumers();
        verify(rabbit, times(1)).startConsumers();
        assertThat(startReport).containsKeys("kafka", "rabbit");
    }

    @Test
    @DisplayName("Tier 2: Broker error isolation on startup - one failing broker does not crash coordinator or skip others")
    void tier2_brokerErrorIsolationOnStartup() {
        ConsumerLifecycleManager faultyBroker = mock(ConsumerLifecycleManager.class);
        when(faultyBroker.name()).thenReturn("faulty-broker");
        when(faultyBroker.stopConsumers()).thenThrow(new IllegalStateException("Broker connection timeout"));

        ConsumerLifecycleManager healthyBroker = mock(ConsumerLifecycleManager.class);
        when(healthyBroker.name()).thenReturn("healthy-broker");
        when(healthyBroker.stopConsumers()).thenReturn(Map.of("stopped", true));

        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of(faultyBroker, healthyBroker));

        Map<String, Object> report = queueManager.stopConsumers();

        // Both managers were invoked despite failure in first
        verify(faultyBroker, times(1)).stopConsumers();
        verify(healthyBroker, times(1)).stopConsumers();

        assertThat(report).containsKey("faulty-broker");
        assertThat(report.get("faulty-broker").toString()).contains("Broker connection timeout");
        assertThat(report).containsKey("healthy-broker");
    }

    @Test
    @DisplayName("Tier 2: Pre-existing idle containers are not resumed on exit (ownership boundary)")
    @SuppressWarnings("unchecked")
    void tier2_preExistingIdleContainersNotResumedOnExit() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer runningContainer = mock(MessageListenerContainer.class);
        MessageListenerContainer preExistingIdleContainer = mock(MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(runningContainer, preExistingIdleContainer));

        when(runningContainer.isRunning()).thenReturn(true);
        when(preExistingIdleContainer.isRunning()).thenReturn(false); // idle before startup

        AtomicBoolean runningPaused = new AtomicBoolean(false);
        when(runningContainer.isPauseRequested()).thenAnswer(inv -> runningPaused.get());
        when(runningContainer.isContainerPaused()).thenAnswer(inv -> runningPaused.get());

        doAnswer(inv -> { runningPaused.set(true); return null; }).when(runningContainer).pause();
        doAnswer(inv -> { runningPaused.set(false); return null; }).when(runningContainer).resume();

        KafkaConsumerLifecycleManager kafkaManager =
                new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(100));

        kafkaManager.stopConsumers();

        // Idle container was NOT paused
        verify(preExistingIdleContainer, never()).pause();

        kafkaManager.startConsumers();

        // Idle container was NOT resumed
        verify(preExistingIdleContainer, never()).resume();
        verify(runningContainer, times(1)).resume();
    }
}
