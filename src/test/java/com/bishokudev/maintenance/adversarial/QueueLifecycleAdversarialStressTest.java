package com.bishokudev.maintenance.adversarial;

import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KafkaConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.RabbitConsumerLifecycleManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Adversarial stress harness verifying message queue lifecycle under abnormal conditions:
 * container pause/resume idempotency, repeated stops/starts, external container crashes,
 * and concurrency stress.
 */
public class QueueLifecycleAdversarialStressTest {

    @Test
    @DisplayName("Idempotency: Repeated stopConsumers calls on Kafka manager do not corrupt owned containers")
    @SuppressWarnings("unchecked")
    void repeatedKafkaStopConsumersIsIdempotent() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(container));

        AtomicBoolean paused = new AtomicBoolean(false);
        when(container.isRunning()).thenReturn(true);
        when(container.isPauseRequested()).thenAnswer(inv -> paused.get());
        when(container.isContainerPaused()).thenAnswer(inv -> paused.get());

        doAnswer(inv -> { paused.set(true); return null; }).when(container).pause();
        doAnswer(inv -> { paused.set(false); return null; }).when(container).resume();

        KafkaConsumerLifecycleManager kafkaManager =
                new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(200));

        // First stop
        Map<String, Object> firstStop = kafkaManager.stopConsumers();
        assertThat(firstStop).containsEntry("kafkaPaused", true);
        assertThat(firstStop).containsEntry("containersAffected", 1);
        verify(container, times(1)).pause();

        // Second stop (idempotent - already paused)
        Map<String, Object> secondStop = kafkaManager.stopConsumers();
        assertThat(secondStop).containsEntry("kafkaPaused", true);
        assertThat(secondStop).containsEntry("containersAffected", 1);
        verify(container, times(1)).pause(); // pause not called again

        // First start (resumes)
        Map<String, Object> firstStart = kafkaManager.startConsumers();
        assertThat(firstStart).containsEntry("kafkaResumed", true);
        assertThat(firstStart).containsEntry("containersAffected", 1);
        verify(container, times(1)).resume();

        // Second start (idempotent - owned set was cleared)
        Map<String, Object> secondStart = kafkaManager.startConsumers();
        assertThat(secondStart).containsEntry("kafkaResumed", true);
        assertThat(secondStart).containsEntry("containersAffected", 0);
        verify(container, times(1)).resume(); // resume not called again
    }

    @Test
    @DisplayName("Idempotency: Repeated stopConsumers calls on RabbitMQ manager do not corrupt owned map")
    @SuppressWarnings("unchecked")
    void repeatedRabbitStopConsumersIsIdempotent() {
        ObjectProvider<RabbitListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        RabbitListenerEndpointRegistry registry = mock(RabbitListenerEndpointRegistry.class);
        org.springframework.amqp.rabbit.listener.MessageListenerContainer container =
                mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(container));

        AtomicBoolean running = new AtomicBoolean(true);
        when(container.isRunning()).thenAnswer(inv -> running.get());

        doAnswer(inv -> {
            running.set(false);
            Runnable callback = inv.getArgument(0);
            if (callback != null) callback.run();
            return null;
        }).when(container).stop(any(Runnable.class));

        doAnswer(inv -> {
            running.set(true);
            return null;
        }).when(container).start();

        RabbitConsumerLifecycleManager rabbitManager =
                new RabbitConsumerLifecycleManager(provider, Duration.ofMillis(200));

        // First stop
        Map<String, Object> firstStop = rabbitManager.stopConsumers();
        assertThat(firstStop).containsEntry("rabbitStopped", true);
        assertThat(firstStop).containsEntry("containersAffected", 1);
        verify(container, times(1)).stop(any(Runnable.class));

        // Second stop (container already stopped, so not stopped again)
        Map<String, Object> secondStop = rabbitManager.stopConsumers();
        assertThat(secondStop).containsEntry("rabbitStopped", true);
        assertThat(secondStop).containsEntry("containersAffected", 1);
        verify(container, times(1)).stop(any(Runnable.class));

        // First start (restarts container)
        Map<String, Object> firstStart = rabbitManager.startConsumers();
        assertThat(firstStart).containsEntry("rabbitStarted", true);
        assertThat(firstStart).containsEntry("containersAffected", 1);
        verify(container, times(1)).start();

        // Second start (owned was cleared)
        Map<String, Object> secondStart = rabbitManager.startConsumers();
        assertThat(secondStart).containsEntry("rabbitStarted", true);
        assertThat(secondStart).containsEntry("containersAffected", 0);
        verify(container, times(1)).start();
    }

    @Test
    @DisplayName("Abnormal: Kafka container stopped externally during maintenance fails fast on resume")
    @SuppressWarnings("unchecked")
    void kafkaContainerStoppedExternallyThrowsOnResume() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(container));

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean paused = new AtomicBoolean(false);
        when(container.isRunning()).thenAnswer(inv -> running.get());
        when(container.isPauseRequested()).thenAnswer(inv -> paused.get());
        when(container.isContainerPaused()).thenAnswer(inv -> paused.get());

        doAnswer(inv -> { paused.set(true); return null; }).when(container).pause();

        KafkaConsumerLifecycleManager kafkaManager =
                new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(200));

        kafkaManager.stopConsumers();

        // External disruption: container is stopped while maintenance is active
        running.set(false);

        // Resume must fail fast and report external container stop
        assertThatThrownBy(kafkaManager::startConsumers)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Maintenance-owned Kafka container stopped externally");
    }

    @Test
    @DisplayName("Abnormal: Broker timeout during stop Consumers throws clear timeout exception")
    @SuppressWarnings("unchecked")
    void kafkaContainerTimeoutThrowsIllegalStateException() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(container));

        when(container.isRunning()).thenReturn(true);
        when(container.isPauseRequested()).thenReturn(false, true);
        when(container.isContainerPaused()).thenReturn(false);
        // Container has active partition assignment but never pauses
        when(container.getAssignedPartitions()).thenReturn(List.of(new org.apache.kafka.common.TopicPartition("topic", 0)));

        KafkaConsumerLifecycleManager kafkaManager =
                new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(50));

        assertThatThrownBy(kafkaManager::stopConsumers)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka pause/resume acknowledgement timed out");
    }

    @Test
    @DisplayName("Concurrency: Concurrent stop/start calls on QueueMaintenanceManager are thread-safe")
    void concurrentQueueManagementIsThreadSafe() throws InterruptedException {
        ConsumerLifecycleManager kafka = mock(ConsumerLifecycleManager.class);
        when(kafka.name()).thenReturn("kafka");
        when(kafka.stopConsumers()).thenReturn(Map.of("kafkaPaused", true));
        when(kafka.startConsumers()).thenReturn(Map.of("kafkaResumed", true));

        ConsumerLifecycleManager rabbit = mock(ConsumerLifecycleManager.class);
        when(rabbit.name()).thenReturn("rabbit");
        when(rabbit.stopConsumers()).thenReturn(Map.of("rabbitStopped", true));
        when(rabbit.startConsumers()).thenReturn(Map.of("rabbitStarted", true));

        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of(kafka, rabbit));

        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final boolean isStop = (i % 2 == 0);
            pool.submit(() -> {
                try {
                    startGate.await();
                    Map<String, Object> result = isStop ? queueManager.stopConsumers() : queueManager.startConsumers();
                    if (result != null && result.containsKey("kafka") && result.containsKey("rabbit")) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception ex) {
                    // unexpected error
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean completed = endGate.await(5, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(completed).isTrue();
        assertThat(successCount.get()).isEqualTo(threads);
    }
}
