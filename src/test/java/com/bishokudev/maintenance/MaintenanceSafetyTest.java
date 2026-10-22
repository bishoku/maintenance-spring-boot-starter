package com.bishokudev.maintenance;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.hook.*;
import com.bishokudev.maintenance.manager.*;
import com.bishokudev.maintenance.model.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class MaintenanceSafetyTest {
    @Test void timedOutHookCannotEnqueueLaterHooksOrPermitExitWhileStillRunning() throws Exception {
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger next = new AtomicInteger();
        MaintenanceHook stuck = new MaintenanceHook() {
            public void onEnterMaintenance() {
                started.countDown();
                while (release.getCount() != 0) {
                    try { release.await(); } catch (InterruptedException ignored) { /* intentionally hostile hook */ }
                }
            }
        };
        MaintenanceHook second = new MaintenanceHook() { public void onEnterMaintenance() { next.incrementAndGet(); } };
        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(List.of(stuck, second), Duration.ofMillis(100));
        try {
            assertThat(executor.execute(true).timedOut()).isEqualTo(1);
            assertThat(started.getCount()).isZero();
            assertThat(executor.isIdle()).isFalse();
            assertThat(executor.execute(false).partialFailure()).isTrue();
            assertThat(next).hasValue(0);
        } finally { release.countDown(); executor.destroy(); }
        assertThat(next).hasValue(0);
    }

    @Test void nestedBrokerErrorsAndInterruptedDrainAreFailures() {
        assertThat(TransitionReport.builder().queues(Map.of("rabbit",Map.of("error","failure"))).build().partialFailure()).isTrue();
        assertThat(TransitionReport.builder().drainDelayInterrupted(true).build().partialFailure()).isTrue();
        ConsumerLifecycleManager manager = mock(ConsumerLifecycleManager.class);
        when(manager.name()).thenReturn("broker");
        when(manager.stopConsumers()).thenThrow(new RuntimeException());
        assertThat(new QueueMaintenanceManager(List.of(manager)).stopConsumers()).containsKey("broker");
    }

    @Test void failureRetainsRefusingReadinessAndAllowsRetry() {
        MaintenanceProperties props = new MaintenanceProperties(); props.setDrainDelay(Duration.ZERO);
        MaintenanceState state = new MaintenanceState();
        KubernetesReadinessManager readiness = mock(KubernetesReadinessManager.class);
        AtomicBoolean fail = new AtomicBoolean(true);
        MaintenanceHook hook = new MaintenanceHook() {
            public void onEnterMaintenance() { if(fail.get()) throw new IllegalStateException("failure"); }
        };
        MaintenanceHookExecutor hooks = new MaintenanceHookExecutor(List.of(hook), Duration.ofSeconds(1));
        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(state,
                new TrafficDrainHandler(readiness),
                new QueueMaintenanceManager(List.of()),hooks,event -> {},props);
        try {
            assertThat(coordinator.setMaintenanceMode(true,"test")).isFalse();
            assertThat(state.getPhase()).isEqualTo("FAILED");
            assertThat(state.isMaintenanceActive()).isFalse();
            assertThat(state.isWorkBlocked()).isTrue();
            verify(readiness, never()).acceptTraffic();
            fail.set(false);
            assertThat(coordinator.setMaintenanceMode(true,"retry")).isTrue();
            assertThat(state.getPhase()).isEqualTo("ACTIVE");
            assertThat(coordinator.setMaintenanceMode(false,"exit")).isTrue();
            verify(readiness).acceptTraffic();
        } finally { hooks.destroy(); }
    }

    @Test void failedResumeIsCompensatedAndErrorNotLost() {
        MaintenanceProperties props = new MaintenanceProperties(); props.setDrainDelay(Duration.ZERO);
        MaintenanceState state = new MaintenanceState();
        KubernetesReadinessManager readiness = mock(KubernetesReadinessManager.class);
        QueueMaintenanceManager queues = mock(QueueMaintenanceManager.class);
        when(queues.stopConsumers()).thenReturn(Map.of("broker",Map.of("stopped",true)));
        when(queues.startConsumers()).thenReturn(Map.of("broker",Map.of("error","resume failed")));
        MaintenanceHookExecutor hooks = new MaintenanceHookExecutor(List.of(),Duration.ofSeconds(1));
        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(state,
                new TrafficDrainHandler(readiness),queues,hooks,e -> {},props);
        try {
            assertThat(coordinator.setMaintenanceMode(true,"enter")).isTrue();
            assertThat(coordinator.setMaintenanceMode(false,"exit")).isFalse();
            assertThat(state.getPhase()).isEqualTo("FAILED");
            assertThat(state.getDetails()).containsEntry("partialFailure",true);
            verify(readiness, never()).acceptTraffic();
            assertThat(((Map<?, ?>) ((Map<?, ?>) state.getDetails().get("queues")).get("broker")).get("error"))
                    .isEqualTo("resume failed");
            verify(queues,times(2)).stopConsumers();
        } finally { hooks.destroy(); }
    }

    @Test @SuppressWarnings("unchecked") void kafkaResumesOnlyContainersPausedByMaintenance() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer active = mock(MessageListenerContainer.class), idle = mock(MessageListenerContainer.class), external = mock(MessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(active,idle,external));
        AtomicBoolean paused = new AtomicBoolean();
        when(active.isRunning()).thenReturn(true);
        when(active.isPauseRequested()).thenAnswer(i -> paused.get());
        when(active.isContainerPaused()).thenAnswer(i -> paused.get());
        doAnswer(i -> { paused.set(true); return null; }).when(active).pause();
        doAnswer(i -> { paused.set(false); return null; }).when(active).resume();
        when(external.isRunning()).thenReturn(true); when(external.isPauseRequested()).thenReturn(true); when(external.isContainerPaused()).thenReturn(true);
        KafkaConsumerLifecycleManager manager = new KafkaConsumerLifecycleManager(provider,Duration.ofMillis(100));
        manager.stopConsumers(); manager.stopConsumers(); manager.startConsumers();
        verify(active,times(1)).pause(); verify(active,times(1)).resume();
        verify(idle,never()).start(); verify(external,never()).resume(); verify(active,never()).stop();
    }

    @Test @SuppressWarnings("unchecked") void kafkaRequiresPauseAcknowledgement() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer active = mock(MessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry); when(registry.getListenerContainers()).thenReturn(List.of(active));
        when(active.isRunning()).thenReturn(true);
        when(active.isPauseRequested()).thenReturn(true);
        when(active.getAssignedPartitions()).thenReturn(List.of(new org.apache.kafka.common.TopicPartition("test", 0)));
        KafkaConsumerLifecycleManager manager = new KafkaConsumerLifecycleManager(provider,Duration.ofMillis(20));
        assertThatThrownBy(manager::stopConsumers).isInstanceOf(IllegalStateException.class).hasMessageContaining("acknowledgement");
    }
    @Test @SuppressWarnings("unchecked") void kafkaUnassignedConsumerKeepsPauseUntilResume() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isRunning()).thenReturn(true);
        when(container.getAssignedPartitions()).thenReturn(List.of());
        AtomicBoolean requested = new AtomicBoolean();
        when(container.isPauseRequested()).thenAnswer(i -> requested.get());
        doAnswer(i -> { requested.set(true); return null; }).when(container).pause();
        doAnswer(i -> { requested.set(false); return null; }).when(container).resume();
        KafkaConsumerLifecycleManager manager = new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(20));
        manager.stopConsumers();
        assertThat(requested).isTrue();
        manager.startConsumers();
        assertThat(requested).isFalse();
        verify(container, never()).stop();
    }

    @Test @SuppressWarnings({"unchecked", "rawtypes"}) void kafkaConcurrentContainerAcceptsUnassignedChildButWaitsForAssignedChild() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        var parent = mock(org.springframework.kafka.listener.ConcurrentMessageListenerContainer.class);
        var empty = mock(org.springframework.kafka.listener.KafkaMessageListenerContainer.class);
        var assigned = mock(org.springframework.kafka.listener.KafkaMessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(parent));
        when(parent.isRunning()).thenReturn(true);
        when(parent.isPauseRequested()).thenReturn(true);
        when(parent.getContainers()).thenReturn(List.of(empty, assigned));
        when(empty.isRunning()).thenReturn(true);
        when(empty.isPauseRequested()).thenReturn(true);
        when(empty.getAssignedPartitions()).thenReturn(List.of());
        when(assigned.isRunning()).thenReturn(true);
        when(assigned.isPauseRequested()).thenReturn(true);
        when(assigned.getAssignedPartitions()).thenReturn(List.of(new org.apache.kafka.common.TopicPartition("test", 0)));
        KafkaConsumerLifecycleManager manager = new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(20));
        assertThatThrownBy(manager::stopConsumers).hasMessageContaining("acknowledgement");
        when(assigned.isContainerPaused()).thenReturn(true);
        assertThatCode(manager::stopConsumers).doesNotThrowAnyException();
    }

    @Test @SuppressWarnings({"unchecked", "rawtypes"}) void kafkaResumeWaitsForEveryConcurrentChild() {
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        var parent = mock(org.springframework.kafka.listener.ConcurrentMessageListenerContainer.class);
        var paused = mock(org.springframework.kafka.listener.KafkaMessageListenerContainer.class);
        var unpaused = mock(org.springframework.kafka.listener.KafkaMessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(parent));
        when(parent.isRunning()).thenReturn(true);
        when(parent.isContainerPaused()).thenReturn(true);
        when(parent.getContainers()).thenReturn(List.of(paused, unpaused));
        when(paused.isContainerPaused()).thenReturn(true);
        KafkaConsumerLifecycleManager manager = new KafkaConsumerLifecycleManager(provider, Duration.ofMillis(20));
        manager.stopConsumers();
        when(parent.isContainerPaused()).thenReturn(false); // aggregate false despite one paused child
        assertThatThrownBy(manager::startConsumers).hasMessageContaining("acknowledgement");
        when(paused.isContainerPaused()).thenReturn(false);
        assertThatCode(manager::startConsumers).doesNotThrowAnyException();
        verify(parent, times(2)).resume();
    }

    @Test @SuppressWarnings("unchecked") void rabbitPreservesIdleContainersAndCompensatesPartialResume() {
        ObjectProvider<org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        var registry = mock(org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry.class);
        var active = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        var idle = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        var failing = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry);
        when(registry.getListenerContainers()).thenReturn(List.of(active,idle,failing));
        AtomicBoolean running = new AtomicBoolean(true), failingRunning = new AtomicBoolean(true);
        when(active.isRunning()).thenAnswer(i -> running.get());
        when(failing.isRunning()).thenAnswer(i -> failingRunning.get());
        doAnswer(i -> { running.set(false); ((Runnable)i.getArgument(0)).run(); return null; }).when(active).stop(any(Runnable.class));
        doAnswer(i -> { failingRunning.set(false); ((Runnable)i.getArgument(0)).run(); return null; }).when(failing).stop(any(Runnable.class));
        doAnswer(i -> { running.set(true); return null; }).when(active).start();
        doThrow(new IllegalStateException("restart failed")).when(failing).start();
        RabbitConsumerLifecycleManager manager = new RabbitConsumerLifecycleManager(provider,Duration.ofMillis(100));
        manager.stopConsumers();
        assertThatThrownBy(manager::startConsumers).hasMessageContaining("restart failed");
        manager.stopConsumers();
        assertThat(running).isFalse();
        verify(idle,never()).start(); verify(idle,never()).stop(any(Runnable.class));
        verify(active,times(2)).stop(any(Runnable.class));
    }
    @Test @SuppressWarnings("unchecked") void rabbitCannotRestartBeforeStopCallback() {
        ObjectProvider<org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        var registry = mock(org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry.class);
        var active = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        when(provider.getIfAvailable()).thenReturn(registry); when(registry.getListenerContainers()).thenReturn(List.of(active));
        when(active.isRunning()).thenReturn(true);
        RabbitConsumerLifecycleManager manager = new RabbitConsumerLifecycleManager(provider,Duration.ofMillis(10));
        assertThatThrownBy(manager::stopConsumers).hasMessageContaining("acknowledgement");
        assertThatThrownBy(manager::startConsumers).hasMessageContaining("still in progress");
        verify(active,never()).start();
    }

    @Test @SuppressWarnings("unchecked") void rabbitThrowingStopDoesNotLoseOwnership() {
        ObjectProvider<org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        var registry = mock(org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry.class);
        var active = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        AtomicBoolean running = new AtomicBoolean(true);
        when(provider.getIfAvailable()).thenReturn(registry); when(registry.getListenerContainers()).thenReturn(List.of(active));
        when(active.isRunning()).thenAnswer(i -> running.get());
        doAnswer(i -> { running.set(false); throw new IllegalStateException("stop failed before acknowledgement"); })
                .when(active).stop(any(Runnable.class));
        RabbitConsumerLifecycleManager manager = new RabbitConsumerLifecycleManager(provider,Duration.ofMillis(10));
        assertThatThrownBy(manager::stopConsumers).hasMessageContaining("stop failed");
        assertThatThrownBy(manager::stopConsumers).hasMessageContaining("acknowledgement");
        assertThatThrownBy(manager::startConsumers).hasMessageContaining("still in progress");
    }

    @Test void invalidDurationsAreRejected() {
        MaintenanceProperties props = new MaintenanceProperties();
        assertThatThrownBy(() -> props.setHookTimeout(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> props.setQueueTimeout(Duration.ofMillis(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> props.setDrainDelay(Duration.ofMillis(-1))).isInstanceOf(IllegalArgumentException.class);
    }
}
