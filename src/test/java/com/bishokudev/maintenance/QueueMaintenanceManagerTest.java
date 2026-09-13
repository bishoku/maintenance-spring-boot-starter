package com.bishokudev.maintenance;

import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class QueueMaintenanceManagerTest {

    @Test
    @DisplayName("Should handle empty manager list gracefully")
    void shouldHandleEmptyManagerList() {
        QueueMaintenanceManager manager = new QueueMaintenanceManager(Collections.emptyList());

        Map<String, Object> stopReport = manager.stopConsumers();
        assertThat(stopReport).containsKey("message");

        Map<String, Object> startReport = manager.startConsumers();
        assertThat(startReport).containsKey("message");
    }

    @Test
    @DisplayName("Should handle null manager list gracefully")
    void shouldHandleNullManagerList() {
        QueueMaintenanceManager manager = new QueueMaintenanceManager(null);

        Map<String, Object> report = manager.stopConsumers();
        assertThat(report).isNotEmpty();
    }

    @Test
    @DisplayName("Should delegate stop to all registered managers")
    void shouldDelegateStopToAllManagers() {
        ConsumerLifecycleManager kafka = mock(ConsumerLifecycleManager.class);
        when(kafka.name()).thenReturn("kafka");
        when(kafka.stopConsumers()).thenReturn(Map.of("kafkaStopped", true));

        ConsumerLifecycleManager rabbit = mock(ConsumerLifecycleManager.class);
        when(rabbit.name()).thenReturn("rabbit");
        when(rabbit.stopConsumers()).thenReturn(Map.of("rabbitStopped", true));

        QueueMaintenanceManager manager = new QueueMaintenanceManager(List.of(kafka, rabbit));
        Map<String, Object> report = manager.stopConsumers();

        verify(kafka).stopConsumers();
        verify(rabbit).stopConsumers();
        assertThat(report).containsKeys("kafka", "rabbit");
    }

    @Test
    @DisplayName("Should delegate start to all registered managers")
    void shouldDelegateStartToAllManagers() {
        ConsumerLifecycleManager kafka = mock(ConsumerLifecycleManager.class);
        when(kafka.name()).thenReturn("kafka");
        when(kafka.startConsumers()).thenReturn(Map.of("kafkaStarted", true));

        QueueMaintenanceManager manager = new QueueMaintenanceManager(List.of(kafka));
        Map<String, Object> report = manager.startConsumers();

        verify(kafka).startConsumers();
        assertThat(report).containsKey("kafka");
    }

    @Test
    @DisplayName("Should isolate errors from individual managers")
    void shouldIsolateManagerErrors() {
        ConsumerLifecycleManager failingManager = mock(ConsumerLifecycleManager.class);
        when(failingManager.name()).thenReturn("faulty");
        when(failingManager.stopConsumers()).thenThrow(new RuntimeException("Connection refused"));

        ConsumerLifecycleManager healthyManager = mock(ConsumerLifecycleManager.class);
        when(healthyManager.name()).thenReturn("healthy");
        when(healthyManager.stopConsumers()).thenReturn(Map.of("stopped", true));

        QueueMaintenanceManager manager = new QueueMaintenanceManager(List.of(failingManager, healthyManager));
        Map<String, Object> report = manager.stopConsumers();

        verify(healthyManager).stopConsumers();
        assertThat(report).containsKey("faulty");
        assertThat(report).containsKey("healthy");
    }
}
