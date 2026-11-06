package com.bishokudev.maintenance;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceStartupInitializer;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.model.MaintenanceSnapshot;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MaintenanceStartupInitializerTest {

    private MaintenanceProperties properties;
    private MaintenanceState state;
    private KubernetesReadinessManager readinessManager;
    private QueueMaintenanceManager queueManager;
    private ApplicationEventPublisher eventPublisher;
    private MaintenanceStartupInitializer initializer;

    @BeforeEach
    void setUp() {
        properties = new MaintenanceProperties();
        state = new MaintenanceState();
        readinessManager = mock(KubernetesReadinessManager.class);
        queueManager = mock(QueueMaintenanceManager.class);
        eventPublisher = mock(ApplicationEventPublisher.class);
        when(queueManager.stopConsumers()).thenReturn(Map.of("kafka", Map.of("kafkaPaused", true)));

        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);
    }

    @Test
    @DisplayName("When start-in-maintenance is false, ApplicationStartedEvent does nothing")
    void shouldDoNothingOnStartedWhenDisabled() {
        ApplicationStartedEvent event = mock(ApplicationStartedEvent.class);

        initializer.onApplicationEvent(event);

        verifyNoInteractions(queueManager);
        verifyNoInteractions(readinessManager);
        assertThat(initializer.isQueuesPausedOnStartup()).isFalse();
    }

    @Test
    @DisplayName("When start-in-maintenance is true, ApplicationStartedEvent pauses queues and refuses traffic")
    void shouldPauseQueuesAndRefuseTrafficOnStartedWhenEnabled() {
        properties.setStartInMaintenance(true);
        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup maintenance"));
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        ApplicationStartedEvent event = mock(ApplicationStartedEvent.class);

        initializer.onApplicationEvent(event);

        verify(queueManager).stopConsumers();
        verify(readinessManager).refuseTraffic();
        assertThat(initializer.isQueuesPausedOnStartup()).isTrue();

        verify(eventPublisher).publishEvent(any(MaintenanceModeChangedEvent.class));
    }

    @Test
    @DisplayName("When queues are disabled in configuration, queues are not paused on startup")
    void shouldSkipQueuesWhenQueuesDisabled() {
        properties.setStartInMaintenance(true);
        properties.getQueues().setEnabled(false);
        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup maintenance"));
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        ApplicationStartedEvent event = mock(ApplicationStartedEvent.class);

        initializer.onApplicationEvent(event);

        verifyNoInteractions(queueManager);
        verify(readinessManager).refuseTraffic();
        assertThat(initializer.isQueuesPausedOnStartup()).isFalse();
    }

    @Test
    @DisplayName("When readiness is disabled in configuration, readiness is not refused on startup")
    void shouldSkipReadinessWhenReadinessDisabled() {
        properties.setStartInMaintenance(true);
        properties.getReadiness().setEnabled(false);
        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup maintenance"));
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        ApplicationStartedEvent event = mock(ApplicationStartedEvent.class);

        initializer.onApplicationEvent(event);

        verify(queueManager).stopConsumers();
        verifyNoInteractions(readinessManager);
    }

    @Test
    @DisplayName("Intercepts ACCEPTING_TRAFFIC and re-enforces REFUSING_TRAFFIC when maintenance is ACTIVE")
    void shouldInterceptAcceptingTrafficWhenActive() {
        properties.setStartInMaintenance(true);
        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup maintenance"));
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        AvailabilityChangeEvent<ReadinessState> event =
                new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC);

        initializer.onApplicationEvent(event);

        verify(readinessManager).refuseTraffic();
    }

    @Test
    @DisplayName("Permits ACCEPTING_TRAFFIC when phase is EXITING (Actuator exit transition)")
    void shouldPermitAcceptingTrafficWhenExiting() {
        properties.setStartInMaintenance(true);
        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup maintenance"));
        state.update(true, "EXITING", "Exiting maintenance", Map.of());
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        AvailabilityChangeEvent<ReadinessState> event =
                new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC);

        initializer.onApplicationEvent(event);

        verifyNoInteractions(readinessManager);
    }

    @Test
    @DisplayName("Permits ACCEPTING_TRAFFIC when phase is INACTIVE")
    void shouldPermitAcceptingTrafficWhenInactive() {
        AvailabilityChangeEvent<ReadinessState> event =
                new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC);

        initializer.onApplicationEvent(event);

        verifyNoInteractions(readinessManager);
    }

    @Test
    @DisplayName("Startup queue pausing is strictly idempotent across multiple startup events")
    void shouldBeIdempotentAcrossStartedAndReadyEvents() {
        properties.setStartInMaintenance(true);
        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup maintenance"));
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        ApplicationStartedEvent startedEvent = mock(ApplicationStartedEvent.class);
        ApplicationReadyEvent readyEvent = mock(ApplicationReadyEvent.class);

        initializer.onApplicationEvent(startedEvent);
        initializer.onApplicationEvent(readyEvent);

        verify(queueManager, times(1)).stopConsumers();
    }

    @Test
    @DisplayName("Re-enforces REFUSING_TRAFFIC if phase is FAILED")
    void shouldRefuseTrafficWhenFailed() {
        state.update(true, "FAILED", "Transition failed", Map.of());
        initializer = new MaintenanceStartupInitializer(
                properties, state, readinessManager, queueManager, eventPublisher);

        AvailabilityChangeEvent<ReadinessState> event =
                new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC);

        initializer.onApplicationEvent(event);

        verify(readinessManager).refuseTraffic();
    }
}
