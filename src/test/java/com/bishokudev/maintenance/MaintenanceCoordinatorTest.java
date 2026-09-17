package com.bishokudev.maintenance;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.TrafficDrainHandler;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MaintenanceCoordinatorTest {

    private MaintenanceState state;
    private KubernetesReadinessManager readinessManager;
    private QueueMaintenanceManager queueManager;
    private ApplicationEventPublisher eventPublisher;
    private MaintenanceHook hook;
    private MaintenanceCoordinator coordinator;
    private MaintenanceProperties properties;
    private TrafficDrainHandler drainHandler;
    private MaintenanceHookExecutor hookExecutor;

    @BeforeEach
    void setUp() {
        state = new MaintenanceState();
        readinessManager = mock(KubernetesReadinessManager.class);
        queueManager = mock(QueueMaintenanceManager.class);
        eventPublisher = mock(ApplicationEventPublisher.class);
        hook = mock(MaintenanceHook.class);

        when(queueManager.stopConsumers()).thenReturn(Map.of("kafkaStopped", true));
        when(queueManager.startConsumers()).thenReturn(Map.of("kafkaStarted", true));

        properties = new MaintenanceProperties();
        properties.setDrainDelay(Duration.ZERO);
        properties.setHookTimeout(Duration.ofSeconds(5));

        drainHandler = new TrafficDrainHandler(readinessManager);
        hookExecutor = new MaintenanceHookExecutor(List.of(hook), properties.getHookTimeout());

        coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager,
                hookExecutor, eventPublisher, properties
        );
    }

    @Test
    @DisplayName("Should execute enter maintenance in correct sequence")
    void shouldExecuteEnterMaintenanceInSequence() {
        boolean changed = coordinator.setMaintenanceMode(true, "Deploying release v2.0");

        assertThat(changed).isTrue();
        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.getReason()).isEqualTo("Deploying release v2.0");

        InOrder inOrder = inOrder(readinessManager, queueManager, hook, eventPublisher);
        inOrder.verify(readinessManager).refuseTraffic();
        inOrder.verify(queueManager).stopConsumers();
        inOrder.verify(hook).onEnterMaintenance();
        inOrder.verify(eventPublisher).publishEvent(any(MaintenanceModeChangedEvent.class));
    }

    @Test
    @DisplayName("Entering maintenance is idempotent")
    void enteringMaintenanceIsIdempotent() {
        assertThat(coordinator.setMaintenanceMode(true, "First")).isTrue();
        assertThat(coordinator.setMaintenanceMode(true, "Second")).isFalse();

        verify(readinessManager, times(1)).refuseTraffic();
        verify(queueManager, times(1)).stopConsumers();
    }

    @Test
    @DisplayName("Should execute exit maintenance in correct sequence")
    void shouldExecuteExitMaintenanceInSequence() {
        coordinator.setMaintenanceMode(true, "Initial");
        reset(readinessManager, queueManager, hook, eventPublisher);
        when(queueManager.startConsumers()).thenReturn(Map.of("kafkaStarted", true));

        boolean changed = coordinator.setMaintenanceMode(false, "Complete");

        assertThat(changed).isTrue();
        assertThat(state.isMaintenanceActive()).isFalse();

        InOrder inOrder = inOrder(queueManager, hook, readinessManager, eventPublisher);
        inOrder.verify(queueManager).startConsumers();
        inOrder.verify(hook).onExitMaintenance();
        inOrder.verify(readinessManager).acceptTraffic();
        inOrder.verify(eventPublisher).publishEvent(any(MaintenanceModeChangedEvent.class));
    }

    @Test
    @DisplayName("Exiting maintenance is idempotent")
    void exitingMaintenanceIsIdempotent() {
        assertThat(coordinator.setMaintenanceMode(false, "Already inactive")).isFalse();
        verifyNoInteractions(readinessManager, queueManager, hook, eventPublisher);
    }

    @Test
    @DisplayName("Hook failure does not prevent transition or event publication")
    void hookFailureDoesNotPreventTransition() {
        doThrow(new RuntimeException("Boom")).when(hook).onEnterMaintenance();

        boolean changed = coordinator.setMaintenanceMode(true, "Faulty hook");

        assertThat(changed).isTrue();
        assertThat(state.isMaintenanceActive()).isTrue();
        verify(eventPublisher).publishEvent(any(MaintenanceModeChangedEvent.class));
    }

    @Test
    @DisplayName("Published event should contain correct entering flag and reason")
    void publishedEventShouldContainCorrectData() {
        coordinator.setMaintenanceMode(true, "Testing event");

        ArgumentCaptor<MaintenanceModeChangedEvent> captor =
                ArgumentCaptor.forClass(MaintenanceModeChangedEvent.class);
        verify(eventPublisher).publishEvent(captor.capture());

        MaintenanceModeChangedEvent event = captor.getValue();
        assertThat(event.isEntering()).isTrue();
        assertThat(event.getReason()).isEqualTo("Testing event");
        assertThat(event.getTransitionTimestamp()).isNotNull();
        assertThat(event.getDetails()).isNotEmpty();
    }

    @Test
    @DisplayName("Should handle null reason gracefully")
    void shouldHandleNullReason() {
        boolean changed = coordinator.setMaintenanceMode(true, null);

        assertThat(changed).isTrue();
        assertThat(state.getReason()).isEmpty();
    }

    @Test
    @DisplayName("Readiness manager failure should not prevent transition")
    void readinessFailureShouldNotPreventTransition() {
        doThrow(new RuntimeException("K8s unreachable")).when(readinessManager).refuseTraffic();

        boolean changed = coordinator.setMaintenanceMode(true, "Resilience test");

        assertThat(changed).isTrue();
        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.getDetails()).containsKey("readinessError");
    }

    @Test
    @DisplayName("Queue manager failure should not prevent transition")
    void queueFailureShouldNotPreventTransition() {
        when(queueManager.stopConsumers()).thenThrow(new RuntimeException("Broker down"));

        boolean changed = coordinator.setMaintenanceMode(true, "Resilience test");

        assertThat(changed).isTrue();
        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.getDetails()).containsKey("queueError");
    }

    @Test
    @DisplayName("Hook report should include partial failure details")
    void hookReportShouldIncludePartialFailureDetails() {
        MaintenanceHook goodHook = mock(MaintenanceHook.class);
        MaintenanceHook badHook = mock(MaintenanceHook.class);
        doThrow(new RuntimeException("Hook failed")).when(badHook).onEnterMaintenance();

        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(List.of(goodHook, badHook), Duration.ofSeconds(5));
        TrafficDrainHandler handler = new TrafficDrainHandler(readinessManager);

        MaintenanceCoordinator coord = new MaintenanceCoordinator(
                new MaintenanceState(), handler, queueManager,
                executor, eventPublisher, properties
        );

        coord.setMaintenanceMode(true, "Partial failure test");

        verify(goodHook).onEnterMaintenance();
        verify(badHook).onEnterMaintenance();
    }

    @Test
    @DisplayName("When readiness is disabled, readiness transitions and drain delay should be skipped")
    void shouldSkipReadinessWhenDisabled() {
        properties.getReadiness().setEnabled(false);

        boolean entered = coordinator.setMaintenanceMode(true, "Skip readiness enter");
        assertThat(entered).isTrue();
        assertThat(state.getDetails()).containsEntry("readiness", "DISABLED");
        verify(readinessManager, never()).refuseTraffic();

        boolean exited = coordinator.setMaintenanceMode(false, "Skip readiness exit");
        assertThat(exited).isTrue();
        assertThat(state.getDetails()).containsEntry("readiness", "DISABLED");
        verify(readinessManager, never()).acceptTraffic();
    }

    @Test
    @DisplayName("When queues is disabled, queue listener stop and start should be skipped")
    void shouldSkipQueuesWhenDisabled() {
        properties.getQueues().setEnabled(false);

        boolean entered = coordinator.setMaintenanceMode(true, "Skip queues enter");
        assertThat(entered).isTrue();
        assertThat(state.getDetails().get("queues")).isEqualTo(Map.of("enabled", false));
        verify(queueManager, never()).stopConsumers();

        boolean exited = coordinator.setMaintenanceMode(false, "Skip queues exit");
        assertThat(exited).isTrue();
        assertThat(state.getDetails().get("queues")).isEqualTo(Map.of("enabled", false));
        verify(queueManager, never()).startConsumers();
    }

    @Test
    @DisplayName("When hooks is disabled, custom hook execution should be skipped")
    void shouldSkipHooksWhenDisabled() {
        properties.getHooks().setEnabled(false);

        boolean entered = coordinator.setMaintenanceMode(true, "Skip hooks enter");
        assertThat(entered).isTrue();
        // When hooks are disabled, no "hooks" key is added to the report
        verify(hook, never()).onEnterMaintenance();

        boolean exited = coordinator.setMaintenanceMode(false, "Skip hooks exit");
        assertThat(exited).isTrue();
        verify(hook, never()).onExitMaintenance();
    }

    @Test
    @DisplayName("When events is disabled, ApplicationEvent publication should be skipped")
    void shouldSkipEventsWhenDisabled() {
        properties.getEvents().setEnabled(false);

        boolean entered = coordinator.setMaintenanceMode(true, "Skip events enter");
        assertThat(entered).isTrue();
        verify(eventPublisher, never()).publishEvent(any(MaintenanceModeChangedEvent.class));

        boolean exited = coordinator.setMaintenanceMode(false, "Skip events exit");
        assertThat(exited).isTrue();
        verify(eventPublisher, never()).publishEvent(any(MaintenanceModeChangedEvent.class));
    }
}
