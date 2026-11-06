package com.bishokudev.maintenance.adversarial;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
import com.bishokudev.maintenance.actuator.MaintenanceStatusResponse;
import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.TrafficDrainHandler;
import com.bishokudev.maintenance.model.MaintenanceSnapshot;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Adversarial test suite stress testing the Actuator endpoint and coordinator:
 * repeated dynamic exit and re-entry cycles, concurrent duplicate exits,
 * conflicting concurrent transitions, and edge-case request payloads.
 */
public class ActuatorDynamicExitStressTest {

    private MaintenanceState state;
    private KubernetesReadinessManager readinessManager;
    private TrafficDrainHandler drainHandler;
    private QueueMaintenanceManager queueManager;
    private MaintenanceHookExecutor hookExecutor;
    private ApplicationEventPublisher eventPublisher;
    private MaintenanceProperties properties;
    private MaintenanceCoordinator coordinator;
    private MaintenanceActuatorEndpoint endpoint;

    @BeforeEach
    void setUp() {
        properties = new MaintenanceProperties();
        properties.setStartInMaintenance(true);
        properties.setDrainDelay(Duration.ZERO);

        state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup cold boot"));
        readinessManager = mock(KubernetesReadinessManager.class);
        drainHandler = new TrafficDrainHandler(readinessManager);
        queueManager = mock(QueueMaintenanceManager.class);
        when(queueManager.stopConsumers()).thenReturn(Map.of("mockQueue", "stopped"));
        when(queueManager.startConsumers()).thenReturn(Map.of("mockQueue", "resumed"));

        hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        eventPublisher = mock(ApplicationEventPublisher.class);

        coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, eventPublisher, properties);
        endpoint = new MaintenanceActuatorEndpoint(coordinator);
    }

    @Test
    @DisplayName("Lifecycle Stress: 10 repeated back-to-back dynamic exit and re-entry cycles")
    void repeatedExitAndReentryCycles() {
        for (int cycle = 1; cycle <= 10; cycle++) {
            // 1. Exit
            MaintenanceStatusResponse exitResp = endpoint.setStatus(false, "Dynamic exit cycle " + cycle);
            assertThat(exitResp.active()).isFalse();
            assertThat(state.getPhase()).isEqualTo("INACTIVE");
            assertThat(state.isWorkBlocked()).isFalse();

            // 2. Re-enter
            MaintenanceStatusResponse enterResp = endpoint.setStatus(true, "Dynamic enter cycle " + cycle);
            assertThat(enterResp.active()).isTrue();
            assertThat(state.getPhase()).isEqualTo("ACTIVE");
            assertThat(state.isWorkBlocked()).isTrue();
        }

        verify(queueManager, times(10)).startConsumers();
        verify(queueManager, times(10)).stopConsumers();
    }

    @Test
    @DisplayName("Concurrency: 20 simultaneous duplicate dynamic exit calls execute safely and idempotently")
    void concurrentDuplicateExitRequests() throws InterruptedException {
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);

        AtomicInteger activeFalseResponses = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    MaintenanceStatusResponse resp = endpoint.setStatus(false, "Concurrent duplicate exit");
                    if (!resp.active()) {
                        activeFalseResponses.incrementAndGet();
                    }
                } catch (Exception ex) {
                    exceptionCount.incrementAndGet();
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean completed = endGate.await(5, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(completed).isTrue();
        assertThat(exceptionCount.get()).isEqualTo(0);
        assertThat(activeFalseResponses.get()).isEqualTo(threads);
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        // startConsumers must be executed exactly once despite 20 concurrent exit callers
        verify(queueManager, times(1)).startConsumers();
    }

    @Test
    @DisplayName("Concurrency: Racing exit vs enter calls resolve cleanly without illegal state")
    void racingExitAndEnterTransitions() throws InterruptedException {
        int pairs = 10;
        ExecutorService pool = Executors.newFixedThreadPool(pairs * 2);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(pairs * 2);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < pairs; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    endpoint.setStatus(false, "Racing exit");
                } catch (Exception ex) {
                    errors.incrementAndGet();
                } finally {
                    endGate.countDown();
                }
            });
            pool.submit(() -> {
                try {
                    startGate.await();
                    endpoint.setStatus(true, "Racing enter");
                } catch (Exception ex) {
                    errors.incrementAndGet();
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean completed = endGate.await(5, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(completed).isTrue();
        assertThat(errors.get()).isEqualTo(0);
        // State must be either ACTIVE or INACTIVE, never corrupt or in intermediate phase
        assertThat(state.getPhase()).isIn("ACTIVE", "INACTIVE");
        assertThat(state.isMaintenanceActive()).isEqualTo("ACTIVE".equals(state.getPhase()));
    }

    @Test
    @DisplayName("Payload edge cases: null, empty string, whitespace, unicode reasons on exit")
    void exitWithEdgeCasePayloads() {
        // Null reason
        MaintenanceStatusResponse resp1 = endpoint.setStatus(false, null);
        assertThat(resp1.active()).isFalse();

        // Empty reason on re-enter
        MaintenanceStatusResponse resp2 = endpoint.setStatus(true, "");
        assertThat(resp2.active()).isTrue();

        // Whitespace reason on exit
        MaintenanceStatusResponse resp3 = endpoint.setStatus(false, "   \t\n  ");
        assertThat(resp3.active()).isFalse();

        // Unicode reason on re-enter
        MaintenanceStatusResponse resp4 = endpoint.setStatus(true, "メンテナンス中 🚀 — Maintenance window");
        assertThat(resp4.active()).isTrue();
        assertThat(resp4.reason()).isEqualTo("メンテナンス中 🚀 — Maintenance window");
    }
}
