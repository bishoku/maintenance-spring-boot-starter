package com.bishokudev.maintenance.adversarial;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.TrafficDrainHandler;
import com.bishokudev.maintenance.model.MaintenanceSnapshot;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Adversarial stress harness verifying Requirement R2:
 * 1. Spring's @Scheduled infrastructure is not paused or tampered with.
 * 2. Scheduled tasks continue ticking periodically during startup maintenance mode.
 * 3. Lock-free AtomicReference read access sustains 100,000+ concurrent state checks without contention,
 *    deadlock, or thread starvation.
 */
public class ScheduledTaskExecutionStressTest {

    @Test
    @DisplayName("R2 Verification: Multiple scheduled tasks tick without starvation during maintenance and transition")
    void scheduledTasksTickContinuouslyAndTransition() throws Exception {
        MaintenanceProperties properties = new MaintenanceProperties();
        properties.setStartInMaintenance(true);
        properties.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Scheduled task stress"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);
        when(queueManager.stopConsumers()).thenReturn(Map.of());
        when(queueManager.startConsumers()).thenReturn(Map.of());
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, eventPublisher, properties);

        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("scheduled-task-test-");
        scheduler.initialize();

        AtomicInteger task1BlockedTicks = new AtomicInteger(0);
        AtomicInteger task1ActiveTicks = new AtomicInteger(0);
        AtomicInteger task2BlockedTicks = new AtomicInteger(0);
        AtomicInteger task2ActiveTicks = new AtomicInteger(0);

        // Schedule task 1 (10ms rate)
        scheduler.scheduleAtFixedRate(() -> {
            if (state.isWorkBlocked()) {
                task1BlockedTicks.incrementAndGet();
            } else {
                task1ActiveTicks.incrementAndGet();
            }
        }, Duration.ofMillis(10));

        // Schedule task 2 (15ms rate)
        scheduler.scheduleAtFixedRate(() -> {
            if (state.isWorkBlocked()) {
                task2BlockedTicks.incrementAndGet();
            } else {
                task2ActiveTicks.incrementAndGet();
            }
        }, Duration.ofMillis(15));

        // Let tasks tick while maintenance is ACTIVE
        Thread.sleep(150);

        assertThat(task1BlockedTicks.get()).isGreaterThan(5);
        assertThat(task2BlockedTicks.get()).isGreaterThan(5);
        assertThat(task1ActiveTicks.get()).isEqualTo(0);
        assertThat(task2ActiveTicks.get()).isEqualTo(0);

        // Perform dynamic exit via coordinator
        coordinator.setMaintenanceMode(false, "Exit for scheduled task transition");

        // Let tasks tick after exit
        Thread.sleep(150);

        scheduler.shutdown();

        assertThat(task1ActiveTicks.get()).isGreaterThan(5);
        assertThat(task2ActiveTicks.get()).isGreaterThan(5);
    }

    @Test
    @DisplayName("High-Throughput: 50 concurrent threads query isWorkBlocked 100,000 times during active transitions")
    void highThroughputLockFreeStateQueries() throws InterruptedException {
        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Initial startup"));

        int threads = 50;
        int iterations = 2000; // 50 * 2000 = 100,000 queries
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);
        AtomicInteger totalReads = new AtomicInteger(0);
        AtomicBoolean transitionDone = new AtomicBoolean(false);

        // Worker thread toggles state back and forth
        pool.submit(() -> {
            try {
                startGate.await();
                for (int i = 0; i < 50; i++) {
                    state.update(i % 2 == 0, i % 2 == 0 ? "ACTIVE" : "INACTIVE", "toggle " + i, Map.of());
                    Thread.sleep(2);
                }
            } catch (Exception ignored) {
            } finally {
                transitionDone.set(true);
            }
        });

        // 50 reader threads hammering isWorkBlocked()
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < iterations; j++) {
                        // Verify snapshot atomicity: snapshot active flag and phase are always internally consistent
                        MaintenanceSnapshot snap = state.getSnapshot();
                        String phase = (String) snap.details().get("phase");
                        if (phase != null) {
                            if ("INACTIVE".equals(phase)) {
                                assertThat(snap.active()).isFalse();
                            } else if ("ACTIVE".equals(phase)) {
                                assertThat(snap.active()).isTrue();
                            }
                        }
                        totalReads.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean finished = endGate.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(finished).isTrue();
        assertThat(totalReads.get()).isEqualTo(threads * iterations);
    }
}
