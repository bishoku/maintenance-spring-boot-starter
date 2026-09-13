package com.bishokudev.maintenance;

import com.bishokudev.maintenance.model.MaintenanceSnapshot;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MaintenanceStateTest {

    @Test
    @DisplayName("Should initialize with default inactive snapshot")
    void shouldInitializeCorrectly() {
        MaintenanceState state = new MaintenanceState();
        MaintenanceSnapshot snap = state.getSnapshot();

        assertThat(snap.active()).isFalse();
        assertThat(snap.reason()).isEmpty();
        assertThat(snap.details()).isEmpty();
        assertThat(snap.lastChanged()).isNotNull();
        assertThat(state.isMaintenanceActive()).isFalse();
    }

    @Test
    @DisplayName("Should transition to active with consistent snapshot")
    void shouldTransitionToActive() {
        MaintenanceState state = new MaintenanceState();

        boolean changed = state.transition(true, "DB Migration", Map.of("db", "postgres"));

        assertThat(changed).isTrue();
        MaintenanceSnapshot snap = state.getSnapshot();
        assertThat(snap.active()).isTrue();
        assertThat(snap.reason()).isEqualTo("DB Migration");
        assertThat(snap.details()).containsEntry("db", "postgres");
        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.getReason()).isEqualTo("DB Migration");
    }

    @Test
    @DisplayName("Should be idempotent when already in target state")
    void shouldBeIdempotent() {
        MaintenanceState state = new MaintenanceState();
        state.transition(true, "First", Map.of("step", "1"));

        boolean secondChange = state.transition(true, "Second", Map.of("step", "2"));

        assertThat(secondChange).isFalse();
        assertThat(state.getReason()).isEqualTo("First");
        assertThat(state.getDetails()).containsEntry("step", "1");
    }

    @Test
    @DisplayName("Should transition from active to inactive")
    void shouldTransitionToInactive() {
        MaintenanceState state = new MaintenanceState();
        state.transition(true, "Enter", Map.of());

        boolean changed = state.transition(false, "Exit", Map.of("cleanup", "done"));

        assertThat(changed).isTrue();
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getReason()).isEqualTo("Exit");
    }

    @Test
    @DisplayName("Should handle null reason and details gracefully")
    void shouldHandleNulls() {
        MaintenanceState state = new MaintenanceState();

        state.transition(true, null, null);

        assertThat(state.getReason()).isEmpty();
        assertThat(state.getDetails()).isEmpty();
    }

    @Test
    @DisplayName("Should guarantee snapshot consistency across fields")
    void shouldGuaranteeSnapshotConsistency() {
        MaintenanceState state = new MaintenanceState();
        state.transition(true, "Consistent", Map.of("key", "value"));

        MaintenanceSnapshot snap = state.getSnapshot();

        assertThat(snap.active()).isTrue();
        assertThat(snap.reason()).isEqualTo("Consistent");
        assertThat(snap.details()).containsEntry("key", "value");
        // All fields from same atomic snapshot — no torn reads possible
    }

    @Test
    @DisplayName("Should be thread-safe: only one CAS succeeds among concurrent threads")
    void shouldBeThreadSafe() throws InterruptedException {
        MaintenanceState state = new MaintenanceState();
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threads);
        AtomicInteger successfulTransitions = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    if (state.transition(true, "Reason " + index, Collections.emptyMap())) {
                        successfulTransitions.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();

        assertThat(successfulTransitions.get()).isEqualTo(1);
        assertThat(state.isMaintenanceActive()).isTrue();
    }
}
