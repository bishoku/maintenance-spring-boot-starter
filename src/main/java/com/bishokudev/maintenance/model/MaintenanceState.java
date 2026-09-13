package com.bishokudev.maintenance.model;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe holder for the current maintenance mode state.
 * <p>
 * All state fields are stored in an immutable {@link MaintenanceSnapshot} and swapped
 * atomically, guaranteeing that readers always see a consistent view — even without
 * external synchronization.
 * <p>
 * Applications can inject this bean anywhere (e.g. within {@code @Scheduled} tasks)
 * to safely determine if maintenance mode is active.
 */
public class MaintenanceState {

    private final AtomicReference<MaintenanceSnapshot> snapshot;

    public MaintenanceState() {
        this.snapshot = new AtomicReference<>(MaintenanceSnapshot.inactive());
    }

    public MaintenanceState(MaintenanceSnapshot initial) {
        this.snapshot = new AtomicReference<>(initial != null ? initial : MaintenanceSnapshot.inactive());
    }

    /**
     * Returns the current immutable snapshot of the maintenance state.
     * Guaranteed to be internally consistent (all fields belong to the same transition).
     *
     * @return current snapshot, never null
     */
    public MaintenanceSnapshot getSnapshot() {
        return snapshot.get();
    }

    /**
     * Checks if maintenance mode is currently active.
     *
     * @return {@code true} if active; {@code false} otherwise
     */
    public boolean isMaintenanceActive() {
        return snapshot.get().active();
    }

    /**
     * Returns the timestamp when the state last transitioned.
     */
    public Instant getLastChanged() {
        return snapshot.get().lastChanged();
    }

    /**
     * Returns the reason associated with the last transition.
     */
    public String getReason() {
        return snapshot.get().reason();
    }

    /**
     * Returns an unmodifiable view of transition details.
     */
    public Map<String, Object> getDetails() {
        return snapshot.get().details();
    }

    /**
     * Atomically transitions the state if the current active flag differs from {@code targetActive}.
     *
     * @param targetActive target maintenance state
     * @param newReason    optional description/reason for the transition
     * @param newDetails   optional component or execution details
     * @return {@code true} if state actually changed; {@code false} if already in target state (idempotent)
     */
    public boolean transition(boolean targetActive, String newReason, Map<String, Object> newDetails) {
        while (true) {
            MaintenanceSnapshot current = snapshot.get();
            if (current.active() == targetActive) {
                return false;
            }
            MaintenanceSnapshot next = new MaintenanceSnapshot(
                    targetActive,
                    Instant.now(),
                    newReason,
                    newDetails
            );
            if (snapshot.compareAndSet(current, next)) {
                return true;
            }
            // CAS failed due to concurrent modification, retry
        }
    }

    @Override
    public String toString() {
        return "MaintenanceState{snapshot=" + snapshot.get() + "}";
    }
}
