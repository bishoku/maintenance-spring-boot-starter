package com.bishokudev.maintenance.manager;

import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.model.MaintenanceState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.context.event.EventListener;

import java.util.Objects;

/**
 * Publishes Micrometer metrics for maintenance mode transitions.
 * <p>
 * Metrics registered:
 * <ul>
 *   <li>{@code maintenance.mode.active} &mdash; Gauge (1 = active, 0 = inactive)</li>
 *   <li>{@code maintenance.mode.transitions} &mdash; Counter with {@code direction} tag (enter/exit)</li>
 * </ul>
 * <p>
 * Only activated when Micrometer is on the classpath.
 */
public class MaintenanceMetrics {

    private final Counter enterCounter;
    private final Counter exitCounter;

    public MaintenanceMetrics(MeterRegistry registry, MaintenanceState state) {
        Objects.requireNonNull(registry, "MeterRegistry must not be null");
        Objects.requireNonNull(state, "MaintenanceState must not be null");

        registry.gauge("maintenance.mode.active", Tags.empty(), state,
                s -> s.isMaintenanceActive() ? 1.0 : 0.0);

        this.enterCounter = Counter.builder("maintenance.mode.transitions")
                .tag("direction", "enter")
                .description("Number of times maintenance mode was entered")
                .register(registry);

        this.exitCounter = Counter.builder("maintenance.mode.transitions")
                .tag("direction", "exit")
                .description("Number of times maintenance mode was exited")
                .register(registry);
    }

    @EventListener
    public void onMaintenanceModeChanged(MaintenanceModeChangedEvent event) {
        if (event.isEntering()) {
            enterCounter.increment();
        } else {
            exitCounter.increment();
        }
    }
}
