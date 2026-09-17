package com.bishokudev.maintenance.manager;

import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.model.HookExecutionDetail;
import com.bishokudev.maintenance.model.HookExecutionReport;
import com.bishokudev.maintenance.model.MaintenanceState;
import com.bishokudev.maintenance.model.TransitionReport;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.event.EventListener;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Publishes rich Micrometer metrics for maintenance mode observability.
 * <p>
 * Zero runtime dependencies: Activated only when {@link MeterRegistry} is present.
 * <p>
 * Registered Metrics:
 * <ul>
 *   <li>{@code maintenance.mode.active} &mdash; Gauge (1.0 = active, 0.0 = inactive)</li>
 *   <li>{@code maintenance.mode.duration.current.seconds} &mdash; Gauge of ongoing downtime (seconds)</li>
 *   <li>{@code maintenance.mode.transitions} &mdash; Counter tagged with direction (enter/exit)</li>
 *   <li>{@code maintenance.mode.window.duration} &mdash; Timer measuring total downtime window per session</li>
 *   <li>{@code maintenance.transition.duration} &mdash; Timer measuring transition execution latency</li>
 *   <li>{@code maintenance.hook.duration} &mdash; Timer measuring individual MaintenanceHook latency</li>
 * </ul>
 */
public class MaintenanceMetrics {

    private final MeterRegistry registry;
    private final Counter enterCounter;
    private final Counter exitCounter;
    private final Timer windowDurationTimer;
    private volatile Instant lastEnterTimestamp;

    public MaintenanceMetrics(MeterRegistry registry, MaintenanceState state) {
        this.registry = Objects.requireNonNull(registry, "MeterRegistry must not be null");
        Objects.requireNonNull(state, "MaintenanceState must not be null");

        // 1. Binary state gauge (1.0 = active, 0.0 = inactive)
        registry.gauge("maintenance.mode.active", Tags.empty(), state,
                s -> s.isMaintenanceActive() ? 1.0 : 0.0);

        // 2. Active duration gauge (how many seconds pod has been in maintenance)
        registry.gauge("maintenance.mode.duration.current.seconds", Tags.empty(), state, s -> {
            if (!s.isMaintenanceActive() || s.getLastChanged() == null) {
                return 0.0;
            }
            return (double) Duration.between(s.getLastChanged(), Instant.now()).toSeconds();
        });

        // 3. Transition Counters
        this.enterCounter = Counter.builder("maintenance.mode.transitions")
                .tag("direction", "enter")
                .description("Number of times maintenance mode was entered")
                .register(registry);

        this.exitCounter = Counter.builder("maintenance.mode.transitions")
                .tag("direction", "exit")
                .description("Number of times maintenance mode was exited")
                .register(registry);

        // 4. Maintenance window timer (total time in maintenance upon exit)
        this.windowDurationTimer = Timer.builder("maintenance.mode.window.duration")
                .description("Total duration spent in maintenance mode per window")
                .register(registry);
    }

    @EventListener
    public void onMaintenanceModeChanged(MaintenanceModeChangedEvent event) {
        String direction = event.isEntering() ? "enter" : "exit";

        if (event.isEntering()) {
            enterCounter.increment();
            this.lastEnterTimestamp = event.getTransitionTimestamp();
        } else {
            exitCounter.increment();
            if (lastEnterTimestamp != null) {
                long windowMs = Duration.between(lastEnterTimestamp, event.getTransitionTimestamp()).toMillis();
                windowDurationTimer.record(windowMs, TimeUnit.MILLISECONDS);
                lastEnterTimestamp = null;
            }
        }

        TransitionReport report = event.getReport();
        if (report != null) {
            recordTransitionDuration(report, direction);
            recordHookDurations(report, direction);
        }
    }

    private void recordTransitionDuration(TransitionReport report, String direction) {
        String status = report.partialFailure() ? "partial_failure" : "success";
        Timer.builder("maintenance.transition.duration")
                .tag("direction", direction)
                .tag("status", status)
                .description("Duration of maintenance mode transition workflow")
                .register(registry)
                .record(report.transitionDurationMs(), TimeUnit.MILLISECONDS);
    }

    private void recordHookDurations(TransitionReport report, String direction) {
        HookExecutionReport hookReport = report.hooks();
        if (hookReport == null) {
            return;
        }
        for (HookExecutionDetail detail : hookReport.executions()) {
            Timer.builder("maintenance.hook.duration")
                    .tag("hook", extractSimpleName(detail.name()))
                    .tag("direction", direction)
                    .tag("status", detail.status())
                    .description("Duration of MaintenanceHook execution")
                    .register(registry)
                    .record(detail.durationMs(), TimeUnit.MILLISECONDS);
        }
    }

    private String extractSimpleName(String className) {
        if (className == null) return "UnknownHook";
        int lastDot = className.lastIndexOf('.');
        return lastDot >= 0 && lastDot < className.length() - 1 ? className.substring(lastDot + 1) : className;
    }
}
