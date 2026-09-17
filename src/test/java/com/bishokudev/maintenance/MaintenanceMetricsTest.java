package com.bishokudev.maintenance;

import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.manager.MaintenanceMetrics;
import com.bishokudev.maintenance.model.HookExecutionDetail;
import com.bishokudev.maintenance.model.HookExecutionReport;
import com.bishokudev.maintenance.model.MaintenanceState;
import com.bishokudev.maintenance.model.TransitionReport;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MaintenanceMetricsTest {

    private SimpleMeterRegistry registry;
    private MaintenanceState state;
    private MaintenanceMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        state = new MaintenanceState();
        metrics = new MaintenanceMetrics(registry, state);
    }

    @Test
    @DisplayName("Should initialize gauges with inactive values")
    void shouldInitializeGauges() {
        Gauge activeGauge = registry.find("maintenance.mode.active").gauge();
        assertThat(activeGauge).isNotNull();
        assertThat(activeGauge.value()).isEqualTo(0.0);

        Gauge durationGauge = registry.find("maintenance.mode.duration.current.seconds").gauge();
        assertThat(durationGauge).isNotNull();
        assertThat(durationGauge.value()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Should update metrics on entering maintenance mode")
    void shouldUpdateMetricsOnEnter() {
        HookExecutionReport hookReport = new HookExecutionReport(
                1, 1, 0, 0,
                List.of(new HookExecutionDetail("com.example.CacheDrainHook", "success", 45L)),
                List.of(), false
        );

        TransitionReport report = TransitionReport.builder()
                .readiness("REFUSING_TRAFFIC")
                .queues(Map.of("kafkaStopped", true))
                .hooks(hookReport)
                .transitionDurationMs(120L)
                .build();

        state.transition(true, "Upgrading DB", report.toMap());
        Instant enterTime = state.getLastChanged();

        metrics.onMaintenanceModeChanged(
                new MaintenanceModeChangedEvent(this, true, enterTime, "Upgrading DB", report)
        );

        Gauge activeGauge = registry.find("maintenance.mode.active").gauge();
        assertThat(activeGauge.value()).isEqualTo(1.0);

        Counter enterCounter = registry.find("maintenance.mode.transitions")
                .tag("direction", "enter").counter();
        assertThat(enterCounter.count()).isEqualTo(1.0);

        Timer transitionTimer = registry.find("maintenance.transition.duration")
                .tag("direction", "enter")
                .tag("status", "success")
                .timer();
        assertThat(transitionTimer).isNotNull();
        assertThat(transitionTimer.count()).isEqualTo(1);

        Timer hookTimer = registry.find("maintenance.hook.duration")
                .tag("hook", "CacheDrainHook")
                .tag("direction", "enter")
                .tag("status", "success")
                .timer();
        assertThat(hookTimer).isNotNull();
        assertThat(hookTimer.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should record maintenance window duration upon exiting maintenance")
    void shouldRecordWindowDurationOnExit() throws InterruptedException {
        // Enter
        TransitionReport enterReport = TransitionReport.builder()
                .readiness("REFUSING_TRAFFIC")
                .transitionDurationMs(50L)
                .build();
        state.transition(true, "Start window", enterReport.toMap());
        metrics.onMaintenanceModeChanged(
                new MaintenanceModeChangedEvent(this, true, state.getLastChanged(), "Start window", enterReport)
        );

        Thread.sleep(15); // ensure elapsed time

        // Exit
        TransitionReport exitReport = TransitionReport.builder()
                .readiness("ACCEPTING_TRAFFIC")
                .transitionDurationMs(40L)
                .build();
        state.transition(false, "Finish window", exitReport.toMap());
        metrics.onMaintenanceModeChanged(
                new MaintenanceModeChangedEvent(this, false, state.getLastChanged(), "Finish window", exitReport)
        );

        Gauge activeGauge = registry.find("maintenance.mode.active").gauge();
        assertThat(activeGauge.value()).isEqualTo(0.0);

        Counter exitCounter = registry.find("maintenance.mode.transitions")
                .tag("direction", "exit").counter();
        assertThat(exitCounter.count()).isEqualTo(1.0);

        Timer windowTimer = registry.find("maintenance.mode.window.duration").timer();
        assertThat(windowTimer).isNotNull();
        assertThat(windowTimer.count()).isEqualTo(1);
        assertThat(windowTimer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(10);
    }

    @Test
    @DisplayName("Should track partial failure status in transition timer if hook failed")
    void shouldTrackPartialFailureStatus() {
        HookExecutionReport hookReport = new HookExecutionReport(
                1, 0, 1, 0,
                List.of(new HookExecutionDetail("FailingHook", "failed", 100L)),
                List.of("Hook [FailingHook] failed: error"), true
        );

        TransitionReport report = TransitionReport.builder()
                .readiness("REFUSING_TRAFFIC")
                .hooks(hookReport)
                .transitionDurationMs(200L)
                .build();

        state.transition(true, "Test partial failure", report.toMap());
        metrics.onMaintenanceModeChanged(
                new MaintenanceModeChangedEvent(this, true, state.getLastChanged(), "Test partial failure", report)
        );

        Timer failureTimer = registry.find("maintenance.transition.duration")
                .tag("direction", "enter")
                .tag("status", "partial_failure")
                .timer();
        assertThat(failureTimer).isNotNull();
        assertThat(failureTimer.count()).isEqualTo(1);
    }
}
