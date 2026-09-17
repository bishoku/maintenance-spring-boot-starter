package com.bishokudev.maintenance;

import com.bishokudev.maintenance.model.HookExecutionDetail;
import com.bishokudev.maintenance.model.HookExecutionReport;
import com.bishokudev.maintenance.model.TransitionReport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TransitionReportTest {

    @Test
    @DisplayName("toMap() should produce backward-compatible map structure for enter transition")
    void toMapShouldProduceBackwardCompatibleEnterMap() {
        HookExecutionReport hookReport = new HookExecutionReport(
                1, 1, 0, 0,
                List.of(new HookExecutionDetail("com.example.CacheDrainHook", "success", 45L)),
                List.of(), false
        );

        TransitionReport report = TransitionReport.builder()
                .readiness("REFUSING_TRAFFIC")
                .drainDelayMs(5000L)
                .queues(Map.of("kafka", Map.of("kafkaStopped", true)))
                .hooks(hookReport)
                .transitionDurationMs(120L)
                .build();

        Map<String, Object> map = report.toMap();

        assertThat(map).containsEntry("readiness", "REFUSING_TRAFFIC");
        assertThat(map).containsEntry("drainDelayMs", 5000L);
        assertThat(map).containsKey("queues");
        assertThat(map).containsKey("hooks");
        assertThat(map).containsEntry("transitionDurationMs", 120L);
        assertThat(map).doesNotContainKey("readinessError");
        assertThat(map).doesNotContainKey("queueError");
    }

    @Test
    @DisplayName("toMap() should include error keys when errors are present")
    void toMapShouldIncludeErrorKeys() {
        TransitionReport report = TransitionReport.builder()
                .readinessError("K8s unreachable")
                .queueError("Broker down")
                .transitionDurationMs(50L)
                .build();

        Map<String, Object> map = report.toMap();

        assertThat(map).containsEntry("readinessError", "K8s unreachable");
        assertThat(map).containsEntry("queueError", "Broker down");
    }

    @Test
    @DisplayName("toMap() for disabled readiness should contain DISABLED value")
    void toMapShouldContainDisabledReadiness() {
        TransitionReport report = TransitionReport.builder()
                .readiness("DISABLED")
                .queues(Map.of("enabled", false))
                .transitionDurationMs(10L)
                .build();

        Map<String, Object> map = report.toMap();

        assertThat(map).containsEntry("readiness", "DISABLED");
        assertThat(map.get("queues")).isEqualTo(Map.of("enabled", false));
    }

    @Test
    @DisplayName("partialFailure() should detect hook failures")
    void partialFailureShouldDetectHookFailures() {
        HookExecutionReport hookReport = new HookExecutionReport(
                1, 0, 1, 0,
                List.of(new HookExecutionDetail("FailingHook", "failed", 100L)),
                List.of("Hook failed"), true
        );

        TransitionReport report = TransitionReport.builder()
                .hooks(hookReport)
                .transitionDurationMs(200L)
                .build();

        assertThat(report.partialFailure()).isTrue();
    }

    @Test
    @DisplayName("partialFailure() should detect readiness errors")
    void partialFailureShouldDetectReadinessErrors() {
        TransitionReport report = TransitionReport.builder()
                .readinessError("K8s unreachable")
                .transitionDurationMs(50L)
                .build();

        assertThat(report.partialFailure()).isTrue();
    }

    @Test
    @DisplayName("partialFailure() should be false when no errors")
    void partialFailureShouldBeFalseWhenNoErrors() {
        TransitionReport report = TransitionReport.builder()
                .readiness("REFUSING_TRAFFIC")
                .transitionDurationMs(50L)
                .build();

        assertThat(report.partialFailure()).isFalse();
    }

    @Test
    @DisplayName("HookExecutionReport.toMap() should include partialFailure and failures when present")
    void hookReportToMapShouldIncludePartialFailure() {
        HookExecutionReport hookReport = new HookExecutionReport(
                2, 1, 1, 0,
                List.of(
                        new HookExecutionDetail("GoodHook", "success", 10L),
                        new HookExecutionDetail("BadHook", "failed", 20L)
                ),
                List.of("Hook [BadHook] failed: error"), true
        );

        Map<String, Object> map = hookReport.toMap();

        assertThat(map).containsEntry("total", 2);
        assertThat(map).containsEntry("succeeded", 1);
        assertThat(map).containsEntry("failed", 1);
        assertThat(map).containsEntry("partialFailure", true);
        assertThat(map).containsKey("failures");
        assertThat(map).containsKey("executions");
    }

    @Test
    @DisplayName("HookExecutionReport.toMap() should not include partialFailure key when all succeeded")
    void hookReportToMapShouldNotIncludePartialFailureWhenSuccess() {
        HookExecutionReport hookReport = new HookExecutionReport(
                1, 1, 0, 0,
                List.of(new HookExecutionDetail("GoodHook", "success", 10L)),
                List.of(), false
        );

        Map<String, Object> map = hookReport.toMap();

        assertThat(map).doesNotContainKey("partialFailure");
        assertThat(map).doesNotContainKey("failures");
    }
}
