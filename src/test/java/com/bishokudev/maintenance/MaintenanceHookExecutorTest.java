package com.bishokudev.maintenance;

import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.model.HookExecutionReport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.Order;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MaintenanceHookExecutorTest {

    @Test
    @DisplayName("Should execute hooks in order and report all success")
    void shouldExecuteHooksAndReportSuccess() {
        MaintenanceHook hook1 = mock(MaintenanceHook.class);
        MaintenanceHook hook2 = mock(MaintenanceHook.class);
        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(hook1, hook2), Duration.ofSeconds(5));

        HookExecutionReport report = executor.execute(true);

        verify(hook1).onEnterMaintenance();
        verify(hook2).onEnterMaintenance();
        assertThat(report.total()).isEqualTo(2);
        assertThat(report.succeeded()).isEqualTo(2);
        assertThat(report.failed()).isZero();
        assertThat(report.timedOut()).isZero();
        assertThat(report.partialFailure()).isFalse();
        assertThat(report.executions()).hasSize(2);
        assertThat(report.executions().get(0).status()).isEqualTo("success");
    }

    @Test
    @DisplayName("Should report partial failure when a hook throws")
    void shouldReportPartialFailureOnException() {
        MaintenanceHook goodHook = mock(MaintenanceHook.class);
        MaintenanceHook badHook = mock(MaintenanceHook.class);
        doThrow(new RuntimeException("Hook exploded")).when(badHook).onEnterMaintenance();

        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(goodHook, badHook), Duration.ofSeconds(5));

        HookExecutionReport report = executor.execute(true);

        assertThat(report.succeeded()).isEqualTo(1);
        assertThat(report.failed()).isEqualTo(1);
        assertThat(report.partialFailure()).isTrue();
        assertThat(report.failures()).hasSize(1);
        assertThat(report.executions().get(1).status()).isEqualTo("failed");
    }

    @Test
    @DisplayName("Should report timeout when a hook exceeds the configured timeout")
    void shouldReportTimeoutOnSlowHook() {
        MaintenanceHook slowHook = new MaintenanceHook() {
            @Override
            public void onEnterMaintenance() {
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(slowHook), Duration.ofMillis(50));

        HookExecutionReport report = executor.execute(true);

        assertThat(report.timedOut()).isEqualTo(1);
        assertThat(report.succeeded()).isZero();
        assertThat(report.partialFailure()).isTrue();
        assertThat(report.executions().get(0).status()).isEqualTo("timed_out");
    }

    @Test
    @DisplayName("Should call onExitMaintenance when entering is false")
    void shouldCallExitHook() {
        MaintenanceHook hook = mock(MaintenanceHook.class);
        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(hook), Duration.ofSeconds(5));

        executor.execute(false);

        verify(hook, never()).onEnterMaintenance();
        verify(hook).onExitMaintenance();
    }

    @Test
    @DisplayName("Should handle empty hooks list gracefully")
    void shouldHandleEmptyHooks() {
        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(), Duration.ofSeconds(5));

        HookExecutionReport report = executor.execute(true);

        assertThat(report.total()).isZero();
        assertThat(report.succeeded()).isZero();
        assertThat(report.partialFailure()).isFalse();
    }

    @Test
    @DisplayName("Should expose hooks list via getHooks()")
    void shouldExposeHooksList() {
        MaintenanceHook hook = mock(MaintenanceHook.class);
        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(hook), Duration.ofSeconds(5));

        assertThat(executor.getHooks()).hasSize(1);
    }

    @Test
    @DisplayName("destroy() should shut down executor without exception")
    void shouldShutDownGracefully() {
        MaintenanceHookExecutor executor = new MaintenanceHookExecutor(
                List.of(), Duration.ofSeconds(5));

        // Should not throw
        executor.destroy();
    }
}
