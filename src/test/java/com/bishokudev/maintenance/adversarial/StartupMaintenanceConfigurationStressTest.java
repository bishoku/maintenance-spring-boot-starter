package com.bishokudev.maintenance.adversarial;

import com.bishokudev.maintenance.config.MaintenanceAutoConfiguration;
import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.env.MockEnvironment;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adversarial test suite stress-testing configuration binding, environment variable vs property
 * precedence, relaxed uppercase variations, whitespace, null safety, and concurrent property access.
 */
public class StartupMaintenanceConfigurationStressTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MaintenanceAutoConfiguration.class));

    @Test
    @DisplayName("Permutation: Both start-in-maintenance and startup.enabled enabled concurrently")
    void shouldHandleBothStartupPropertiesSetToTrue() {
        runner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.startup.enabled=true",
                "maintenance.startup.reason=Redundant Flags Active"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.getPhase()).isEqualTo("ACTIVE");
            assertThat(state.isWorkBlocked()).isTrue();
            assertThat(state.getReason()).isEqualTo("Redundant Flags Active");
        });
    }

    @Test
    @DisplayName("Permutation: Alias enabled while primary start-in-maintenance is explicitly false")
    void shouldEnableWhenAliasIsTrueEvenIfPrimaryIsFalse() {
        runner.withPropertyValues(
                "maintenance.start-in-maintenance=false",
                "maintenance.startup.enabled=true"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.isWorkBlocked()).isTrue();
        });
    }

    @Test
    @DisplayName("Permutation: Primary enabled while alias is explicitly false")
    void shouldEnableWhenPrimaryIsTrueEvenIfAliasIsFalse() {
        runner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.startup.enabled=false"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.isWorkBlocked()).isTrue();
        });
    }

    @Test
    @DisplayName("Precedence: MAINTENANCE_STARTUP_REASON environment variable overrides default property")
    void envVarStartupReasonShouldOverrideDefault() {
        runner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "MAINTENANCE_STARTUP_REASON=Env-Defined Emergency Maintenance Window"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.getReason()).isEqualTo("Env-Defined Emergency Maintenance Window");
        });
    }

    @Test
    @DisplayName("Edge Case: Empty or whitespace MAINTENANCE_STARTUP_REASON falls back to configured property")
    void emptyEnvVarStartupReasonShouldFallBackToProperty() {
        runner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.startup.reason=Property Defined Reason",
                "MAINTENANCE_STARTUP_REASON=   "
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.getReason()).isEqualTo("Property Defined Reason");
        });
    }

    @Test
    @DisplayName("Edge Case: Null, empty or whitespace reason programmatically set falls back to default")
    void programmaticNullOrWhitespaceReasonShouldDefault() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.getStartup().setReason(null);
        assertThat(props.getStartupReason()).isEqualTo("Startup maintenance mode");

        props.getStartup().setReason("");
        assertThat(props.getStartupReason()).isEqualTo("Startup maintenance mode");

        props.getStartup().setReason("   \t\n  ");
        assertThat(props.getStartupReason()).isEqualTo("Startup maintenance mode");

        props.getStartup().setReason("Valid Reason");
        assertThat(props.getStartupReason()).isEqualTo("Valid Reason");
    }

    @Test
    @DisplayName("Null safety: setStartup(null) does not throw NPE on subsequent getter calls")
    void setStartupNullShouldBeSafelyHandled() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartup(null);
        assertThat(props.getStartup()).isNotNull();
        assertThat(props.isStartInMaintenance()).isFalse();
        assertThat(props.getStartupReason()).isEqualTo("Startup maintenance mode");
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "TRUE", "True", "  true  "})
    @DisplayName("Relaxed binding: Case-insensitive and trimmed boolean values for MAINTENANCE_START_IN_MAINTENANCE")
    void relaxedBooleanValuesForStartInMaintenanceEnvVar(String value) {
        runner.withPropertyValues("MAINTENANCE_START_IN_MAINTENANCE=" + value)
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isTrue();
                    assertThat(state.getPhase()).isEqualTo("ACTIVE");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "FALSE", "False", "no", "0", "off"})
    @DisplayName("Negative binding: Boolean-equivalent false environment variable values keep maintenance disabled")
    void negativeValuesForEnvVarShouldKeepInactive(String value) {
        runner.withPropertyValues("MAINTENANCE_START_IN_MAINTENANCE=" + value)
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isFalse();
                    assertThat(state.getPhase()).isEqualTo("INACTIVE");
                    assertThat(state.isWorkBlocked()).isFalse();
                });
    }

    @Test
    @DisplayName("Empty string env var: MAINTENANCE_START_IN_MAINTENANCE empty string does not enable maintenance")
    void emptyStringEnvVarShouldNotCrashStartup() {
        runner.withPropertyValues("MAINTENANCE_START_IN_MAINTENANCE=")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isFalse();
                });
    }

    @Test
    @DisplayName("Concurrency: Concurrent reads of isStartInMaintenance under multithreaded access")
    void concurrentReadAccessToProperties() throws InterruptedException {
        MaintenanceProperties props = new MaintenanceProperties();
        MockEnvironment env = new MockEnvironment();
        env.setProperty("MAINTENANCE_START_IN_MAINTENANCE", "true");
        props.setEnvironment(env);

        int threads = 20;
        int iterations = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);
        AtomicInteger activeCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < iterations; j++) {
                        if (props.isStartInMaintenance()) {
                            activeCount.incrementAndGet();
                        }
                    }
                } catch (Exception ex) {
                    // unexpected error
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        endGate.await();
        executor.shutdown();

        assertThat(activeCount.get()).isEqualTo(threads * iterations);
    }
}
