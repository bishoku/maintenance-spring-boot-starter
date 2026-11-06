package com.bishokudev.maintenance.e2e;

import com.bishokudev.maintenance.config.MaintenanceAutoConfiguration;
import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test suite for startup maintenance configuration variants, property aliases,
 * relaxed environment variable bindings, and feature toggle boundaries (Tiers 1 & 2).
 */
public class StartupMaintenancePropertyVariantsTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MaintenanceAutoConfiguration.class));

    // ──────────────────────────────────────────────────────────────────────
    // Tier 1: Feature Coverage (Configuration & Defaults)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Tier 1: Inactive startup by default when no startup properties are configured")
    void tier1_inactiveStartupByDefault() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(MaintenanceState.class);
            MaintenanceState state = context.getBean(MaintenanceState.class);

            assertThat(state.isMaintenanceActive()).isFalse();
            assertThat(state.getPhase()).isEqualTo("INACTIVE");
            assertThat(state.isWorkBlocked()).isFalse();
            assertThat(state.getReason()).isEmpty();
        });
    }

    @Test
    @DisplayName("Tier 1: Explicitly disabled startup with maintenance.start-in-maintenance=false")
    void tier1_explicitlyDisabledStartup() {
        contextRunner.withPropertyValues("maintenance.start-in-maintenance=false")
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isFalse();
                    assertThat(state.getPhase()).isEqualTo("INACTIVE");
                    assertThat(state.isWorkBlocked()).isFalse();
                });
    }

    @Test
    @DisplayName("Tier 1: Startup maintenance enabled via maintenance.start-in-maintenance=true")
    void tier1_startupInMaintenanceViaStartInMaintenanceProperty() {
        contextRunner.withPropertyValues("maintenance.start-in-maintenance=true")
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isTrue();
                    assertThat(state.getPhase()).isEqualTo("ACTIVE");
                    assertThat(state.isWorkBlocked()).isTrue();
                    assertThat(state.getReason()).isEqualTo("Startup maintenance mode");
                });
    }

    @Test
    @DisplayName("Tier 1: Startup maintenance enabled via property alias maintenance.startup.enabled=true")
    void tier1_startupInMaintenanceViaStartupEnabledPropertyAlias() {
        contextRunner.withPropertyValues("maintenance.startup.enabled=true")
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isTrue();
                    assertThat(state.getPhase()).isEqualTo("ACTIVE");
                    assertThat(state.isWorkBlocked()).isTrue();
                    assertThat(state.getReason()).isEqualTo("Startup maintenance mode");
                });
    }

    @Test
    @DisplayName("Tier 1: Relaxed environment variable binding for MAINTENANCE_START_IN_MAINTENANCE=true")
    void tier1_relaxedEnvVarBinding_startInMaintenance() {
        contextRunner.withPropertyValues("MAINTENANCE_START_IN_MAINTENANCE=true")
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isTrue();
                    assertThat(state.getPhase()).isEqualTo("ACTIVE");
                    assertThat(state.isWorkBlocked()).isTrue();
                });
    }

    @Test
    @DisplayName("Tier 1: Relaxed environment variable binding for MAINTENANCE_STARTUP_ENABLED=true")
    void tier1_relaxedEnvVarBinding_startupEnabled() {
        contextRunner.withPropertyValues("MAINTENANCE_STARTUP_ENABLED=true")
                .run(context -> {
                    MaintenanceState state = context.getBean(MaintenanceState.class);
                    assertThat(state.isMaintenanceActive()).isTrue();
                    assertThat(state.getPhase()).isEqualTo("ACTIVE");
                    assertThat(state.isWorkBlocked()).isTrue();
                });
    }

    @Test
    @DisplayName("Tier 1: Startup maintenance with custom reason configured via property")
    void tier1_startupMaintenanceWithCustomReason() {
        contextRunner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.startup.reason=Scheduled Infrastructure Migration v3"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.getReason()).isEqualTo("Scheduled Infrastructure Migration v3");
        });
    }

    @Test
    @DisplayName("Tier 1: Startup maintenance with custom reason via env var MAINTENANCE_STARTUP_REASON")
    void tier1_startupMaintenanceWithCustomReasonEnvVar() {
        contextRunner.withPropertyValues(
                "MAINTENANCE_START_IN_MAINTENANCE=true",
                "MAINTENANCE_STARTUP_REASON=Env Var Cold Boot Maintenance"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.getReason()).isEqualTo("Env Var Cold Boot Maintenance");
        });
    }

    // ──────────────────────────────────────────────────────────────────────
    // Tier 2: Boundary & Corner Cases (Feature Overrides & Toggles)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Tier 2: Startup maintenance with master queues toggle disabled (maintenance.queues.enabled=false)")
    void tier2_startupMaintenanceWithQueuesDisabled() {
        contextRunner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.queues.enabled=false"
        ).run(context -> {
            MaintenanceState state = context.getBean(MaintenanceState.class);
            assertThat(state.isMaintenanceActive()).isTrue();

            MaintenanceProperties properties = context.getBean(MaintenanceProperties.class);
            assertThat(properties.getQueues().isEnabled()).isFalse();
        });
    }

    @Test
    @DisplayName("Tier 2: Startup maintenance with Kafka disabled (maintenance.queues.kafka.enabled=false)")
    void tier2_startupMaintenanceWithKafkaDisabled() {
        contextRunner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.queues.kafka.enabled=false"
        ).run(context -> {
            MaintenanceProperties properties = context.getBean(MaintenanceProperties.class);
            assertThat(properties.getQueues().getKafka().isEnabled()).isFalse();
            assertThat(properties.getQueues().getRabbit().isEnabled()).isTrue();
        });
    }

    @Test
    @DisplayName("Tier 2: Startup maintenance with RabbitMQ disabled (maintenance.queues.rabbit.enabled=false)")
    void tier2_startupMaintenanceWithRabbitDisabled() {
        contextRunner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.queues.rabbit.enabled=false"
        ).run(context -> {
            MaintenanceProperties properties = context.getBean(MaintenanceProperties.class);
            assertThat(properties.getQueues().getRabbit().isEnabled()).isFalse();
            assertThat(properties.getQueues().getKafka().isEnabled()).isTrue();
        });
    }

    @Test
    @DisplayName("Tier 2: Startup maintenance with hooks disabled (maintenance.hooks.enabled=false)")
    void tier2_startupMaintenanceWithHooksDisabled() {
        contextRunner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.hooks.enabled=false"
        ).run(context -> {
            MaintenanceProperties properties = context.getBean(MaintenanceProperties.class);
            assertThat(properties.getHooks().isEnabled()).isFalse();
        });
    }

    @Test
    @DisplayName("Tier 2: Startup maintenance with events disabled (maintenance.events.enabled=false)")
    void tier2_startupMaintenanceWithEventsDisabled() {
        contextRunner.withPropertyValues(
                "maintenance.start-in-maintenance=true",
                "maintenance.events.enabled=false"
        ).run(context -> {
            MaintenanceProperties properties = context.getBean(MaintenanceProperties.class);
            assertThat(properties.getEvents().isEnabled()).isFalse();
        });
    }
}
