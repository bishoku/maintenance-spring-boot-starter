package com.bishokudev.maintenance.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MaintenancePropertiesTest {

    @Test
    @DisplayName("Should have correct default values")
    void shouldHaveCorrectDefaultValues() {
        MaintenanceProperties properties = new MaintenanceProperties();

        assertThat(properties.isStartInMaintenance()).isFalse();
        assertThat(properties.getStartup().isEnabled()).isFalse();
        assertThat(properties.getStartup().getReason()).isEqualTo("Startup maintenance mode");
        assertThat(properties.getStartupReason()).isEqualTo("Startup maintenance mode");
        assertThat(properties.getDrainDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(properties.getQueueTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getHookTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getReadiness().isEnabled()).isTrue();
        assertThat(properties.getQueues().isEnabled()).isTrue();
        assertThat(properties.getHooks().isEnabled()).isTrue();
        assertThat(properties.getEvents().isEnabled()).isTrue();
        assertThat(properties.getMetrics().isEnabled()).isTrue();
    }

    @Test
    @DisplayName("Setting startInMaintenance programmatically should update startup.enabled")
    void shouldSyncStartInMaintenanceToStartup() {
        MaintenanceProperties properties = new MaintenanceProperties();

        properties.setStartInMaintenance(true);
        assertThat(properties.isStartInMaintenance()).isTrue();
        assertThat(properties.getStartup().isEnabled()).isTrue();

        properties.setStartInMaintenance(false);
        properties.getStartup().setEnabled(true);
        assertThat(properties.isStartInMaintenance()).isTrue();
        assertThat(properties.getStartup().isEnabled()).isTrue();
    }

    @Test
    @DisplayName("Setting custom startup reason programmatically")
    void shouldSetStartupReason() {
        MaintenanceProperties properties = new MaintenanceProperties();

        properties.getStartup().setReason("Custom cluster maintenance");
        assertThat(properties.getStartupReason()).isEqualTo("Custom cluster maintenance");
        assertThat(properties.getStartup().getReason()).isEqualTo("Custom cluster maintenance");

        // Null should fall back to default
        properties.getStartup().setReason(null);
        assertThat(properties.getStartupReason()).isEqualTo("Startup maintenance mode");
    }

    @Test
    @DisplayName("Should bind maintenance.start-in-maintenance via Binder")
    void shouldBindStartInMaintenanceViaBinder() {
        MapConfigurationPropertySource source = new MapConfigurationPropertySource();
        source.put("maintenance.start-in-maintenance", "true");

        Binder binder = new Binder(source);
        MaintenanceProperties properties = binder.bind("maintenance", MaintenanceProperties.class).get();

        assertThat(properties.isStartInMaintenance()).isTrue();
        assertThat(properties.getStartup().isEnabled()).isTrue();
    }

    @Test
    @DisplayName("Should bind maintenance.startup.enabled and reason via Binder")
    void shouldBindStartupEnabledAndReasonViaBinder() {
        MapConfigurationPropertySource source = new MapConfigurationPropertySource();
        source.put("maintenance.startup.enabled", "true");
        source.put("maintenance.startup.reason", "Scheduled DB Upgrade");

        Binder binder = new Binder(source);
        MaintenanceProperties properties = binder.bind("maintenance", MaintenanceProperties.class).get();

        assertThat(properties.isStartInMaintenance()).isTrue();
        assertThat(properties.getStartup().isEnabled()).isTrue();
        assertThat(properties.getStartup().getReason()).isEqualTo("Scheduled DB Upgrade");
        assertThat(properties.getStartupReason()).isEqualTo("Scheduled DB Upgrade");
    }

    @Test
    @DisplayName("Should bind relaxed environment variables MAINTENANCE_START_IN_MAINTENANCE and MAINTENANCE_STARTUP_REASON")
    void shouldBindRelaxedEnvironmentVariables() {
        StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().addFirst(new SystemEnvironmentPropertySource("systemEnvironment",
                Map.of(
                        "MAINTENANCE_START_IN_MAINTENANCE", "true",
                        "MAINTENANCE_STARTUP_REASON", "Env Var Startup Maintenance"
                )));

        Binder binder = Binder.get(env);
        MaintenanceProperties properties = binder.bind("maintenance", MaintenanceProperties.class).get();

        assertThat(properties.isStartInMaintenance()).isTrue();
        assertThat(properties.getStartup().getReason()).isEqualTo("Env Var Startup Maintenance");
    }

    @Test
    @DisplayName("Should bind relaxed environment variable MAINTENANCE_STARTUP_ENABLED")
    void shouldBindRelaxedEnvVarStartupEnabled() {
        StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().addFirst(new SystemEnvironmentPropertySource("systemEnvironment",
                Map.of(
                        "MAINTENANCE_STARTUP_ENABLED", "true"
                )));

        Binder binder = Binder.get(env);
        MaintenanceProperties properties = binder.bind("maintenance", MaintenanceProperties.class).get();

        assertThat(properties.isStartInMaintenance()).isTrue();
    }
}
