package com.bishokudev.maintenance;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
import com.bishokudev.maintenance.config.MaintenanceAutoConfiguration;
import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class MaintenanceAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MaintenanceAutoConfiguration.class));

    @Test
    @DisplayName("Should auto-configure all core beans with default settings")
    void shouldAutoConfigureAllBeans() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(MaintenanceState.class);
            assertThat(context).hasSingleBean(KubernetesReadinessManager.class);
            assertThat(context).hasSingleBean(QueueMaintenanceManager.class);
            assertThat(context).hasSingleBean(MaintenanceCoordinator.class);
            assertThat(context).hasSingleBean(MaintenanceActuatorEndpoint.class);
            assertThat(context).hasSingleBean(MaintenanceProperties.class);
        });
    }

    @Test
    @DisplayName("Should not configure when property is false")
    void shouldBackOffWhenPropertyDisabled() {
        contextRunner.withPropertyValues("management.endpoint.maintenance.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(MaintenanceState.class);
                    assertThat(context).doesNotHaveBean(MaintenanceActuatorEndpoint.class);
                });
    }

    @Test
    @DisplayName("Should back off when Endpoint class is missing")
    void shouldBackOffWhenEndpointClassMissing() {
        contextRunner.withClassLoader(
                        new FilteredClassLoader(org.springframework.boot.actuate.endpoint.annotation.Endpoint.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(MaintenanceState.class);
                    assertThat(context).doesNotHaveBean(MaintenanceActuatorEndpoint.class);
                });
    }

    @Test
    @DisplayName("Should collect custom MaintenanceHook beans")
    void shouldCollectCustomHooks() {
        contextRunner.withUserConfiguration(TestHookConfiguration.class)
                .run(context -> {
                    MaintenanceCoordinator coordinator = context.getBean(MaintenanceCoordinator.class);
                    assertThat(coordinator.getHooks()).hasSize(1);
                });
    }

    @Test
    @DisplayName("QueueMaintenanceManager should work with empty consumer list when brokers absent")
    void shouldWorkWithNoConsumerManagers() {
        contextRunner.withClassLoader(new FilteredClassLoader(
                        "org.springframework.kafka.config.KafkaListenerEndpointRegistry",
                        "org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry"))
                .run(context -> {
                    QueueMaintenanceManager qmm = context.getBean(QueueMaintenanceManager.class);
                    assertThat(qmm.getManagers()).isEmpty();
                    assertThat(qmm.stopConsumers()).containsKey("message");
                });
    }

    @Test
    @DisplayName("Should bind MaintenanceProperties from configuration")
    void shouldBindProperties() {
        contextRunner.withPropertyValues(
                        "maintenance.drain-delay=10s",
                        "maintenance.hook-timeout=60s")
                .run(context -> {
                    MaintenanceProperties props = context.getBean(MaintenanceProperties.class);
                    assertThat(props.getDrainDelay().getSeconds()).isEqualTo(10);
                    assertThat(props.getHookTimeout().getSeconds()).isEqualTo(60);
                });
    }

    @Test
    @DisplayName("Should allow overriding MaintenanceState bean")
    void shouldAllowOverridingMaintenanceState() {
        contextRunner.withUserConfiguration(CustomStateConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(MaintenanceState.class);
                    assertThat(context.getBean(MaintenanceState.class).isMaintenanceActive()).isTrue();
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class TestHookConfiguration {
        @Bean
        MaintenanceHook customHook() {
            return new MaintenanceHook() {};
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomStateConfiguration {
        @Bean
        MaintenanceState maintenanceState() {
            MaintenanceState state = new MaintenanceState();
            state.transition(true, "pre-configured", java.util.Map.of());
            return state;
        }
    }
}
