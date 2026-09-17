package com.bishokudev.maintenance;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
import com.bishokudev.maintenance.config.MaintenanceAutoConfiguration;
import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KafkaConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.MaintenanceMetrics;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.RabbitConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.TrafficDrainHandler;
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
            assertThat(context).hasSingleBean(TrafficDrainHandler.class);
            assertThat(context).hasSingleBean(QueueMaintenanceManager.class);
            assertThat(context).hasSingleBean(MaintenanceHookExecutor.class);
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
    @DisplayName("Should bind MaintenanceProperties from configuration including feature toggles")
    void shouldBindProperties() {
        contextRunner.withPropertyValues(
                        "maintenance.drain-delay=10s",
                        "maintenance.hook-timeout=60s",
                        "maintenance.readiness.enabled=false",
                        "maintenance.queues.enabled=false",
                        "maintenance.queues.kafka.enabled=false",
                        "maintenance.queues.rabbit.enabled=false",
                        "maintenance.hooks.enabled=false",
                        "maintenance.events.enabled=false",
                        "maintenance.metrics.enabled=false")
                .run(context -> {
                    MaintenanceProperties props = context.getBean(MaintenanceProperties.class);
                    assertThat(props.getDrainDelay().getSeconds()).isEqualTo(10);
                    assertThat(props.getHookTimeout().getSeconds()).isEqualTo(60);
                    assertThat(props.getReadiness().isEnabled()).isFalse();
                    assertThat(props.getQueues().isEnabled()).isFalse();
                    assertThat(props.getQueues().getKafka().isEnabled()).isFalse();
                    assertThat(props.getQueues().getRabbit().isEnabled()).isFalse();
                    assertThat(props.getHooks().isEnabled()).isFalse();
                    assertThat(props.getEvents().isEnabled()).isFalse();
                    assertThat(props.getMetrics().isEnabled()).isFalse();
                });
    }

    @Test
    @DisplayName("Should exclude KafkaConsumerLifecycleManager when maintenance.queues.kafka.enabled is false")
    void shouldExcludeKafkaConsumerManagerWhenDisabled() {
        contextRunner.withPropertyValues("maintenance.queues.kafka.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(KafkaConsumerLifecycleManager.class);
                    assertThat(context).hasSingleBean(RabbitConsumerLifecycleManager.class);
                });
    }

    @Test
    @DisplayName("Should exclude RabbitConsumerLifecycleManager when maintenance.queues.rabbit.enabled is false")
    void shouldExcludeRabbitConsumerManagerWhenDisabled() {
        contextRunner.withPropertyValues("maintenance.queues.rabbit.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(RabbitConsumerLifecycleManager.class);
                    assertThat(context).hasSingleBean(KafkaConsumerLifecycleManager.class);
                });
    }

    @Test
    @DisplayName("Should not configure MaintenanceMetrics when MeterRegistry is absent from classpath")
    void shouldNotConfigureMetricsWhenMicrometerAbsent() {
        contextRunner.withClassLoader(new FilteredClassLoader("io.micrometer.core.instrument.MeterRegistry"))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(MaintenanceMetrics.class);
                });
    }

    @Test
    @DisplayName("Should not configure MaintenanceMetrics when maintenance.metrics.enabled is false")
    void shouldNotConfigureMetricsWhenDisabled() {
        contextRunner.withPropertyValues("maintenance.metrics.enabled=false")
                .withUserConfiguration(TestMeterRegistryConfiguration.class)
                .run(context -> {
                    assertThat(context).doesNotHaveBean(MaintenanceMetrics.class);
                });
    }

    @Test
    @DisplayName("Should configure MaintenanceMetrics when MeterRegistry is present and enabled")
    void shouldConfigureMetricsWhenMeterRegistryPresent() {
        contextRunner.withUserConfiguration(TestMeterRegistryConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(MaintenanceMetrics.class);
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

    @Configuration(proxyBeanMethods = false)
    static class TestMeterRegistryConfiguration {
        @Bean
        io.micrometer.core.instrument.MeterRegistry meterRegistry() {
            return new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        }
    }
}
