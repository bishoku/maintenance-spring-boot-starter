package com.bishokudev.maintenance.config;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Auto-configuration for the maintenance mode starter.
 * <p>
 * Registered via {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}.
 * <p>
 * Configured to run after Kafka and RabbitMQ auto-configurations so that messaging infrastructure
 * is fully resolved. Broker managers use {@link ObjectProvider} to avoid bean-ordering issues.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration"
})
@ConditionalOnClass(Endpoint.class)
@ConditionalOnProperty(
        prefix = "management.endpoint.maintenance",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@EnableConfigurationProperties(MaintenanceProperties.class)
public class MaintenanceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public MaintenanceState maintenanceState() {
        return new MaintenanceState();
    }

    @Bean
    @ConditionalOnMissingBean
    public KubernetesReadinessManager kubernetesReadinessManager(ApplicationEventPublisher eventPublisher) {
        return new KubernetesReadinessManager(eventPublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public TrafficDrainHandler trafficDrainHandler(KubernetesReadinessManager readinessManager) {
        return new TrafficDrainHandler(readinessManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public QueueMaintenanceManager queueMaintenanceManager(
            ObjectProvider<ConsumerLifecycleManager> managersProvider) {
        List<ConsumerLifecycleManager> managers = managersProvider.orderedStream().toList();
        return new QueueMaintenanceManager(managers);
    }

    @Bean
    @ConditionalOnMissingBean
    public MaintenanceHookExecutor maintenanceHookExecutor(
            ObjectProvider<MaintenanceHook> hooksProvider,
            MaintenanceProperties properties) {
        List<MaintenanceHook> hooks = hooksProvider.orderedStream().toList();
        return new MaintenanceHookExecutor(hooks, properties.getHookTimeout());
    }

    @Bean
    @ConditionalOnMissingBean
    public MaintenanceCoordinator maintenanceCoordinator(
            MaintenanceState state,
            TrafficDrainHandler drainHandler,
            QueueMaintenanceManager queueManager,
            MaintenanceHookExecutor hookExecutor,
            ApplicationEventPublisher eventPublisher,
            MaintenanceProperties properties) {
        return new MaintenanceCoordinator(state, drainHandler, queueManager,
                hookExecutor, eventPublisher, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MaintenanceActuatorEndpoint maintenanceActuatorEndpoint(MaintenanceCoordinator coordinator) {
        return new MaintenanceActuatorEndpoint(coordinator);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Broker-specific inner configurations (classpath-guarded)
    // ──────────────────────────────────────────────────────────────────────

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.kafka.config.KafkaListenerEndpointRegistry")
    @ConditionalOnProperty(
            prefix = "maintenance.queues.kafka",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    static class KafkaConsumerConfiguration {

        @Bean
        @ConditionalOnMissingBean(KafkaConsumerLifecycleManager.class)
        KafkaConsumerLifecycleManager kafkaConsumerLifecycleManager(
                ObjectProvider<org.springframework.kafka.config.KafkaListenerEndpointRegistry> registryProvider) {
            return new KafkaConsumerLifecycleManager(registryProvider);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry")
    @ConditionalOnProperty(
            prefix = "maintenance.queues.rabbit",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    static class RabbitConsumerConfiguration {

        @Bean
        @ConditionalOnMissingBean(RabbitConsumerLifecycleManager.class)
        RabbitConsumerLifecycleManager rabbitConsumerLifecycleManager(
                ObjectProvider<org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry> registryProvider) {
            return new RabbitConsumerLifecycleManager(registryProvider);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Micrometer metrics (optional)
    // ──────────────────────────────────────────────────────────────────────

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
    @ConditionalOnProperty(
            prefix = "maintenance.metrics",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    static class MetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnBean(type = "io.micrometer.core.instrument.MeterRegistry")
        MaintenanceMetrics maintenanceMetrics(
                io.micrometer.core.instrument.MeterRegistry registry,
                MaintenanceState state) {
            return new MaintenanceMetrics(registry, state);
        }
    }
}
