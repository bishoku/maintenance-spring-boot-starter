package com.bishokudev.maintenance.config;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KafkaConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.MaintenanceMetrics;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.RabbitConsumerLifecycleManager;
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
 * Broker-specific consumer lifecycle managers are registered in separate inner
 * {@code @Configuration} classes guarded by {@code @ConditionalOnClass}, so that
 * missing broker dependencies never cause {@link ClassNotFoundException}s.
 */
@AutoConfiguration
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
    public QueueMaintenanceManager queueMaintenanceManager(
            ObjectProvider<ConsumerLifecycleManager> managersProvider) {
        List<ConsumerLifecycleManager> managers = managersProvider.orderedStream().toList();
        return new QueueMaintenanceManager(managers);
    }

    @Bean
    @ConditionalOnMissingBean
    public MaintenanceCoordinator maintenanceCoordinator(
            MaintenanceState state,
            KubernetesReadinessManager readinessManager,
            QueueMaintenanceManager queueManager,
            ApplicationEventPublisher eventPublisher,
            ObjectProvider<MaintenanceHook> hooksProvider,
            MaintenanceProperties properties) {
        List<MaintenanceHook> hooks = hooksProvider.orderedStream().toList();
        return new MaintenanceCoordinator(state, readinessManager, queueManager,
                eventPublisher, hooks, properties);
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
    static class KafkaConsumerConfiguration {

        @Bean
        @ConditionalOnMissingBean(KafkaConsumerLifecycleManager.class)
        @ConditionalOnBean(
                type = "org.springframework.kafka.config.KafkaListenerEndpointRegistry"
        )
        KafkaConsumerLifecycleManager kafkaConsumerLifecycleManager(
                org.springframework.kafka.config.KafkaListenerEndpointRegistry registry) {
            return new KafkaConsumerLifecycleManager(registry);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry")
    static class RabbitConsumerConfiguration {

        @Bean
        @ConditionalOnMissingBean(RabbitConsumerLifecycleManager.class)
        @ConditionalOnBean(
                type = "org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry"
        )
        RabbitConsumerLifecycleManager rabbitConsumerLifecycleManager(
                org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry registry) {
            return new RabbitConsumerLifecycleManager(registry);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Micrometer metrics (optional)
    // ──────────────────────────────────────────────────────────────────────

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
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
