package com.bishokudev.maintenance;

import com.bishokudev.maintenance.config.MaintenanceAutoConfiguration;
import com.bishokudev.maintenance.manager.KafkaConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.RabbitConsumerLifecycleManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StartupConsumerDeferralTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MaintenanceAutoConfiguration.class))
            .withUserConfiguration(Factories.class);

    @Configuration(proxyBeanMethods = false)
    static class Factories {
        @Bean org.springframework.kafka.config.KafkaListenerEndpointRegistry kafkaRegistry() {
            return new org.springframework.kafka.config.KafkaListenerEndpointRegistry();
        }
        @Bean org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry rabbitRegistry() {
            return new org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry();
        }
        @Bean ConcurrentKafkaListenerContainerFactory<String, String> kafkaFactory() {
            var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
            factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(Map.of("bootstrap.servers", "localhost:1")));
            factory.setContainerCustomizer(c -> c.getContainerProperties().setClientId("user-customizer"));
            return factory;
        }
        @Bean CachingConnectionFactory connectionFactory() { return new CachingConnectionFactory("localhost", 1); }
        @Bean SimpleRabbitListenerContainerFactory rabbitFactory(CachingConnectionFactory connectionFactory) {
            var factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            factory.setContainerCustomizer(c -> c.setPhase(17));
            return factory;
        }
    }

    static final class FinalKafkaFactory extends ConcurrentKafkaListenerContainerFactory<String, String> { }

    @Test void unproxyableFactoryFailsStartupRatherThanSilentlyAllowingConsumption() {
        runner.withPropertyValues("maintenance.start-in-maintenance=true")
                .withBean("finalKafkaFactory", FinalKafkaFactory.class, FinalKafkaFactory::new)
                .run(context -> assertThat(context.getStartupFailure())
                        .hasStackTraceContaining("Startup maintenance requires a non-final listener container factory"));
    }

    private MethodKafkaListenerEndpoint<String, String> kafkaEndpoint(Boolean autoStartup) {
        var endpoint = new MethodKafkaListenerEndpoint<String, String>();
        endpoint.setId("test"); endpoint.setTopics("test"); endpoint.setGroupId("test");
        endpoint.setBean(this);
        try { endpoint.setMethod(getClass().getDeclaredMethod("receive", String.class)); }
        catch (NoSuchMethodException ex) { throw new AssertionError(ex); }
        endpoint.setMessageHandlerMethodFactory(new org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory());
        endpoint.setAutoStartup(autoStartup);
        return endpoint;
    }
    void receive(String value) { }

    private SimpleRabbitListenerEndpoint rabbitEndpoint(Boolean autoStartup) {
        var endpoint = new SimpleRabbitListenerEndpoint();
        endpoint.setId("test"); endpoint.setQueueNames("test");
        endpoint.setMessageListener(message -> { }); endpoint.setAutoStartup(autoStartup);
        return endpoint;
    }

    @Test void realFactoriesDeferAfterApplyingEndpointSettingsAndPreserveCustomizers() {
        runner.withPropertyValues("maintenance.start-in-maintenance=true").run(context -> {
            assertThat(context).hasNotFailed();
            var kafka = context.getBean(ConcurrentKafkaListenerContainerFactory.class).createListenerContainer(kafkaEndpoint(true));
            var rabbit = context.getBean(SimpleRabbitListenerContainerFactory.class).createListenerContainer(rabbitEndpoint(true));
            assertThat(kafka.isAutoStartup()).isFalse(); assertThat(kafka.isRunning()).isFalse();
            assertThat(kafka.getContainerProperties().getClientId()).isEqualTo("user-customizer");
            assertThat(rabbit.isAutoStartup()).isFalse(); assertThat(rabbit.isRunning()).isFalse();
            assertThat(rabbit.getPhase()).isEqualTo(17);
            assertThat(context.getBean(KafkaConsumerLifecycleManager.class).stopConsumers()).containsEntry("startupDeferred", 1);
            assertThat(context.getBean(RabbitConsumerLifecycleManager.class).stopConsumers()).containsEntry("startupDeferred", 1);
        });
    }

    @Test void explicitlyDisabledEndpointsAreNotClaimed() {
        runner.withPropertyValues("maintenance.start-in-maintenance=true").run(context -> {
            var kafka = context.getBean(ConcurrentKafkaListenerContainerFactory.class).createListenerContainer(kafkaEndpoint(false));
            var rabbit = context.getBean(SimpleRabbitListenerContainerFactory.class).createListenerContainer(rabbitEndpoint(false));
            assertThat(kafka.isAutoStartup()).isFalse(); assertThat(rabbit.isAutoStartup()).isFalse();
            assertThat(context.getBean(KafkaConsumerLifecycleManager.class).stopConsumers()).containsEntry("startupDeferred", 0);
            assertThat(context.getBean(RabbitConsumerLifecycleManager.class).stopConsumers()).containsEntry("startupDeferred", 0);
        });
    }

    @Test void startupDisabledLeavesAutomaticStartupUnchanged() {
        runner.run(context -> {
            assertThat(context.getBean(ConcurrentKafkaListenerContainerFactory.class).createListenerContainer(kafkaEndpoint(true)).isAutoStartup()).isTrue();
            assertThat(context.getBean(SimpleRabbitListenerContainerFactory.class).createListenerContainer(rabbitEndpoint(true)).isAutoStartup()).isTrue();
        });
    }

    @Test void queueManagementDisabledLeavesAutomaticStartupUnchanged() {
        runner.withPropertyValues("maintenance.start-in-maintenance=true", "maintenance.queues.enabled=false").run(context -> {
            assertThat(context.getBean(ConcurrentKafkaListenerContainerFactory.class).createListenerContainer(kafkaEndpoint(true)).isAutoStartup()).isTrue();
            assertThat(context.getBean(SimpleRabbitListenerContainerFactory.class).createListenerContainer(rabbitEndpoint(true)).isAutoStartup()).isTrue();
        });
    }

    @Test void factoryLevelAutoStartupFalseIsPreserved() {
        runner.withPropertyValues("maintenance.start-in-maintenance=true").run(context -> {
            var kafkaFactory = context.getBean(ConcurrentKafkaListenerContainerFactory.class);
            var rabbitFactory = context.getBean(SimpleRabbitListenerContainerFactory.class);
            kafkaFactory.setAutoStartup(false); rabbitFactory.setAutoStartup(false);
            assertThat(kafkaFactory.createListenerContainer(kafkaEndpoint(null)).isAutoStartup()).isFalse();
            assertThat(rabbitFactory.createListenerContainer(rabbitEndpoint(null)).isAutoStartup()).isFalse();
            assertThat(context.getBean(KafkaConsumerLifecycleManager.class).stopConsumers()).containsEntry("startupDeferred", 0);
            assertThat(context.getBean(RabbitConsumerLifecycleManager.class).stopConsumers()).containsEntry("startupDeferred", 0);
        });
    }

    @Test void brokerSpecificOptOutLeavesFactoriesUntouched() {
        runner.withPropertyValues("maintenance.start-in-maintenance=true", "maintenance.queues.kafka.enabled=false",
                "maintenance.queues.rabbit.enabled=false").run(context -> {
            assertThat(context.getBean(ConcurrentKafkaListenerContainerFactory.class).createListenerContainer(kafkaEndpoint(true)).isAutoStartup()).isTrue();
            assertThat(context.getBean(SimpleRabbitListenerContainerFactory.class).createListenerContainer(rabbitEndpoint(true)).isAutoStartup()).isTrue();
        });
    }

    @Test @SuppressWarnings("unchecked") void failedDeferredKafkaStartRetainsOwnershipForRetry() {
        var container = mock(MessageListenerContainer.class);
        AtomicBoolean auto = new AtomicBoolean(true), running = new AtomicBoolean();
        when(container.isAutoStartup()).thenAnswer(i -> auto.get());
        doAnswer(i -> { auto.set(i.getArgument(0)); return null; }).when(container).setAutoStartup(anyBoolean());
        when(container.isRunning()).thenAnswer(i -> running.get());
        doThrow(new IllegalStateException("unavailable")).doAnswer(i -> { running.set(true); return null; }).when(container).start();
        var manager = new KafkaConsumerLifecycleManager(mock(ObjectProvider.class));
        manager.deferStartup(container);
        assertThatThrownBy(manager::startConsumers).hasMessageContaining("unavailable");
        manager.startConsumers(); manager.startConsumers();
        verify(container, times(2)).start(); assertThat(running).isTrue();
    }

    @Test @SuppressWarnings("unchecked") void failedDeferredRabbitStartRetainsOwnershipForRetry() {
        var container = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        AtomicBoolean auto = new AtomicBoolean(true), running = new AtomicBoolean();
        when(container.isAutoStartup()).thenAnswer(i -> auto.get());
        doAnswer(i -> { auto.set(i.getArgument(0)); return null; }).when(container).setAutoStartup(anyBoolean());
        when(container.isRunning()).thenAnswer(i -> running.get());
        doThrow(new IllegalStateException("unavailable")).doAnswer(i -> { running.set(true); return null; }).when(container).start();
        var manager = new RabbitConsumerLifecycleManager(mock(ObjectProvider.class));
        manager.deferStartup(container);
        assertThatThrownBy(manager::startConsumers).hasMessageContaining("unavailable");
        manager.startConsumers(); manager.startConsumers();
        verify(container, times(2)).start(); assertThat(running).isTrue();
    }

    @Test @SuppressWarnings("unchecked") void kafkaStartsDeferredButNeverUserDisabledContainers() {
        var active = mock(MessageListenerContainer.class); var disabled = mock(MessageListenerContainer.class);
        AtomicBoolean auto = new AtomicBoolean(true), running = new AtomicBoolean();
        when(active.isAutoStartup()).thenAnswer(i -> auto.get());
        doAnswer(i -> { auto.set(i.getArgument(0)); return null; }).when(active).setAutoStartup(anyBoolean());
        when(active.isRunning()).thenAnswer(i -> running.get());
        doAnswer(i -> { running.set(true); return null; }).when(active).start();
        var manager = new KafkaConsumerLifecycleManager(mock(ObjectProvider.class));
        manager.deferStartup(active); manager.deferStartup(disabled);
        assertThat(auto).isFalse(); verify(active, never()).start();
        manager.startConsumers(); manager.startConsumers();
        assertThat(auto).isTrue(); assertThat(running).isTrue();
        verify(active, times(1)).start(); verify(active, never()).resume(); verify(disabled, never()).start();
    }

    @Test @SuppressWarnings("unchecked") void rabbitStartsDeferredButNeverUserDisabledContainers() {
        var active = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        var disabled = mock(org.springframework.amqp.rabbit.listener.MessageListenerContainer.class);
        AtomicBoolean auto = new AtomicBoolean(true), running = new AtomicBoolean();
        when(active.isAutoStartup()).thenAnswer(i -> auto.get());
        doAnswer(i -> { auto.set(i.getArgument(0)); return null; }).when(active).setAutoStartup(anyBoolean());
        when(active.isRunning()).thenAnswer(i -> running.get());
        doAnswer(i -> { running.set(true); return null; }).when(active).start();
        var manager = new RabbitConsumerLifecycleManager(mock(ObjectProvider.class));
        manager.deferStartup(active); manager.deferStartup(disabled);
        assertThat(auto).isFalse(); verify(active, never()).start();
        manager.startConsumers(); manager.startConsumers();
        assertThat(auto).isTrue(); assertThat(running).isTrue();
        verify(active, times(1)).start(); verify(disabled, never()).start();
    }
}
