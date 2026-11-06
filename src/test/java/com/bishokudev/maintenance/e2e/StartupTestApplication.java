package com.bishokudev.maintenance.e2e;

import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Spring Boot application configured for E2E testing of startup in maintenance mode,
 * probe degradation, dynamic exit via Actuator, queue pausing/resumption, and scheduled tasks.
 */
@SpringBootApplication(scanBasePackages = "com.bishokudev.maintenance")
@EnableScheduling
@RestController
public class StartupTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(StartupTestApplication.class, args);
    }

    @GetMapping("/api/test/hello")
    public String hello() {
        return "Hello Startup Maintenance";
    }

    // ──────────────────────────────────────────────────────────────────────
    // Diagnostic State Trackers for E2E Test Assertions
    // ──────────────────────────────────────────────────────────────────────

    public static final AtomicBoolean hookEntered = new AtomicBoolean(false);
    public static final AtomicBoolean hookExited = new AtomicBoolean(false);
    public static final AtomicInteger eventCount = new AtomicInteger(0);
    public static final AtomicReference<MaintenanceModeChangedEvent> lastEvent = new AtomicReference<>();

    public static final AtomicBoolean kafkaContainerPaused = new AtomicBoolean(false);
    public static final AtomicBoolean rabbitContainerRunning = new AtomicBoolean(true);

    public static void resetDiagnostics() {
        hookEntered.set(false);
        hookExited.set(false);
        eventCount.set(0);
        lastEvent.set(null);
    }

    @Bean
    public MaintenanceHook testMaintenanceHook() {
        return new MaintenanceHook() {
            @Override
            public void onEnterMaintenance() {
                hookEntered.set(true);
            }

            @Override
            public void onExitMaintenance() {
                hookExited.set(true);
            }
        };
    }

    @EventListener
    public void onMaintenanceModeChanged(MaintenanceModeChangedEvent event) {
        eventCount.incrementAndGet();
        lastEvent.set(event);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Simulated Kafka & RabbitMQ Listener Registries for Queue E2E Testing
    // ──────────────────────────────────────────────────────────────────────

    @Bean
    @Primary
    public KafkaListenerEndpointRegistry testKafkaListenerEndpointRegistry() {
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        org.springframework.kafka.listener.MessageListenerContainer container =
                mock(org.springframework.kafka.listener.MessageListenerContainer.class);

        when(container.isRunning()).thenReturn(true);
        when(container.isPauseRequested()).thenAnswer(inv -> kafkaContainerPaused.get());
        when(container.isContainerPaused()).thenAnswer(inv -> kafkaContainerPaused.get());

        doAnswer(inv -> {
            kafkaContainerPaused.set(true);
            return null;
        }).when(container).pause();

        doAnswer(inv -> {
            kafkaContainerPaused.set(false);
            return null;
        }).when(container).resume();

        when(registry.getListenerContainers()).thenReturn(List.of(container));
        return registry;
    }

    @Bean
    @Primary
    public RabbitListenerEndpointRegistry testRabbitListenerEndpointRegistry() {
        RabbitListenerEndpointRegistry registry = mock(RabbitListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        when(container.isRunning()).thenAnswer(inv -> rabbitContainerRunning.get());

        doAnswer(inv -> {
            rabbitContainerRunning.set(false);
            Runnable callback = inv.getArgument(0);
            if (callback != null) {
                callback.run();
            }
            return null;
        }).when(container).stop(any(Runnable.class));

        doAnswer(inv -> {
            rabbitContainerRunning.set(true);
            return null;
        }).when(container).start();

        when(registry.getListenerContainers()).thenReturn(List.of(container));
        return registry;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Scheduled Task Tracker verifying Requirement R2 (Scheduled Preservation)
    // ──────────────────────────────────────────────────────────────────────

    @Component
    public static class ScheduledTaskTracker {
        private final MaintenanceState maintenanceState;
        private final AtomicInteger executionCount = new AtomicInteger(0);
        private final AtomicInteger blockedCount = new AtomicInteger(0);

        public ScheduledTaskTracker(MaintenanceState maintenanceState) {
            this.maintenanceState = maintenanceState;
        }

        @Scheduled(fixedRate = 25)
        public void performPeriodicTask() {
            if (maintenanceState.isWorkBlocked()) {
                blockedCount.incrementAndGet();
                return;
            }
            executionCount.incrementAndGet();
        }

        public int getExecutionCount() {
            return executionCount.get();
        }

        public int getBlockedCount() {
            return blockedCount.get();
        }

        public void reset() {
            executionCount.set(0);
            blockedCount.set(0);
        }
    }
}
