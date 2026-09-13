package com.bishokudev.maintenance.example;

import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHook;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example Spring Boot microservice application used for E2E integration tests.
 */
@SpringBootApplication
@RestController
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    @GetMapping("/api/hello")
    public String hello() {
        return "Hello World";
    }

    // Diagnostic tracking for E2E test verification
    public static final AtomicBoolean hookEntered = new AtomicBoolean(false);
    public static final AtomicBoolean hookExited = new AtomicBoolean(false);
    public static final AtomicInteger eventCount = new AtomicInteger(0);

    @Bean
    public MaintenanceHook sampleBusinessHook() {
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
    public void onMaintenanceEvent(MaintenanceModeChangedEvent event) {
        eventCount.incrementAndGet();
    }
}
