package com.bishokudev.maintenance.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates all available {@link ConsumerLifecycleManager} implementations and
 * orchestrates stopping/starting of message queue consumers.
 * <p>
 * If no broker is on the classpath, the manager list will be empty and
 * operations complete gracefully with no side effects.
 */
public class QueueMaintenanceManager {

    private static final Logger log = LoggerFactory.getLogger(QueueMaintenanceManager.class);

    private final List<ConsumerLifecycleManager> managers;

    public QueueMaintenanceManager(List<ConsumerLifecycleManager> managers) {
        this.managers = managers != null ? List.copyOf(managers) : Collections.emptyList();
        if (this.managers.isEmpty()) {
            log.debug("No ConsumerLifecycleManager beans found. Queue lifecycle management is inactive.");
        } else {
            log.info("Registered {} consumer lifecycle manager(s): {}",
                    this.managers.size(),
                    this.managers.stream().map(ConsumerLifecycleManager::name).toList());
        }
    }

    /**
     * Stops consumers for all registered brokers.
     *
     * @return aggregated report across all brokers
     */
    public Map<String, Object> stopConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        for (ConsumerLifecycleManager manager : managers) {
            try {
                Map<String, Object> brokerReport = manager.stopConsumers();
                report.put(manager.name(), brokerReport);
            } catch (Exception ex) {
                log.error("Failed to stop consumers for broker [{}]", manager.name(), ex);
                report.put(manager.name(), Map.of("error", ex.getMessage()));
            }
        }
        if (managers.isEmpty()) {
            report.put("message", "No message brokers configured");
        }
        return report;
    }

    /**
     * Starts consumers for all registered brokers.
     *
     * @return aggregated report across all brokers
     */
    public Map<String, Object> startConsumers() {
        Map<String, Object> report = new LinkedHashMap<>();
        for (ConsumerLifecycleManager manager : managers) {
            try {
                Map<String, Object> brokerReport = manager.startConsumers();
                report.put(manager.name(), brokerReport);
            } catch (Exception ex) {
                log.error("Failed to start consumers for broker [{}]", manager.name(), ex);
                report.put(manager.name(), Map.of("error", ex.getMessage()));
            }
        }
        if (managers.isEmpty()) {
            report.put("message", "No message brokers configured");
        }
        return report;
    }

    /**
     * @return unmodifiable list of registered lifecycle managers
     */
    public List<ConsumerLifecycleManager> getManagers() {
        return managers;
    }
}
