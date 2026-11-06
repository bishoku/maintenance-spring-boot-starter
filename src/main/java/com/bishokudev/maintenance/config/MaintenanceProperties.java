package com.bishokudev.maintenance.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import java.time.Duration;

/**
 * Configuration properties for the maintenance mode starter.
 *
 * <pre>
 * maintenance:
 *   drain-delay: 5s
 *   hook-timeout: 30s
 *   readiness:
 *     enabled: true
 *   queues:
 *     enabled: true
 *     kafka:
 *       enabled: true
 *     rabbit:
 *       enabled: true
 *   hooks:
 *     enabled: true
 *   events:
 *     enabled: true
 * </pre>
 */
@ConfigurationProperties(prefix = "maintenance")
public class MaintenanceProperties implements EnvironmentAware {

    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /** Maximum wait for broker pause/stop acknowledgement. */
    private Duration queueTimeout = Duration.ofSeconds(30);

    public Duration getQueueTimeout() { return queueTimeout; }
    public void setQueueTimeout(Duration value) { queueTimeout = positive(value, "queue-timeout"); }
    private static Duration positive(Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero() || value.toMillis() < 1)
            throw new IllegalArgumentException(name + " must be at least 1ms");
        return value;
    }

    /** Propagation grace after readiness refusal; not an HTTP request drain guarantee. */
    private Duration drainDelay = Duration.ofSeconds(5);

    /**
     * Maximum time to wait for each {@code MaintenanceHook} to complete before timing out.
     * A hook that exceeds this duration receives an interrupt request and fails the transition.
     */
    private Duration hookTimeout = Duration.ofSeconds(30);

    /**
     * Whether the application should start directly in maintenance mode upon boot.
     * Alias for {@code maintenance.startup.enabled}.
     */
    private boolean startInMaintenance = false;

    /**
     * Configuration for starting directly in maintenance mode upon boot.
     */
    private StartupProperties startup = new StartupProperties();

    /**
     * Configuration for Kubernetes readiness probe manipulation.
     */
    private ReadinessProperties readiness = new ReadinessProperties();

    /**
     * Configuration for message queue consumer lifecycle management.
     */
    private QueuesProperties queues = new QueuesProperties();

    /**
     * Configuration for custom {@code MaintenanceHook} executions.
     */
    private HooksProperties hooks = new HooksProperties();

    /**
     * Configuration for {@code MaintenanceModeChangedEvent} publishing.
     */
    private EventsProperties events = new EventsProperties();

    /**
     * Configuration for Micrometer metrics publishing (when Micrometer is present).
     */
    private MetricsProperties metrics = new MetricsProperties();

    public MetricsProperties getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsProperties metrics) {
        this.metrics = metrics != null ? metrics : new MetricsProperties();
    }

    public boolean isStartInMaintenance() {
        if (startInMaintenance || (startup != null && startup.isEnabled())) {
            return true;
        }
        if (environment != null) {
            Boolean envStart = environment.getProperty("MAINTENANCE_START_IN_MAINTENANCE", Boolean.class);
            if (Boolean.TRUE.equals(envStart)) {
                return true;
            }
            Boolean envStartup = environment.getProperty("MAINTENANCE_STARTUP_ENABLED", Boolean.class);
            if (Boolean.TRUE.equals(envStartup)) {
                return true;
            }
        }
        return false;
    }

    public void setStartInMaintenance(boolean startInMaintenance) {
        this.startInMaintenance = startInMaintenance;
        if (this.startup != null) {
            this.startup.setEnabled(startInMaintenance);
        }
    }

    public StartupProperties getStartup() {
        return startup;
    }

    public void setStartup(StartupProperties startup) {
        this.startup = startup != null ? startup : new StartupProperties();
        if (this.startInMaintenance && this.startup != null) {
            this.startup.setEnabled(true);
        }
    }

    public String getStartupReason() {
        if (environment != null) {
            String envReason = environment.getProperty("MAINTENANCE_STARTUP_REASON");
            if (envReason != null && !envReason.trim().isEmpty()) {
                return envReason;
            }
        }
        return startup != null ? startup.getReason() : "Startup maintenance mode";
    }

    public Duration getDrainDelay() {
        return drainDelay;
    }

    public void setDrainDelay(Duration drainDelay) {
        if (drainDelay == null || drainDelay.isNegative()) throw new IllegalArgumentException("drain-delay must be non-negative");
        this.drainDelay = drainDelay;
    }

    public Duration getHookTimeout() {
        return hookTimeout;
    }

    public void setHookTimeout(Duration hookTimeout) {
        this.hookTimeout = positive(hookTimeout, "hook-timeout");
    }

    public ReadinessProperties getReadiness() {
        return readiness;
    }

    public void setReadiness(ReadinessProperties readiness) {
        this.readiness = readiness != null ? readiness : new ReadinessProperties();
    }

    public QueuesProperties getQueues() {
        return queues;
    }

    public void setQueues(QueuesProperties queues) {
        this.queues = queues != null ? queues : new QueuesProperties();
    }

    public HooksProperties getHooks() {
        return hooks;
    }

    public void setHooks(HooksProperties hooks) {
        this.hooks = hooks != null ? hooks : new HooksProperties();
    }

    public EventsProperties getEvents() {
        return events;
    }

    public void setEvents(EventsProperties events) {
        this.events = events != null ? events : new EventsProperties();
    }

    public static class StartupProperties {
        /**
         * Whether the application should start directly in maintenance mode.
         */
        private boolean enabled = false;

        /**
         * Reason description when starting directly in maintenance mode.
         */
        private String reason = "Startup maintenance mode";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = (reason != null && !reason.trim().isEmpty()) ? reason : "Startup maintenance mode";
        }
    }

    public static class ReadinessProperties {
        /**
         * Whether to automatically toggle Kubernetes readiness probes
         * (REFUSING_TRAFFIC on enter, ACCEPTING_TRAFFIC on exit).
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class QueuesProperties {
        /**
         * Master toggle for queue consumer pausing/resuming.
         * If false, all queue lifecycle management is skipped.
         */
        private boolean enabled = true;

        /**
         * Specific toggle for Kafka consumers.
         */
        private KafkaQueueProperties kafka = new KafkaQueueProperties();

        /**
         * Specific toggle for RabbitMQ consumers.
         */
        private RabbitQueueProperties rabbit = new RabbitQueueProperties();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public KafkaQueueProperties getKafka() {
            return kafka;
        }

        public void setKafka(KafkaQueueProperties kafka) {
            this.kafka = kafka != null ? kafka : new KafkaQueueProperties();
        }

        public RabbitQueueProperties getRabbit() {
            return rabbit;
        }

        public void setRabbit(RabbitQueueProperties rabbit) {
            this.rabbit = rabbit != null ? rabbit : new RabbitQueueProperties();
        }
    }

    public static class KafkaQueueProperties {
        /**
         * Whether to pause/resume Kafka listener containers during maintenance transitions.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class RabbitQueueProperties {
        /**
         * Whether to pause/resume RabbitMQ listener containers during maintenance transitions.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class HooksProperties {
        /**
         * Whether to execute registered {@code MaintenanceHook} beans during maintenance transitions.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class EventsProperties {
        /**
         * Whether to publish {@code MaintenanceModeChangedEvent} on the Spring ApplicationEventPublisher.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class MetricsProperties {
        /**
         * Whether to publish Micrometer / Prometheus metrics (only if Micrometer is on the classpath).
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
