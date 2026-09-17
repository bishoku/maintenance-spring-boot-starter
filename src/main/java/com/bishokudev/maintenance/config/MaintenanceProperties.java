package com.bishokudev.maintenance.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

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
public class MaintenanceProperties {

    /**
     * Delay between setting readiness to REFUSING_TRAFFIC and stopping queue consumers.
     * Allows Kubernetes time to remove the pod from service endpoints before in-flight
     * requests are affected.
     */
    private Duration drainDelay = Duration.ofSeconds(5);

    /**
     * Maximum time to wait for each {@code MaintenanceHook} to complete before timing out.
     * A hook that exceeds this duration will be interrupted and logged as failed.
     */
    private Duration hookTimeout = Duration.ofSeconds(30);

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

    public Duration getDrainDelay() {
        return drainDelay;
    }

    public void setDrainDelay(Duration drainDelay) {
        this.drainDelay = drainDelay;
    }

    public Duration getHookTimeout() {
        return hookTimeout;
    }

    public void setHookTimeout(Duration hookTimeout) {
        this.hookTimeout = hookTimeout;
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
