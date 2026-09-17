# maintenance-spring-boot-starter

[![Maven Central](https://img.shields.io/maven-central/v/com.bishokudev/maintenance-spring-boot-starter.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.bishokudev/maintenance-spring-boot-starter)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-17%20%7C%2021%20%7C%2025-orange.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x%20%7C%204.x-brightgreen.svg)](https://spring.io/projects/spring-boot)

A lightweight, production-ready Spring Boot starter library designed for **Kubernetes-native Maintenance Mode management**. 

It enables microservices to gracefully pause ingress traffic, suspend asynchronous message consumers (Kafka, RabbitMQ), throttle scheduled tasks, and invoke custom business hooks at runtime via Spring Boot Actuator—**without terminating or restarting pods**.

---

## 📌 The Problem

In modern Kubernetes-based microservice architectures, critical maintenance scenarios frequently arise:
- **Breaking Database Migrations:** Schema changes that require zero write traffic to prevent data corruption.
- **Data Backfilling & Consistency Fixes:** Batch processing where asynchronous event consumption must be temporarily paused.
- **Downstream Dependency Outages:** Critical third-party APIs going down, requiring the service to pause consumer queues to prevent retry/dead-letter storms.
- **Controlled Blue/Green or Canary Validations:** Needing to isolate specific pods for diagnostics without killing their state.

### Why Existing Approaches Fall Short

| Approach | Pitfalls & Risks |
| :--- | :--- |
| **`kubectl scale --replicas=0`** | Completely destroys the application context. Pod logs, in-memory caches, and JVM diagnostic states are lost. Slow recovery time when scaling back up. |
| **Killing or Restarting Pods** | Immediately severs in-flight HTTP requests (causing 502/504 errors). Triggers aggressive consumer group rebalances in Apache Kafka or RabbitMQ. |
| **Ad-Hoc Boolean Flags** | Often implemented without thread safety, lacking synchronization between ingress load balancers and asynchronous queue listeners, leading to race conditions. |

---

## 💡 The Solution

`maintenance-spring-boot-starter` introduces a unified, thread-safe, and Kubernetes-aware orchestration layer. With a single HTTP call to Spring Boot Actuator (or a programmatic trigger), the application transitions into an isolated maintenance state through a strictly ordered, idempotent workflow:

```
[ Enter Maintenance Workflow ]
┌───────────────────────────────┐
│ 1. K8s Readiness ➔ REFUSING   │ ➔ Pod removed from Kubernetes Service endpoints immediately
└──────────────┬────────────────┘
               ▼
┌───────────────────────────────┐
│ 2. Configurable Drain Delay   │ ➔ In-flight HTTP requests complete; K8s iptables/IPVS rules propagate
└──────────────┬────────────────┘
               ▼
┌───────────────────────────────┐
│ 3. Pause Queue Listeners      │ ➔ Kafka & RabbitMQ message consumers paused safely (no rebalance)
└──────────────┬────────────────┘
               ▼
┌───────────────────────────────┐
│ 4. Execute MaintenanceHooks   │ ➔ Developer-defined hooks executed in priority order (timeout-guarded)
└──────────────┬────────────────┘
               ▼
┌───────────────────────────────┐
│ 5. Atomic State & Event       │ ➔ State snapshot updated via CAS; MaintenanceModeChangedEvent published
└───────────────────────────────┘
```

When maintenance is complete, the reverse sequence seamlessly brings the pod back into service: queues resume first, hooks execute, and Kubernetes readiness probes return to `ACCEPTING_TRAFFIC`.

---

## ☸️ Deep Dive: Kubernetes Pod Lifecycle & Traffic Flow

Understanding how this starter interacts with Kubernetes probe mechanisms is key to seamless operations:

### 1. Readiness Probe vs. Liveness Probe Isolation
- **Readiness Probe (`/actuator/health/readiness`):** Represents whether the pod is ready to accept user traffic. When maintenance mode is activated, the starter publishes an internal Spring `AvailabilityChangeEvent` with `ReadinessState.REFUSING_TRAFFIC`. The readiness health check begins returning HTTP `503 Service Unavailable`.
- **Liveness Probe (`/actuator/health/liveness`):** Checks whether the container is healthy and alive. **The starter never alters liveness state.** The pod remains healthy and alive (`HTTP 200`).
- **Zero Restart Guarantee:** Because the liveness probe remains green, Kubernetes **never restarts or marks the pod as `CrashLoopBackOff`**. The pod remains in `Running` status indefinitely throughout the maintenance window.

### 2. Service Endpoints & Ingress Traffic Flow
1. When readiness flips to `REFUSING_TRAFFIC`, the Kubernetes `EndpointSlice` / `Endpoints` controller detects the probe failure.
2. The controller immediately removes the pod's IP address from the Kubernetes `Service` backends.
3. Ingress controllers (NGINX, Traefik, ALB, Istio, etc.) stop routing new external HTTP/gRPC traffic to this pod.

### 3. The Role of `drain-delay`
Network policy and iptables/IPVS route table updates across worker nodes do not happen instantaneously—propagation typically takes 1 to 3 seconds.
- Without a drain delay, message listeners or backend workers might stop while the Ingress controller is still forwarding the last batch of client requests.
- The starter provides a configurable `maintenance.drain-delay` (default: `5s`). After signaling `REFUSING_TRAFFIC`, the coordinator waits for this duration before shutting down consumers or executing hooks, guaranteeing that **all in-flight client requests finish gracefully**.

---

## 🎯 Spring Boot & Java Compatibility

The starter is built to align with modern Spring Boot standards and long-term support (LTS) runtimes:

- **Spring Boot Compatibility:** 
  - Fully compatible with **Spring Boot 3.0.x, 3.1.x, 3.2.x, 3.3.x, 3.4.x, 3.5.x**, and forward-compatible with **Spring Boot 4.x**.
  - Uses modern auto-configuration registration (`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`). No legacy `spring.factories`.
- **Java Runtime Compatibility:** 
  - Baseline: **Java 17 (LTS)**
  - Fully tested on **Java 21 (LTS)** and **Java 25**.
- **Broker Independence:** 
  - Kafka and RabbitMQ dependencies are marked as `optional`. If your application does not use Kafka or RabbitMQ, the starter runs with zero overhead and no missing-class warnings.

---

## 📦 Dependency Setup

### Maven (`pom.xml`)

```xml
<dependency>
    <groupId>com.bishokudev</groupId>
    <artifactId>maintenance-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle (`build.gradle`)

```groovy
implementation 'com.bishokudev:maintenance-spring-boot-starter:1.0.0'
```

---

## ⚙️ Configuration Reference

Add the following to your `application.yml`:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,maintenance
  endpoint:
    maintenance:
      # Required: The endpoint is disabled by default for security
      enabled: true

maintenance:
  # Delay between REFUSING_TRAFFIC and queue shutdown (allows K8s endpoint propagation)
  drain-delay: 5s
  # Maximum time allotted per MaintenanceHook before timeout cancellation
  hook-timeout: 30s

  # ─── Granular Operation Toggles (All Default to true) ─────────────────────
  readiness:
    # Set to false to disable Kubernetes readiness probe manipulation (and skip drain-delay)
    enabled: true

  queues:
    # Master switch for all message queue consumer pausing/resuming
    enabled: true
    kafka:
      # Set to false to leave Kafka consumers running during maintenance
      enabled: true
    rabbit:
      # Set to false to leave RabbitMQ consumers running during maintenance
      enabled: true

  hooks:
    # Set to false to skip executing registered MaintenanceHook beans
    enabled: true

  events:
    # Set to false to skip publishing MaintenanceModeChangedEvent
    enabled: true

  metrics:
    # Set to false to disable Micrometer metrics registration (auto-detected when Micrometer is present)
    enabled: true
```

### 💡 Common Tailored Scenarios

#### Scenario 1: Only Pause Kafka (Keep Ingress Traffic & Other Queues Active)
```yaml
maintenance:
  readiness:
    enabled: false  # Do not take pod out of K8s Service
  queues:
    enabled: true
    kafka:
      enabled: true
    rabbit:
      enabled: false # Keep RabbitMQ running
```

#### Scenario 2: Traffic-Only Maintenance (No Queue Consumers Paused)
```yaml
maintenance:
  readiness:
    enabled: true   # Cut ingress traffic
  queues:
    enabled: false  # Allow background queue processing to finish
```

#### Scenario 3: Hook-Only Notification Mode (Custom Business Logic Only)
```yaml
maintenance:
  readiness:
    enabled: false
  queues:
    enabled: false
  hooks:
    enabled: true   # Only run custom MaintenanceHook callbacks
```

---

## 🔒 Security Best Practices

> **⚠️ WARNING:** Because the maintenance endpoint can take a pod out of service endpoints, it should **never** be publicly accessible.

### 1. Internal Management Port (Recommended)
Configure Spring Boot Actuator to run on a dedicated internal port:

```yaml
management:
  server:
    port: 8081 # Expose only to internal network / cluster
```

### 2. Spring Security Restriction
Restrict access to authenticated operations or specific roles:

```java
@Configuration
public class ActuatorSecurityConfig {

    @Bean
    public SecurityFilterChain actuatorSecurity(HttpSecurity http) throws Exception {
        http.securityMatcher(EndpointRequest.toAnyEndpoint())
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(EndpointRequest.to("health", "info")).permitAll()
                .requestMatchers(EndpointRequest.to("maintenance")).hasRole("OPS_ADMIN")
                .anyRequest().authenticated()
            )
            .httpBasic(Customizer.withDefaults());
        return http.build();
    }
}
```

---

## 🚀 Usage & Integration Points

### 1. Actuator HTTP Operations

#### Check Current Status
```bash
curl -X GET http://localhost:8081/actuator/maintenance
```
**Response (`200 OK`):**
```json
{
  "active": false,
  "lastChanged": "2026-09-13T12:00:00Z",
  "reason": "",
  "details": {}
}
```

#### Enter Maintenance Mode (Idempotent)
```bash
curl -X POST http://localhost:8081/actuator/maintenance \
  -H "Content-Type: application/json" \
  -d '{"enabled": true, "reason": "Database migration v2.4 in progress"}'
```
**Response (`200 OK`):**
```json
{
  "active": true,
  "lastChanged": "2026-09-13T12:05:00Z",
  "reason": "Database migration v2.4 in progress",
  "details": {
    "readiness": "REFUSING_TRAFFIC",
    "drainDelayMs": 5000,
    "queues": {
      "kafka": { "kafkaStopped": true, "containersAffected": 4 },
      "rabbit": { "rabbitStopped": true, "containersAffected": 2 }
    },
    "hooks": {
      "total": 1,
      "succeeded": 1,
      "failed": 0,
      "timedOut": 0
    }
  }
}
```

#### Exit Maintenance Mode (Idempotent)
```bash
curl -X POST http://localhost:8081/actuator/maintenance \
  -H "Content-Type: application/json" \
  -d '{"enabled": false, "reason": "Migration completed successfully"}'
```

---

### 2. Scheduled Job Protection (`MaintenanceState`)
Inject `MaintenanceState` directly into scheduled tasks or batch workers:

```java
@Component
public class OrderSyncScheduler {

    private final MaintenanceState maintenanceState;

    public OrderSyncScheduler(MaintenanceState maintenanceState) {
        this.maintenanceState = maintenanceState;
    }

    @Scheduled(fixedRate = 30000)
    public void syncPendingOrders() {
        if (maintenanceState.isMaintenanceActive()) {
            // Guard: Skip batch job during active maintenance
            return;
        }
        // Execute business logic
    }
}
```

---

### 3. Custom Business Hooks (`MaintenanceHook`)
Implement `MaintenanceHook` in any Spring Bean to execute pre/post maintenance logic. Ordered via Spring's `@Order`:

```java
@Component
@Order(10)
public class WebSocketSessionDrainHook implements MaintenanceHook {

    private static final Logger log = LoggerFactory.getLogger(WebSocketSessionDrainHook.class);

    @Override
    public void onEnterMaintenance() {
        log.info("Sending reconnect advice to active WebSocket clients...");
        // Gracefully notify and close active client connections
    }

    @Override
    public void onExitMaintenance() {
        log.info("Re-opening WebSocket listener channels...");
        // Re-open connections or warm up local caches
    }
}
```

---

### 4. Application Domain Events (`MaintenanceModeChangedEvent`)
Listen to state transitions asynchronously or in decoupled modules:

```java
@Component
public class MaintenanceAuditLogger {

    @EventListener
    public void handleMaintenanceEvent(MaintenanceModeChangedEvent event) {
        if (event.isEntering()) {
            // Send alert to Slack, Datadog, or PagerDuty
            alertOnSlack("Pod entered maintenance mode. Reason: " + event.getReason());
        }
    }
}
```

---

### 5. Micrometer & Prometheus Metrics (Zero Dependency Overhead)

The starter provides built-in, production-grade observability via **Micrometer**. 

> [!NOTE]
> **Zero Dependency Overhead Guarantee:** `micrometer-core` is defined with `<optional>true</optional>`. If the host application does not use Micrometer, no classes are loaded, no beans are created, and zero additional dependencies are dragged into the project.

When `MeterRegistry` is present in the application context and `maintenance.metrics.enabled=true` (default), the following metrics are automatically recorded:

| Metric Name | Meter Type | Tags | Description |
| :--- | :--- | :--- | :--- |
| `maintenance.mode.active` | `Gauge` | None | Instantaneous status: `1.0` if in maintenance mode, `0.0` otherwise. |
| `maintenance.mode.duration.current.seconds` | `Gauge` | None | Real-time elapsed time (in seconds) the pod has spent in active maintenance. Resets to `0.0` when maintenance is inactive. |
| `maintenance.mode.transitions` | `Counter` | `direction=enter\|exit` | Cumulative counter tracking total maintenance transitions. |
| `maintenance.mode.window.duration` | `Timer` | None | Duration of the total maintenance window. Recorded upon exiting maintenance mode. |
| `maintenance.transition.duration` | `Timer` | `direction=enter\|exit`<br>`status=success\|partial_failure` | Execution latency of the orchestration transition workflow. |
| `maintenance.hook.duration` | `Timer` | `hook=<SimpleName>`<br>`direction=enter\|exit`<br>`status=success\|failed\|timed_out` | Execution latency per `MaintenanceHook`. |

#### 🛡️ Cardinality Protection
To safeguard time-series databases (e.g. Prometheus, Cortex, VictoriaMetrics, M3DB) against cardinality explosion, user-supplied free-text descriptions (the transition `reason`) are **never used as metric tags**. Only bounded, controlled enum-like values are attached as tags.

#### 📊 Prometheus & Grafana Query Examples

- **Alert: Pod stuck in maintenance mode for more than 1 hour:**
  ```promql
  maintenance_mode_duration_current_seconds > 3600
  ```

- **Dashboard: Number of pods currently in maintenance mode by service:**
  ```promql
  sum(maintenance_mode_active) by (app, namespace)
  ```

- **Alert: Maintenance transition encountered partial failure (e.g. hook error or timeout):**
  ```promql
  increase(maintenance_transition_duration_seconds_count{status="partial_failure"}[5m]) > 0
  ```

- **Alert: Maintenance hook execution failed or timed out:**
  ```promql
  increase(maintenance_hook_duration_seconds_count{status=~"failed|timed_out"}[5m]) > 0
  ```

- **Dashboard: 95th percentile transition duration (latency):**
  ```promql
  histogram_quantile(0.95, sum(rate(maintenance_transition_duration_seconds_bucket[5m])) by (le, direction))
  ```

---

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
