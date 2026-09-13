# \`maintenance-spring-boot-starter\`

A lightweight, production-ready Spring Boot starter library that manages **Maintenance Mode** for Spring Boot microservices running in Kubernetes environments, exposed safely via Spring Boot Actuator.

---

## Key Features

- **Kubernetes Readiness Integration:** Automatically publishes \`ReadinessState.REFUSING_TRAFFIC\` / \`ACCEPTING_TRAFFIC\` so Kubernetes probes immediately adjust service endpoints.
- **Configurable Drain Delay:** Waits for Kubernetes to update endpoints before stopping consumers, preventing in-flight request loss.
- **Dynamic Queue Lifecycle Management:** Stops/resumes Kafka and RabbitMQ consumers via typed \`ConsumerLifecycleManager\` beans. Fully decoupled — if brokers are absent, the starter works without warnings.
- **Hook Timeout Protection:** Each \`MaintenanceHook\` runs with a configurable per-hook timeout, preventing rogue hooks from blocking the entire transition.
- **Micrometer Metrics:** Optional \`maintenance.mode.active\` gauge and \`maintenance.mode.transitions\` counter (requires Micrometer on classpath).
- **Actuator Endpoint (\`/actuator/maintenance\`):**
  - \`@ReadOperation\`: Returns state, timestamp, reason, and transition details.
  - \`@WriteOperation\`: Idempotently toggles maintenance mode.
  - **Disabled by default** for security — must be explicitly enabled.
- **Developer Extension Points:**
  - \`MaintenanceState\`: Thread-safe, snapshot-based state holder injectable into any bean.
  - \`MaintenanceHook\`: Callback interface with ordered, timeout-protected execution.
  - \`MaintenanceModeChangedEvent\`: Immutable \`ApplicationEvent\` for decoupled \`@EventListener\` handling.
- **Modern Auto-Configuration:** Registered via \`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports\`.

---

## Dependency Setup

### Maven

```xml
<dependency>
    <groupId>com.bishokudev</groupId>
    <artifactId>maintenance-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## Configuration

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,maintenance
  endpoint:
    maintenance:
      enabled: true  # Required — endpoint is disabled by default

maintenance:
  drain-delay: 5s      # Time to wait after REFUSING_TRAFFIC before stopping consumers
  hook-timeout: 30s    # Max time per MaintenanceHook before timeout
```

---

## Security

> **⚠️ IMPORTANT:** The maintenance endpoint can take a pod out of service. Always protect it.

```java
@Configuration
public class ActuatorSecurityConfig {

    @Bean
    public SecurityFilterChain actuatorSecurity(HttpSecurity http) throws Exception {
        http.securityMatcher(EndpointRequest.toAnyEndpoint())
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(EndpointRequest.to("health", "info")).permitAll()
                .requestMatchers(EndpointRequest.to("maintenance")).hasRole("OPS")
                .anyRequest().authenticated()
            );
        return http.build();
    }
}
```

---

## Usage Examples

### Check Current Status
```bash
curl http://localhost:8081/actuator/maintenance
```

### Enter Maintenance Mode
```bash
curl -X POST http://localhost:8081/actuator/maintenance \\
  -H "Content-Type: application/json" \\
  -d '{"enabled": true, "reason": "Database migration v2.4"}'
```

### Exit Maintenance Mode
```bash
curl -X POST http://localhost:8081/actuator/maintenance \\
  -H "Content-Type: application/json" \\
  -d '{"enabled": false, "reason": "Migration completed"}'
```

### Querying State in Scheduled Tasks

```java
@Component
public class BatchJobRunner {

    private final MaintenanceState maintenanceState;

    public BatchJobRunner(MaintenanceState maintenanceState) {
        this.maintenanceState = maintenanceState;
    }

    @Scheduled(fixedRate = 60000)
    public void runJob() {
        if (maintenanceState.isMaintenanceActive()) {
            return; // Skip during maintenance window
        }
        // Business logic
    }
}
```

### Custom Maintenance Hooks

```java
@Component
@Order(10)
public class CacheDrainHook implements MaintenanceHook {

    @Override
    public void onEnterMaintenance() {
        // Flush buffers, disconnect websockets, etc.
    }

    @Override
    public void onExitMaintenance() {
        // Warm caches, re-establish connection pools
    }
}
```

### Listening to Maintenance Events

```java
@Component
public class MaintenanceAuditNotifier {

    @EventListener
    public void onMaintenanceChange(MaintenanceModeChangedEvent event) {
        if (event.isEntering()) {
            // Notify Slack, PagerDuty, or audit logs
        }
    }
}
```

---

## Architecture Notes

- **Drain Delay:** After publishing \`REFUSING_TRAFFIC\`, the coordinator waits \`maintenance.drain-delay\` before stopping consumers. This allows Kubernetes time to update Service endpoints and stop routing new requests.
- **State Persistence:** Maintenance state is in-memory. If a pod restarts, it comes up in normal (non-maintenance) mode. This is intentional — Kubernetes orchestrates pod lifecycle, and maintenance is a transient operational state.
- **Liveness Probes:** Only readiness is affected. Liveness probes continue to pass, so Kubernetes will not restart pods in maintenance mode.
