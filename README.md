# Maintenance Spring Boot Starter

[![CI](https://github.com/bishoku/maintenance-spring-boot-starter/actions/workflows/ci.yml/badge.svg)](https://github.com/bishoku/maintenance-spring-boot-starter/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.bishokudev/maintenance-spring-boot-starter)](https://central.sonatype.com/artifact/com.bishokudev/maintenance-spring-boot-starter)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Put a Spring Boot application into maintenance mode without stopping its pod.
The starter changes Kubernetes readiness, coordinates Kafka/RabbitMQ listeners,
and runs your maintenance hooks.

**Kubernetes controls traffic routing.** The starter makes the readiness probe return
503; Kubernetes excludes the pod from the Service's ready endpoints. Liveness remains
healthy, and the management endpoint remains accessible through a pod-specific route.
There is no business HTTP filter; direct pod requests and existing connections can continue.

## Install

Requires Java 17+. Version **1.3.2** uses Boot **4.0.9** / Framework **7.0.10** as
its default dependency/build baseline. Use your application's Spring Boot BOM to
manage a coherent runtime stack; the starter does not override that BOM.

The same candidate JAR passed Kubernetes validation with Boot **2.7.0, 2.7.18,
3.5.9 and 4.0.9 / Java 17**; source tests pass against 3.5.9 and 4.0.9. Boot 2.7
uses the legacy endpoint enablement contract described below. Functional compatibility
does not patch security issues in dependencies selected by an old application BOM.
See the [dependency security and compatibility report](benchmark/reports/DEPENDENCY_SECURITY.md)
for exact artifacts, findings and limitations; [earlier compatibility results](benchmark/reports/BOOT27_COMPATIBILITY.md)
cover additional Boot/JVM combinations on previous artifacts.

```xml
<dependency>
    <groupId>com.bishokudev</groupId>
    <artifactId>maintenance-spring-boot-starter</artifactId>
    <version>1.3.2</version>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

Gradle users can add `implementation("com.bishokudev:maintenance-spring-boot-starter:1.3.2")`
and `implementation("org.springframework.boot:spring-boot-starter-actuator")`.
Your application also needs a web stack to expose the HTTP endpoint. Broker integrations
are optional: bring your application's Kafka/AMQP dependencies when using them.
See [upgrade notes](release-notes/v1.3.2.md) before upgrading from earlier releases.

## Enable maintenance

```yaml
management:
  endpoints.web.exposure.include: health,maintenance
  endpoint:
    maintenance.access: unrestricted
    health.probes.enabled: true
maintenance:
  drain-delay: 5s
  queue-timeout: 30s
  hook-timeout: 30s
  start-in-maintenance: false # set true (or MAINTENANCE_START_IN_MAINTENANCE=true) to boot in maintenance mode
  startup:
    enabled: false            # alias for start-in-maintenance (or MAINTENANCE_STARTUP_ENABLED=true)
    reason: "Startup maintenance mode"
```

The endpoint is disabled by default. `unrestricted` enables its read/write operations;
it is not authentication. Apply your application's management authorization policy.
Use `access: read-only` if only status should be exposed. Do not configure both the
legacy `maintenance.enabled` endpoint property and `maintenance.access` under
`management.endpoint`.

### Start directly in maintenance mode

To boot an application directly into maintenance mode (for example during Kubernetes
platform migrations, pre-cutover verification, or dark launches), enable
`maintenance.start-in-maintenance: true` (or `maintenance.startup.enabled: true`):

```yaml
maintenance:
  start-in-maintenance: true
  startup:
    reason: "k8s-migration-verification"
```

When enabled at startup:

- `MaintenanceState` initializes immediately in the `ACTIVE` phase (`isMaintenanceActive() == true`, `isWorkBlocked() == true`).
- The Kubernetes readiness probe reports `503 OUT_OF_SERVICE` (`REFUSING_TRAFFIC`) from startup while liveness remains `UP`, keeping the pod running without receiving Service traffic.
- Managed Kafka and RabbitMQ listener containers have their initial automatic start deferred. They do not consume messages before maintenance exit; Kafka consumers do not join their group during this initial window.
- Scheduled jobs (`@Scheduled`) that check `MaintenanceState.isWorkBlocked()` see maintenance mode active from their first execution.
- Once startup initialization and verification are complete, send `POST /actuator/maintenance` with `{"enabled":false}` to start the deferred consumers and transition readiness to `ACCEPTING_TRAFFIC` (`200 OK`) without restarting the pod. Listeners configured with `autoStartup=false` remain stopped. Subsequent maintenance windows retain Kafka pause/resume and RabbitMQ stop/start behavior.

Startup deferral applies to `@KafkaListener`/`@RabbitListener` endpoints created through
Spring-managed listener factories during initial context startup, with queue management enabled. It preserves
endpoint settings and factory customizers. Custom factories and their
`createListenerContainer` methods must be non-final and must not start containers before
returning them. Manual starts and endpoints registered after context refresh are outside
this startup guarantee; see [operating constraints](docs/operations.md#startup-maintenance).

Initial-start deferral is available from **1.3.1**. Version 1.3.0 starts listeners before
pausing them and does not guarantee zero startup consumption. See the
[fix validation report](benchmark/reports/STARTUP_CONSUMPTION_FIX.md).

Configure the pod probes separately:

```yaml
readinessProbe:
  httpGet: {path: /actuator/health/readiness, port: 8080}
  periodSeconds: 2
  failureThreshold: 1
livenessProbe:
  httpGet: {path: /actuator/health/liveness, port: 8080}
  periodSeconds: 10
  failureThreshold: 3
```

Keep the application Service's `publishNotReadyAddresses` false. `drain-delay` provides
a propagation grace period; it does not confirm that every connection or request has drained.

## Spring Boot 2.7 configuration

Use the same dependency and JAR, with Java 17 or a later JVM in the tested matrix.
Configure legacy endpoint enablement instead of Actuator access modes:

```yaml
management:
  endpoints.web.exposure.include: health,maintenance
  endpoint:
    maintenance.enabled: true
    health.probes.enabled: true
```

Set `management.endpoint.maintenance.enabled=false` to disable it. Boot 2.7 does not
implement `maintenance.access=none/read-only/unrestricted` under `management.endpoint`;
these modes are outside the 2.7 compatibility contract. Readiness, management exit,
consumer orchestration, hooks and failure/retry behavior use the same library code.
Runtime compatibility of this JAR does not mean the current source compiles against
a Boot 2.7 BOM; the source build continues to use modern Actuator annotations.

## Enter, inspect and leave

Send management requests through a route that remains available for an unready pod,
such as a direct Pod-IP from an authorized in-cluster client. The ordinary application
Service will exclude the pod during maintenance.

```sh
# Add the authentication required by your application.
curl http://POD_IP:8080/actuator/maintenance
curl -H 'Content-Type: application/json' \
  -d '{"enabled":true,"reason":"planned maintenance"}' \
  http://POD_IP:8080/actuator/maintenance
curl -H 'Content-Type: application/json' -d '{"enabled":false}' \
  http://POD_IP:8080/actuator/maintenance
```

Enter follows **refuse readiness → grace period → suspend consumers → enter hooks**.
Exit follows **exit hooks → resume owned consumers → accept readiness**.

Check `details.phase` and `details.partialFailure`, not just HTTP 200. Successful entry
reports `ACTIVE`; successful exit reports `INACTIVE`. A failed transition reports `FAILED`,
withholds readiness acceptance and can be retried. `active` retains the last confirmed mode.
Maintenance state is local to the application process.

## Add application hooks

```java
import com.bishokudev.maintenance.hook.MaintenanceHook;
import org.springframework.stereotype.Component;

@Component
class ApplicationMaintenanceHook implements MaintenanceHook {
    @Override
    public void onEnterMaintenance() {
        // Coordinate application-owned background work.
    }

    @Override
    public void onExitMaintenance() {
        // Prepare the application before consumers and Service routing resume.
    }
}
```

Hooks must be bounded, interruptible and idempotent. A timeout requests interruption;
Java cannot forcibly stop a hook that ignores it. Scheduled jobs are not paused automatically.

Kafka listeners are paused without deliberately leaving their consumer group. RabbitMQ
listeners are stopped and only those owned by maintenance are restarted. Previously stopped
or externally paused listeners are preserved. See [operations and configuration](docs/operations.md)
for failure handling, broker semantics, feature switches and metrics.

## Verification and development

```sh
mvn --batch-mode clean verify
mvn --batch-mode -Dspring-boot.version=4.0.9 clean verify
```

The [local benchmark](benchmark/README.md) includes a version-selectable sample,
Docker Compose Kafka/RabbitMQ, and real MicroK8s Service/EndpointSlice tests.
The [validation report](benchmark/reports/READINESS_CONTRACT.md) records 83 library tests
per tested Boot/JDK combination, 64 Compose checks and 68 Kubernetes checks, including
management access and exit while all application pods are unready.
Benchmarks are run locally; **GitHub Actions does not start Docker brokers or MicroK8s**.

## Community

- [Contributing](CONTRIBUTING.md): setup, tests and pull requests.
- [Support](SUPPORT.md): questions, bug reports and feature requests.
- [Security policy](SECURITY.md): private vulnerability reporting.
- [Code of conduct](CODE_OF_CONDUCT.md): community expectations.
- [Changelog](CHANGELOG.md) and [GitHub releases](https://github.com/bishoku/maintenance-spring-boot-starter/releases): upgrade notes and downloadable artifacts.
- [Release process](docs/releasing.md): maintainer instructions.

Licensed under the [Apache License 2.0](LICENSE).
