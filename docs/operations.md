# Operations and configuration

See the [README](../README.md) for installation and quick start. This guide describes
the detailed lifecycle contract and optional integrations.

## Configuration

```yaml
management:
  endpoints.web.exposure.include: health,maintenance
  endpoint:
    maintenance.access: unrestricted
    health.probes.enabled: true
maintenance:
  drain-delay: 5s          # propagation grace period, NOT an in-flight completion guarantee
  queue-timeout: 30s      # broker pause/stop acknowledgement deadline
  hook-timeout: 30s       # per hook; cancellation is cooperative
  start-in-maintenance: false # boot directly in ACTIVE maintenance mode (alias: maintenance.startup.enabled)
  startup:
    enabled: false
    reason: "Startup maintenance mode"
```

The endpoint defaults to `Access.NONE`. Use `access=read-only` for status-only access.
Do not configure both `management.endpoint.maintenance.enabled` and `.access`.
The legacy explicit `enabled=false` still disables the entire auto-configuration;
prefer the new access model and per-feature switches for new applications.

`unrestricted` enables operations; it does **not** authenticate callers. Restrict
management access with Spring Security authorization and network policy. A separate
management port alone is not authentication. Basic/session authentication also needs
an appropriate CSRF policy/token for POST; do not disable CSRF globally to make curl work.
Use a pod-specific management route that remains reachable when readiness is false.

## Operations and failure contract

```sh
curl http://localhost:8080/actuator/maintenance
curl -H 'Content-Type: application/json' -d '{"enabled":true,"reason":"planned maintenance"}' \
  http://localhost:8080/actuator/maintenance
curl -H 'Content-Type: application/json' -d '{"enabled":false}' \
  http://localhost:8080/actuator/maintenance
```

Authenticate these requests according to your application's security policy.

The response contains `active`, `lastChanged`, `reason` and `details`. **HTTP 200 is an
operation response, not proof of a completed transition.** Inspect `details.phase` and
`details.partialFailure`. `active` retains the last confirmed mode on failure.

| Phase | Meaning |
|---|---|
| INACTIVE | Normal operation |
| ENTERING | Readiness/grace period and broker/hook steps running |
| ACTIVE | All enabled entry steps confirmed |
| EXITING | Exit preparation running before readiness acceptance |
| FAILED | A critical step failed; readiness acceptance is withheld; inspect details |

Enter: refuse readiness → propagation grace → confirm broker suspension → hooks → ACTIVE.
Exit: hooks → resume owned consumers → accept readiness → INACTIVE.
Startup (`start-in-maintenance=true` or `startup.enabled=true`): initializes `MaintenanceState` directly in `ACTIVE`, enforces `REFUSING_TRAFFIC` immediately (with zero drain delay), and pauses/stops managed broker consumers on startup so that a subsequent `POST /actuator/maintenance` with `{"enabled":false}` executes the normal exit sequence.
Concurrent commands
are serialized. Duplicate commands in a completed state are no-ops; failed commands can
be retried. Failed exit attempts withhold readiness acceptance and attempt to suspend
any partially resumed consumers. Readiness-publication failures are reported, not treated
as proof that Kubernetes has stopped traffic.

Hooks run in order. Failure/timeout stops subsequent hooks. Timeout requests interruption;
Java cannot forcibly terminate a hook ignoring interruption. While that hook remains
running, subsequent transitions cannot complete. Hooks must be interruptible, bounded and
idempotent because a retry repeats them. Applications should inspect event reports:
`MaintenanceModeChangedEvent` also carries failed transition attempts; only reports
without `partialFailure` represent completion.

## Broker semantics

- Kafka uses container `pause()` and waits for acknowledgement from assigned consumers.
  Unassigned consumers retain their pause request without waiting for a partition-only
  acknowledgement; concurrent container children are checked separately on pause/resume. Consumers keep
  polling while paused, avoiding a maintenance-induced group leave. External broker
  failures/topology changes can still cause rebalance. Pause can wait for the current
  poll batch to finish; choose acknowledgement timeouts accordingly.
- Only containers paused by maintenance are resumed. Externally paused and previously
  stopped containers are preserved. Do not concurrently mutate listener registries,
  dynamically register/start consumers, or change external pause ownership during a window.
- RabbitMQ uses container stop callbacks and restores only previously running containers.
  This is stop/start, not Kafka-style pause. Ack mode, prefetch, transactions,
  shutdown timeout and forced channel closure determine redelivery behavior. A stop
  callback is not proof that arbitrary offloaded business work has finished. Track such
  work explicitly; handlers must tolerate redelivery. Custom lifecycle implementations
  must bound their own blocking calls; queue-timeout cannot forcibly interrupt them.

## Startup maintenance

When startup maintenance is enabled, the starter defers the initial automatic start of
Kafka/Rabbit listener containers created by Spring-managed factories. It records only
containers whose resolved `autoStartup` was true, after annotation/factory settings and
user customizers have been applied. Exit restores that setting and starts those containers.
Explicitly disabled listeners remain stopped. Kafka joins its group on this first exit;
subsequent dynamic maintenance windows use the existing pause/resume behavior.

Factory interception uses class proxies to preserve concrete factory injection. Custom
factories and their `createListenerContainer` methods must be non-final. An unsupported
factory fails application initialization instead of silently bypassing deferral. Custom
factories must not start a container before returning it. Manually started containers and
late endpoint registration after context refresh are not part of the startup guarantee.
Disabling queue management or an individual broker integration disables its deferral too.

The following existing startup constraints remain:

- Wait for application startup initialization to finish before requesting maintenance
  exit. Management HTTP availability alone does not establish startup completion. Exit
  during startup is not serialized with the startup initializer and can leave an INACTIVE
  pod with consumers suspended; issuing another exit is not a repair mechanism.
- Use one startup switch/configuration source. A true uppercase startup environment flag
  remains effective even when a higher-priority canonical property says false. Remove or
  change that environment flag to disable startup maintenance. `MAINTENANCE_STARTUP_REASON`
  also takes precedence over the bound reason.
- Startup queue-stop errors are logged but are not reflected as FAILED/partialFailure
  in the management snapshot. Inspect startup errors; ACTIVE alone is not proof that
  every custom lifecycle manager completed successfully.

The [1.3.0 review](../benchmark/reports/V130_REVIEW.md) retains the original reproductions.
The [startup consumption fix report](../benchmark/reports/STARTUP_CONSUMPTION_FIX.md)
records the separate validation of initial consumer deferral.

## Kubernetes routing and background work

Configure readiness and liveness probes separately:

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

Do not use a combined health endpoint affected by maintenance as liveness. Keep the
Service's `publishNotReadyAddresses` false. When all endpoints are unready, fresh Service
connections may fail or time out; Kubernetes does not promise an application-generated
503 body. Use a separate pod-specific management route to exit maintenance. A
`kubectl port-forward service/...` connection bypasses normal Service routing and is
not a valid test of endpoint exclusion.

[MicroK8s benchmark](../benchmark/kubernetes/README.md) tests actual ClusterIP routing from
another pod, EndpointSlice conditions, direct Pod-IP access, existing connections,
readiness recovery and unchanged pod UID/restart count. Compose tests cover local
readiness/broker behavior; they do not prove Kubernetes routing.

Scheduled jobs are not automatically paused. Applications may consult
`MaintenanceState.isWorkBlocked()` (includes transition/failure phases) or implement
an explicit hook to coordinate their own jobs. The library does not count or forcibly
drain HTTP, WebFlux, WebSocket or gRPC work. Other readiness publishers must coordinate
to avoid last-writer overrides. Multi-replica maintenance requires external orchestration.

## Feature switches and metrics

`maintenance.readiness.enabled`, `queues.enabled`, `queues.kafka.enabled`,
`queues.rabbit.enabled`, `hooks.enabled`, `events.enabled`, `metrics.enabled` default to true.
Disabling an entry step explicitly gives up its guarantee.

Micrometer exposes `maintenance.mode.active`, `maintenance.mode.duration.current.seconds`,
`maintenance.mode.transitions`, `maintenance.mode.window.duration`,
`maintenance.transition.duration`, and `maintenance.hook.duration`.
Failure attempts contribute to transition/hook timers, not successful mode counters.
Metrics based on events are unavailable when `maintenance.events.enabled=false`.
No free-text reason is used as a metric tag. Configure Micrometer histogram publication
explicitly before using Prometheus `_bucket` / histogram_quantile queries.

Apache License 2.0; see [LICENSE](../LICENSE).
