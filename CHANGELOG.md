# Changelog

## 1.3.2

Upgrade the build/default dependency baseline from Spring Boot 3.5.9 to 4.0.9, including
the non-optional `spring-boot-autoconfigure` / `spring-boot` dependencies and their
Spring Framework 7.0.10 transitives. Consumer applications retain their own Boot BOM;
Boot 2.7 compatibility is validated separately from security of that application's stack.
Add a minimal downstream-consumer dependency audit with OSV before/after evidence.

## 1.3.1

Prevent Kafka/RabbitMQ message consumption while an application starts in maintenance
mode by deferring the initial automatic listener start. Maintenance exit starts only
the deferred listeners; explicitly disabled listeners remain stopped. Later maintenance
windows retain Kafka pause/resume and RabbitMQ stop/start behavior. Document startup
exit timing, property precedence and custom factory constraints. See the
[release and upgrade notes](release-notes/v1.3.1.md).

## 1.3.0

Support starting applications directly in maintenance mode (`maintenance.start-in-maintenance`
/ `maintenance.startup.enabled`) with immediate readiness refusal, startup pausing of managed
Kafka/RabbitMQ listeners, and dynamic exit via `/actuator/maintenance` while preserving
`@Scheduled` behavior. Read the [release and upgrade notes](release-notes/v1.3.0.md).

## 1.2.0

Readiness lifecycle reliability, Spring Boot 3.5/4 compatibility, real-broker/Kubernetes
validation and complete GitHub release assets. Includes configuration and behavior changes;
read the [release and upgrade notes](release-notes/v1.2.0.md).

## Earlier releases

See [GitHub releases](https://github.com/bishoku/maintenance-spring-boot-starter/releases)
and the [commit history](https://github.com/bishoku/maintenance-spring-boot-starter/commits/main/).
