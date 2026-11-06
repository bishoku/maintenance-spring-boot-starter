# Changelog

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
