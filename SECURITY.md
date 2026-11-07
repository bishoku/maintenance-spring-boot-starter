# Security policy

## Supported versions

Security fixes target the latest patch release of the current minor line (1.3.x).
Older lines do not have a promised backport schedule. Applications are responsible for
managing and updating their Spring Boot and optional broker dependencies.

## Dependency ownership and scan scope

`spring-boot-autoconfigure` is non-optional. Its default transitive Boot/Framework
versions are maintained by this project; the 1.3.2 baseline is Boot 4.0.9 / Spring
Framework 7.0.10. Optional integrations are not automatically added to downstream Maven
consumers. The library's imported BOM is not automatically imported into the application;
the application's own dependency management can replace the default versions.

We distinguish the library-default downstream runtime graph from the library's full
build/test graph and from an application's actual runtime graph. All three can have
different security findings. An optional build dependency is not automatically an
application runtime vulnerability, but it must not be globally excluded from security
review. See [dependency security evidence](benchmark/reports/DEPENDENCY_SECURITY.md).

Boot 2.7 support is a functional compatibility contract, not a promise that an old
application BOM is secure. Upgrading this starter does not patch versions selected by
the application's BOM. Applications must evaluate the advisories affecting their own
resolved dependencies, update them, or use an appropriate supported maintenance stream.

## Report a vulnerability

Do not disclose a suspected vulnerability in a public issue. Email
**contact@bishokudev.com** (the project maintainer contact), with affected versions,
reproduction steps, impact and any suggested mitigation. Avoid sending credentials or
production data. Coordinate public disclosure with the maintainer; response times are
best effort rather than a guaranteed SLA.

The maintenance endpoint is not exposed over HTTP by default. Exposing Actuator operations does not
provide authentication. Applications control management endpoint authorization and network
access. Expected direct pod access during readiness refusal is not an HTTP-filter bypass:
the library deliberately delegates routing to Kubernetes.
