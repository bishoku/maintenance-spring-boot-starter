# Security policy

## Supported versions

Security fixes target the latest patch release of the current minor line (1.2.x).
Older lines do not have a promised backport schedule. Applications are responsible for
managing and updating their Spring Boot and optional broker dependencies.

## Report a vulnerability

Do not disclose a suspected vulnerability in a public issue. Email
**contact@bishokudev.com** (the project maintainer contact), with affected versions,
reproduction steps, impact and any suggested mitigation. Avoid sending credentials or
production data. Coordinate public disclosure with the maintainer; response times are
best effort rather than a guaranteed SLA.

The maintenance endpoint is disabled by default. Enabling Actuator operations does not
provide authentication. Applications control management endpoint authorization and network
access. Expected direct pod access during readiness refusal is not an HTTP-filter bypass:
the library deliberately delegates routing to Kubernetes.
