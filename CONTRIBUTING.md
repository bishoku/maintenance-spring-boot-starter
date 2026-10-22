# Contributing

Bug reports, documentation improvements, reproducible examples and focused pull requests
are welcome. Participation follows our [code of conduct](CODE_OF_CONDUCT.md).

## Before starting

Search [existing issues](https://github.com/bishoku/maintenance-spring-boot-starter/issues)
and pull requests. For a substantial feature or API change, open an issue describing the
problem and proposed behavior first. Report vulnerabilities privately using [SECURITY.md](SECURITY.md).

## Local setup

Install Java 17+ and Maven, fork the repository, clone your fork and create a branch:

```sh
git clone https://github.com/YOUR_USERNAME/maintenance-spring-boot-starter.git
cd maintenance-spring-boot-starter
git switch -c fix/describe-the-change
mvn --batch-mode clean verify
```

The source baseline is Boot 3.5.9. Also verify changes against Boot 4.0.2:

```sh
mvn --batch-mode -Dspring-boot.version=4.0.2 clean verify
```

Unit and application-context tests do not require Docker or Kubernetes. For changes to
broker lifecycle or readiness behavior, use the relevant local [benchmark scenarios](benchmark/README.md).
Run one benchmark/build at a time: they share the sample target directory and local artifact.
Benchmark runs are intentionally outside GitHub Actions. Never commit credentials, raw logs,
compiled JARs, IDE settings or production data.

## Scope and behavior

This starter publishes readiness and coordinates pod-local consumers/hooks. Kubernetes
owns Service routing. Direct pod access during maintenance is valid, and management exit
must remain possible. HTTP filtering, distributed maintenance orchestration and persistence
across pod replacement are outside the library's scope.

Preserve optional integrations, the Java 17 baseline and the application's dependency BOM.
Add meaningful regression tests for behavior changes. Update documentation and release notes
when a change affects configuration, public APIs or failure semantics. Do not change release
versions or publish artifacts as part of an unrelated contribution.

## Pull requests

Open a PR against `main` with the problem, resulting behavior and commands/results used to
validate it. Link the issue if one exists. Keep unrelated refactors separate. Maintainers may
request changes before merging; no response-time guarantee is implied.

By contributing, you agree that your contribution is provided under this repository's
[Apache License 2.0](LICENSE). No separate CLA or sign-off process is currently required.
