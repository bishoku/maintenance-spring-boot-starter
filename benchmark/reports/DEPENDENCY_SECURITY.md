# Mandatory dependency security baseline

Date: 2026-11-07. Scope: dependency changes for 1.3.2 after published 1.3.1.
Tracked in [issue #3](https://github.com/bishoku/maintenance-spring-boot-starter/issues/3).
No library Java or GitHub Actions changes are part of this update.

## Change and rationale

The root Boot BOM/build baseline changes from **3.5.9 to 4.0.9**. The non-optional
`spring-boot-autoconfigure` and `spring-boot` defaults become 4.0.9, with a coherent
Spring Framework **7.0.10** transitive graph instead of 6.2.15. Java 17 remains the
minimum. Optional integration declarations remain optional; their build versions
follow the same BOM, and consumer applications continue supplying those integrations.

Only upgrading to 3.5.16 would leave Framework 6.2.19. The publisher identifies
6.2.19 as affected by [CVE-2026-59283](https://spring.io/security/cve-2026-59283/),
with a public fix in 7.0.9 and a commercial 6.2.20 fix. Boot 4.0.9 manages 7.0.10.
[Boot 3.5.16 was the final OSS 3.5 release](https://spring.io/blog/2026/06/25/spring-boot-3-5-16-available-now/).
Using the coherent 4.0 BOM avoids mixing independently selected Boot/Framework versions.
The autoconfiguration [Artemis advisory](https://spring.io/security/cve-2026-41001/)
also affects the old baseline; its public 4.0 fix starts at 4.0.7.

## Actual downstream graph and findings

A separate Maven consumer depends only on the starter, without a consumer BOM.
This measures mandatory transitives rather than filtering optional dependencies out
of the starter's own build graph. Kafka and Rabbit integration artifacts are absent
in both resolved consumer graphs.

| Graph | Boot | Framework | OSV findings |
|---|---|---|---|
| Published 1.3.1 | 3.5.9 | 6.2.15 | 6 advisories across 4 components |
| Local candidate | 4.0.9 | 7.0.10 | 0 matches across 11 components |

The before graph matched CVE-2026-40973 (`spring-boot`), CVE-2026-41001
(`spring-boot-autoconfigure`), CVE-2026-41848 (`spring-core`), and
CVE-2026-41850 / 41851 / 41852 (`spring-expression`). These are version/advisory
matches, not findings that each vulnerable code path is reachable through this library.

Raw evidence: [before](security-before-consumer/results.json),
[after](security-after-consumer/results.json), and their adjacent consumer POM/tree JSON.
The old root build tree in `dependency-security/before-tree.json` is ancillary evidence
and includes optional/test dependencies; it is not the audited downstream graph.

Zero matches means OSV returned no non-withdrawn advisories for the exact resolved
versions at scan time. It is not a blanket absence-of-vulnerabilities guarantee;
databases can lag publisher notices (including the later SpEL advisory above).
Build/test dependencies and a full application's graph require their own review.
Do not globally suppress optional dependency findings.

## Spring Boot 2.7 compatibility and ownership

The library's imported BOM is not inherited as the consumer's dependency management.
The application must use its own Boot parent/BOM to keep a coherent stack; Maven's
[dependency management rules](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html)
allow that management to override transitive defaults. Resolution checks confirm:

| Consumer BOM | Resolved Boot/autoconfigure | Resolved Framework |
|---|---|---|
| 2.7.0 | 2.7.0 | 5.3.20 |
| 2.7.18 | 2.7.18 | 5.3.31 |

See [2.7.0 graph](security-consumer-boot270/results.json) and
[2.7.18 graph](security-consumer-boot2718/results.json). These two runs do not request
a security scan. **Functional Boot 2.7 compatibility does not imply a secure Boot 2.7
application stack**: updating the starter cannot patch versions selected by the
consumer BOM. The application owns updates or a supported maintenance stream.
Boot 2.7 continues using `enabled=true`; modern Actuator access policies are not
part of its compatibility contract. No separate starter JAR is introduced.

## Validation and binary identity

The [Java 17 source matrix](security-source-jdk17/results.json) passes **188 tests
each** against Boot 3.5.9 and 4.0.9, with zero failures/errors/skips and `verify`
including JAR, sources and Javadoc artifacts. Source compilation against a 2.7 BOM
is not claimed; runtime consumption of the same modern-built JAR is tested instead.

Candidate: `1.3.2-security-SNAPSHOT`, a temporary local version, never published.
It was built in an isolated copy with only the project version changed semantically,
using the root 4.0.9 baseline and Java 17. Its SHA-256 is:

```text
bb8476ac2b9cb747e7c5b77b6931ceeb5cf59a3698e824fa09aa94f77bf24ec8
```

[Candidate metadata](dependency-security/candidate.json) records source equality and
the preserved published 1.3.1 artifact hash. All runtime suites require this candidate
hash, preventing accidental testing of the old installed release.

The [Kubernetes startup suite](security-startup-k8s/summary.md) passes **236 checks**:
59 each on Boot 2.7.0, 2.7.18, 3.5.9 and 4.0.9, with zero failed checks.
The [two-pod regression](security-boot27-regression-k8s/summary.md) passes another
**70 checks** (35 each on 2.7.0 and 2.7.18). Runtime JAR inventories confirm that
Boot 2.7 applications load Framework 5.3.20/5.3.31, without Boot 4 or Framework 7
leaking into their packaged application. Real MicroK8s tests use
Kafka/Rabbit brokers, kubelet probes, EndpointSlices and fresh Service connections.
They exercise management access while excluded from the Service, exit and delivery
of pending messages, explicitly disabled listeners, and a second maintenance cycle.
Both test namespaces were successfully removed; MicroK8s remains running.
The early-exit race and property-precedence behavior remain outside this change.

## Reproduction

Follow the [audit instructions](../security/README.md) to install an isolated candidate.
Use new labels and run E2E suites serially (they share sample build output):

```sh
python3 benchmark/security/audit-consumer.py --label security-default-new \
  --starter-version 1.3.2-security-SNAPSHOT --scan
python3 benchmark/test-library.py --label security-source-new \
  --java-home /path/to/jdk17 --versions 3.5.9 4.0.9 --verify
python3 benchmark/kubernetes/startup.py --label security-startup-new \
  --starter-version 1.3.2-security-SNAPSHOT --skip-build-starter \
  --expected-starter-sha256 bb8476ac2b9cb747e7c5b77b6931ceeb5cf59a3698e824fa09aa94f77bf24ec8 \
  --expect-deferred-startup --cases flat cold-window \
  --java-home /path/to/jdk17 --versions 2.7.0 2.7.18 3.5.9 4.0.9
```

The two-pod check uses the same artifact and hash:

```sh
python3 benchmark/kubernetes/run.py --label security-regression-new \
  --starter-version 1.3.2-security-SNAPSHOT --skip-build-starter \
  --expected-starter-sha256 bb8476ac2b9cb747e7c5b77b6931ceeb5cf59a3698e824fa09aa94f77bf24ec8 \
  --java-home /path/to/jdk17 --versions 2.7.0 2.7.18
```

A newly rebuilt candidate can have a different hash; use its actual SHA-256.
Compose/Kubernetes builds now honor the root build baseline instead of overriding
it with a hard-coded 3.5.9. Historical reports remain unchanged.

## Release packaging

Local Java 17 `clean verify` for project version **1.3.2** passed another **188 tests**
and built the JAR, source and Javadoc artifacts. All **39 non-Maven entries** in the
release JAR are byte-identical to the candidate used in the Kubernetes suites
(only Maven version metadata differs). See [packaging evidence](dependency-security/release-package.json).
This links the tested candidate to the local release package, not to a signed Central JAR.
