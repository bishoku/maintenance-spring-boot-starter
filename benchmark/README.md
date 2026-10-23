# Maintenance correctness benchmarks

The current contract is **kubernetes-readiness-v2**: the library signals readiness;
Kubernetes controls Service routing. It does not intercept or reject business HTTP
requests. This supersedes the earlier HTTP admission/drain interpretation recorded in
historical reports. See [contract correction](reports/READINESS_CONTRACT.md).

There are two complementary fixtures:

| Fixture | What it verifies |
|---|---|
| Compose + sample JVM | Broker lifecycle, readiness/liveness HTTP status, hook failure/timeout, retries, metrics, endpoint access policies and no local HTTP filtering |
| [MicroK8s](kubernetes/README.md) | Real kubelet probes, Pod readiness, EndpointSlice eligibility, fresh ClusterIP connections, existing connections, recovery and restart counts |

These are correctness/lifecycle harnesses, not throughput benchmarks. GitHub CI is unchanged.

## Compose

Prerequisites: Java 17+, Maven, Python 3, Docker Engine/Compose. Ports 18080, 29092,
25673 and 15673 must be free. Published ports bind to localhost.

```sh
docker compose -f benchmark/compose.yaml up -d --wait
python3 benchmark/run.py --label local-readiness --versions 3.5.9 4.0.2
docker compose -f benchmark/compose.yaml down --volumes
```

The runner installs the starter compiled against 3.5.9 and tests the **same JAR** with
both application BOMs. Boot 4 broker modules use the `boot4` Maven profile. Each scenario
runs in a fresh JVM. `--scenarios lifecycle propagation_grace` selects scenarios;
`--skip-build-starter` uses the installed artifact. Always use a new report label.

Maintenance readiness=503 plus direct `/work`=200 is the expected Compose result.
The propagation-grace test verifies the configured delay and that ongoing work is not
aborted; it deliberately does not claim all HTTP requests have drained. Actual Service
exclusion must be tested with MicroK8s, not inferred from a local JVM.

Broker fixtures include autoStartup=false listeners and delayed message handlers.
Injected hook/queue failures are deterministic fault injection, not network outage tests.
Credentials and `/bench/**` controls are local fixtures; never deploy this sample to a
shared production environment. Authentication is application-owned; endpoint access tests
only measure none/read-only/unrestricted Actuator policy.

## Spring Boot 2.7 runtime compatibility

The sample's `boot2` profile uses `management.endpoint.maintenance.enabled=true`;
its default `modern` profile uses Actuator `access=unrestricted`. Runners select
`boot2` automatically for 2.x application BOMs. The read-only access scenario is
explicitly excluded for Boot 2.x; endpoint disabling is tested with `enabled=false`.
All broker, hook, failure/retry, metrics, readiness and propagation-grace checks remain.

To test the published 1.2.0 binary instead of rebuilding library sources:

```sh
docker compose -f benchmark/compose.yaml up -d --wait
python3 benchmark/run.py --label boot27-java17 --versions 2.7.0 2.7.18 \
  --java-home /path/to/jdk17 --skip-build-starter \
  --expected-starter-sha256 690cd742f734b4c9ed2e35afaa20e2eccdde70a4fcdbb4679ac1617c071b3eca
docker compose -f benchmark/compose.yaml down --volumes
```

Maven must resolve the published artifact from Central or its local cache. The hash
check fails if the executable sample contains a different starter. `--java-home`
selects both the sample compiler and JVM; reports also record managed runtime libraries.
Each Compose scenario has its own Kafka group to avoid stale members from an earlier
JVM delaying a later scenario's warm-up. The MicroK8s pods share one group per tested
Boot version, retaining the two-pod/one-partition case.

The Kubernetes runner accepts the same three options. `--java-home` controls its local
build; Kubernetes sample pods still use the manifest's Java 17 JRE, recorded in results.
Runtime compatibility of the published JAR is separate from recompiling the starter
against a 2.7 BOM: the current library source still uses modern Actuator annotations.

## MicroK8s

```sh
sudo microk8s start  # only if stopped; enter passwords in your own terminal
python3 benchmark/kubernetes/run.py --label cluster-readiness --versions 3.5.9 4.0.2
```

See [MicroK8s instructions](kubernetes/README.md) for namespace isolation, JAR upload,
cleanup and the exact Service/EndpointSlice assertions. It does not need Compose running.

## Library source matrix

```sh
python3 benchmark/test-library.py --label source-local --versions 3.5.9 4.0.2
python3 benchmark/test-library.py --label source-java17 --java-home /path/to/jdk17 --versions 3.5.9 4.0.2 --verify
```

`--verify` additionally builds JAR, sources and Javadoc artifacts without publishing.
Source compilation against each BOM and runtime consumption of the same JAR are distinct
checks. Run one E2E runner at a time; avoid other builds during starter installation.

JSON and Markdown reports are retained; raw `.log` files are local and ignored by Git.
A failed/blocked run exits nonzero. Inspect `completed`/`error` on Kubernetes runs and
per-check outcomes on Compose runs; zero checks does not establish compatibility.
Only exact Boot/JVM combinations recorded in successful reports are verified.
