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
