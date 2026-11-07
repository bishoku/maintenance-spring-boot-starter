# MicroK8s readiness / Service benchmark

This fixture validates the intended routing contract: **the library changes readiness;
Kubernetes removes an unready pod from eligible Service backends**. There is no library
HTTP filter. New direct Pod-IP requests remain possible, and already established
connections are not terminated by a readiness change.

Prerequisites: local MicroK8s, permission to run `microk8s kubectl`,
Maven, Java 17+, Python 3. The fixture needs approximately 3 GiB for its pods plus the
cluster. It uses one new namespace, two sample application pods, real Kafka/RabbitMQ,
a Python client pod and ClusterIP Services. It creates no ingress, NodePort, PVC, RBAC,
cluster-wide resource or addon. Broker addresses use allocated ClusterIPs, so the
fixture does not require or enable the DNS addon. Only the Kafka bootstrap Service uses
`publishNotReadyAddresses=true` to avoid a metadata/readiness bootstrap cycle; the
application Service under test uses the default false.

```sh
# If stopped, run this yourself when sudo asks for your password:
sudo microk8s start
microk8s status

python3 benchmark/kubernetes/run.py --label my-readiness-run --versions 3.5.9 4.0.2
```

`--start` can start a stopped cluster if your current account already has permission;
the runner never collects a sudo password. It leaves MicroK8s running and does not alter
other namespaces. `--keep` retains its test namespace for troubleshooting; otherwise
only the namespace it successfully created is deleted, including on failure. An existing
namespace is never reused. Public images remain cached in containerd for later runs.

The runner builds the starter once against 3.5.9 and builds the sample with each selected
BOM. Kubernetes pulls public images with `imagePullPolicy: IfNotPresent`. The runner
uploads each executable JAR with `kubectl cp` into an emptyDir mounted in an unprivileged
Java 17 pod, verifies its SHA-256, then starts Java. No sudo containerd import is needed.
The nested starter SHA-256 is also recorded to verify both BOMs consume the same binary.
The optional Dockerfile is supplied for users who prefer packaging their own image.

## Checks

1. Both ready application pods receive new connections through the real ClusterIP.
2. Maintenance on sample-a gives readiness 503 while liveness remains 200.
3. Kubelet sets Pod Ready=False; EndpointSlice eligible endpoints contain only sample-b.
   The unready address may remain listed with `ready=false`, which is correct.
4. New Service connections go only to sample-b. Direct sample-a Pod-IP `/work` stays 200.
5. An injected exit-hook failure does not restore readiness or Service eligibility.
6. Successful exit restores both pods to Service routing.
7. With both pods in maintenance, no ready endpoints remain and new Service requests
   fail/time out. **No particular HTTP status is required**: kube-proxy is not an HTTP server.
8. An in-flight request and a subsequent request on its established keep-alive connection
   can complete against the unready pod. This explicitly demonstrates the boundary of
   readiness-based routing rather than treating continued connection traffic as a bug.
9. Two pods share a one-partition Kafka group, exercising an unassigned consumer as
   well as the assigned consumer during pause/resume.
10. Pod UID, Running phase and container restart count stay unchanged across the window.
11. Recovering first one and then both pods restores the corresponding Service routes.

All business requests originate **inside the client pod**. Service tests use ClusterIP
and a fresh connection per request, except the explicitly named keep-alive test.
`kubectl port-forward service/...` is deliberately not used because it bypasses routing.
Control-plane operations use `microk8s kubectl`; management calls use direct Pod-IP
from the client so maintenance exit remains reachable while Service traffic is excluded.

Reports under `benchmark/reports/<label>/` contain JSON/Markdown results, cluster version,
JAR hashes, Pod/EndpointSlice snapshots and local command logs. Propagation times are
observed upper bounds from command start, not a kube-proxy or ingress convergence SLA.
Zero executed checks with `completed=false` is a blocked/failed run, never a pass.

This is a single-node ClusterIP test, not ingress/controller, multi-node, HTTP/2, gRPC,
network partition or exactly-once broker certification. It does not imply that all
traffic everywhere ceases the instant readiness changes. Services configured with
`publishNotReadyAddresses=true` and consumers bypassing Services have different behavior.

References: [Kubernetes EndpointSlices](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/),
[Kubernetes probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/).

## Management access acceptance

After proving Service exclusion, the client explicitly reads GET /actuator/maintenance
at the unready pod's IP. Before every successful exit it repeats that check while the
pod is absent from the ready endpoint set, then posts enabled=false. Exit must return
HTTP 200, active=false, phase=INACTIVE and partialFailure=false. An independent GET
must confirm INACTIVE and readiness must return 200; EndpointSlice eligibility and
fresh Service routing must then recover. This includes recovery when all pods are unready.

Management uses direct Pod-IP from the in-cluster client, not the ordinary application
Service. This verifies the library endpoint remains available; it does not provision an
external management route. Restart persistence, rollout orchestration and deployment
security are outside this library acceptance scope.

## Startup maintenance acceptance (1.3.0)

`startup.py` adds real startup checks alongside the normal `run.py` maintenance-regression
suite. Install/resolve the published starter and pin its nested binary SHA-256:

```sh
python3 benchmark/kubernetes/startup.py --label startup-130-local \
  --versions 3.5.9 4.0.2 --java-home /path/to/jdk17 --skip-build-starter \
  --expected-starter-sha256 bc5ffa4e40b304135087540e9693586d543c123d53e32a00e4604249e90b707d
```

Each case creates a new sample pod/group, seeds eight messages in each real broker before
boot, and records management state, broker observations, scheduler gating and probe status.
The four startup property/env aliases, a bounded earlier ApplicationStarted listener,
an injected queue stop error, an Actuator exit during a bounded startup stop barrier, and
a CLI false overriding env true are covered. Service checks use the in-cluster client.
Seeded Rabbit messages use the local fixture's management API; Kafka uses its broker CLI.

The sample's optional `benchmark.startup-probe=true` diagnostics observe the library's
MaintenanceState and consumer calls. Scheduled work checks isWorkBlocked() from its first
tick. No application HTTP admission filter is introduced. The 2-second earlier event
listener and 8-second queue stop barrier are explicit, bounded fault/timing fixtures.
They do not replace the ordinary, undelayed startup cases.

A recovered pod must resume broker processing, return readiness 200 and rejoin the real
Service without replacement. A race leaving consumers stopped after successful exit is
recorded as failure and the case is cleaned up rather than silently repaired. Assertions
continue across behavioral failures; setup/execution errors stop the run and mark it incomplete.
All failures, seeded tokens and snapshots are retained; a completed run with failed checks
is not a pass. No benchmark is added to GitHub Actions.

### Validating initial-start deferral

`--starter-version` selects a locally installed candidate without changing the root POM
or replacing a published artifact. Build the candidate from a temporary copy of the
source tree with an isolated SNAPSHOT version; install it locally and record its hash.
For the startup-consumption fix:

```sh
python3 benchmark/kubernetes/startup.py --label startup-fix-repeat \
  --starter-version 1.3.1-startup-fix-SNAPSHOT --skip-build-starter \
  --expected-starter-sha256 YOUR_CANDIDATE_SHA256 --expect-deferred-startup \
  --cases flat nested env-flat env-nested cold-window \
  --versions 2.7.18 3.5.9 4.0.2 --java-home /path/to/jdk17
```

This explicitly selected matrix covers the startup-consumption fix. It does not hide or
reclassify the separate early-exit, property-override or queue-failure review cases; those
remain available in the default full review suite. `--expect-deferred-startup` expects
both brokers to be stopped before the first exit rather than Kafka already running and
paused. It also checks delivery of every seeded message after exit, preservation of
user-disabled listeners and a second dynamic maintenance cycle using Kafka pause/resume.

Run the normal two-pod routing regression separately, with the same `--starter-version`,
`--skip-build-starter` and `--expected-starter-sha256` arguments. Do not run E2E runners
concurrently: they share the sample build output. Results and artifact identity are in
the [startup consumption fix report](../reports/STARTUP_CONSUMPTION_FIX.md).
