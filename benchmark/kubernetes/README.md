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
