# microk8s-readiness-v3

Contract: Kubernetes owns traffic routing; the library only signals readiness.

| Version | Check | Result |
|---|---|---|
| 3.5.9 | sample-a-jar-integrity | PASS |
| 3.5.9 | sample-b-jar-integrity | PASS |
| 3.5.9 | service-reaches-both-ready-pods | PASS |
| 3.5.9 | sample-a-enter | FAIL |
| 3.5.9 | sample-a-endpointslice-ready-set | PASS |
| 3.5.9 | kubelet-observes-not-ready | PASS |
| 3.5.9 | readiness-503-liveness-200 | PASS |
| 3.5.9 | unready-pod-direct-http-still-200 | PASS |
| 3.5.9 | service-excludes-maintenance-pod | PASS |

Run error: 'body'
