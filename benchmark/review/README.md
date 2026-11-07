# Published 1.3.0 review reproductions

This standalone Maven consumer runs acceptance assertions against the published starter,
not a modified library source tree. The root source CI does not include this project.

```sh
mvn -B -f benchmark/review/pom.xml -Dspring-boot.version=3.5.9 test
mvn -B -f benchmark/review/pom.xml -Dspring-boot.version=4.0.2 test
```

Against 1.3.0, the three tests intentionally **fail assertions** (not compilation or
startup errors). They expose a hidden startup queue failure, an early Actuator exit racing
with startup suspension, and an explicit false being overridden by a lower-priority env true.
The tests assert the desired contract so they can verify a future fix without weakening assertions.
Use a new version/isolated artifact when testing fixes; do not overwrite the published 1.3.0 artifact.

`benchmark/reports/v130-review-repro/` records facts and totals for both Boot versions.
The real-broker cold-start consumption window is separately covered by the MicroK8s
startup suite described in `benchmark/kubernetes/README.md`.
