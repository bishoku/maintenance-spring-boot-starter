> Historical scope: HTTP admission/drain assumptions in this report were superseded by [the readiness contract correction](READINESS_CONTRACT.md). Raw outcomes are retained as history, not current acceptance criteria.

# Validation plan

The baseline is captured before changing library sources. Tests assert the desired
contract, so baseline failures are expected and are retained. The same contracts are
rerun after changes; build/environment failures are reported separately from behavior.

Implementation order:
1. Boot 3.5 baseline and Boot 4-compatible actuator API / optional parameter mapping.
2. Explicit transition phases, fail-closed critical steps, retries and error reporting.
3. Cooperative hook cancellation, stop subsequent hooks on failure, and reject a new
   transition while a timed-out hook is still executing.
4. Kafka pause acknowledgement and ownership-aware resume; Rabbit callback stop and
   restore only containers stopped by maintenance.
5. Servlet admission gate with tracked in-flight requests and bounded drain. Generic
   work-tracking API for application-owned jobs; no claim of automatic scheduler control.
6. Updated unit contracts and repeated real-broker E2E runs, without CI modifications.

Scope decisions: this remains a pod-local starter. Distributed maintenance orchestration,
restart persistence, Kubernetes networking and reactive/streaming traffic cannot be
made safe by a local boolean. The documentation must state those limits explicitly.
