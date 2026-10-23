# boot27-compose-jdk21

2026-10-23T09:30:45.220388+00:00

| Version | Contract | Result |
|---|---|---|
| 2.7.18 | sample-build | PASS |
| 2.7.18 | starter-binary-identity | PASS |
| 2.7.18 | kafka-paused-without-stop | PASS |
| 2.7.18 | rabbit-stopped | PASS |
| 2.7.18 | readiness-refuses-liveness-alive | PASS |
| 2.7.18 | no-consumption-during-maintenance | PASS |
| 2.7.18 | direct-http-unfiltered-during-maintenance | PASS |
| 2.7.18 | queued-messages-consumed-after-exit | PASS |
| 2.7.18 | idle-consumers-preserved | PASS |
| 2.7.18 | exit-hooks-before-resume | PASS |
| 2.7.18 | optional-reason | PASS |
| 2.7.18 | failed-entry-is-not-active | PASS |
| 2.7.18 | failed-entry-retains-readiness-refusal | PASS |
| 2.7.18 | failed-entry-retry | PASS |
| 2.7.18 | failed-exit-retains-readiness-refusal | PASS |
| 2.7.18 | failed-exit-consumers-remain-paused | PASS |
| 2.7.18 | failed-exit-retry | PASS |
| 2.7.18 | timed-out-hook-cancelled | PASS |
| 2.7.18 | timeout-is-failed-transition | PASS |
| 2.7.18 | broker-failure-metric | PASS |
| 2.7.18 | broker-failure-propagated | PASS |
| 2.7.18 | configured-propagation-grace | PASS |
| 2.7.18 | no-library-http-drain-barrier | PASS |
| 2.7.18 | inflight-request-not-aborted | PASS |
| 2.7.18 | concurrent-entry-idempotent | PASS |
| 2.7.18 | disabled-endpoint-hidden | PASS |
| 2.7.18 | readiness-disabled-keeps-probe-up | PASS |
| 2.7.18 | readiness-disabled-skips-grace | PASS |
| 2.7.18 | inflight-broker-handlers-finish | PASS |
| 2.7.18 | broker-drain-completed | PASS |

Excluded scenarios:
- 2.7.18 / readonly: Boot 2.x has no Actuator access mode; excluded by compatibility contract
