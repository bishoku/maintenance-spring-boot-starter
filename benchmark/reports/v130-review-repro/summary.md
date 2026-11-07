# 1.3.0 deterministic review reproducers

The acceptance assertions intentionally fail against the published binary. These are product-behavior reproductions, not compilation errors.

| Boot | Tests | Failed assertions | Errors |
|---|---|---|---|
| 3.5.9 | 3 | 3 | 0 |
| 4.0.2 | 3 | 3 | 0 |

Run with `mvn -f benchmark/review/pom.xml -Dspring-boot.version=VERSION test`. Source CI does not include this standalone project.
