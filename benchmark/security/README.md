# Minimal consumer dependency audit

Run with Python 3, Maven, Java 17+, and network access to Maven repositories and OSV.
The artifact must already be installed locally or available from a repository.
Use a new label for each run; existing evidence is never overwritten.

```sh
python3 benchmark/security/audit-consumer.py --label security-default-new \
  --starter-version 1.3.2-security-SNAPSHOT --java-home /path/to/jdk17 --scan
python3 benchmark/security/audit-consumer.py --label security-boot27-new \
  --starter-version 1.3.2-security-SNAPSHOT --boot-version 2.7.18 \
  --java-home /path/to/jdk17
```

`1.3.2-security-SNAPSHOT` is a local validation candidate, not a published release.
For modified sources, copy the root POM and `src` into a temporary directory, change
only that copy's project version, and run `mvn clean install` there. Never install
modified sources over an existing published artifact version.

The script builds a separate minimal Maven consumer containing only this starter,
optionally imports an application Boot BOM, and saves the consumer POM, resolved
runtime tree and JSON results under `benchmark/reports/<label>`. It checks that
optional Kafka/Rabbit dependencies are absent and that a selected consumer Boot BOM
is respected. This avoids incorrect conclusions from filtering the library's own
compile/test graph, where optional direct dependencies can hide mandatory transitive
paths through dependency mediation.

`--scan` queries OSV for each resolved Maven component and saves full advisory data.
Exit code 1 means findings or a failed/incomplete run; inspect `completed`, `error`
and `advisory_ids`. A successful resolution without `--scan` is not a security scan.
Network/API errors cannot produce a clean security result.

Zero matches means no non-withdrawn advisories returned by OSV for that graph at
that time, not proof that all vulnerabilities are absent. Advisory databases can lag
publisher notices. Version matches also do not establish exploitability in this
library. Review publisher notices and scan the actual application graph as well.
Optional dependencies remain subject to build/integration review; do not suppress
them globally. These audits and E2E runners are local; GitHub Actions is unchanged.
