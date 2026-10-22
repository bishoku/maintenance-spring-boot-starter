# Releasing

Releases use an existing immutable `vX.Y.Z` Git tag. The root POM version must match
that tag exactly. Only maintainers with publishing access should perform these steps.

1. Update the root POM version, README coordinates and sample's `maintenance.version`.
   Benchmark runners derive the starter version from the root POM automatically.
2. Add `release-notes/vX.Y.Z.md`, including compatibility/upgrade notes and validation,
   and link it from CHANGELOG.md. Review all intended changes before committing.
3. Run `mvn --batch-mode clean verify` and the Boot compatibility checks described in
   CONTRIBUTING.md. Run relevant benchmarks locally; they do not belong in GitHub Actions.
4. Push the reviewed commit to `main` and wait for CI to pass.
5. Create an annotated tag at that exact commit and push it:

   ```sh
   git tag -a vX.Y.Z COMMIT_SHA -m 'Release vX.Y.Z'
   git push origin vX.Y.Z
   ```

The release workflow checks out the tag, validates version/notes, runs source tests,
creates main/source/Javadoc JARs and POM signatures, and publishes to Maven Central.
It waits for Central's **published** state before transferring the same artifacts to
the separate GitHub release job. That job verifies checksums, uploads all assets to a
draft, then publishes it with the reviewed notes. No benchmark infrastructure starts.

The repository needs `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`,
`MAVEN_GPG_PRIVATE_KEY` and `MAVEN_GPG_PASSPHRASE` Actions secrets. The scoped workflow
`GITHUB_TOKEN` creates the GitHub release; no additional PAT is required.

## Verification and recovery

Confirm the Maven Central coordinates, release tag/commit, notes, main/source/Javadoc
JARs, POM, four `.asc` signatures and `SHA256SUMS`. Download the assets and run
`sha256sum --check SHA256SUMS` in their directory.

If only the GitHub release job fails, use **Re-run failed jobs** for the same workflow
run. Its `release-X.Y.Z` artifact is retained for 30 days, so recovery does not redeploy
to Central. Existing draft assets can be retried; published release assets are not replaced.

Manual workflow dispatch takes a version without `v` and still checks out the existing
tag, not the selected branch's working state. Use it only when Central publication has
not already succeeded. Never move a published tag or attempt to overwrite a Central
version. If Central reports publication success but a later artifact-collection step fails,
recover the published artifacts from Central and complete the GitHub release separately.
Do not rerun the Central publish step blindly after an ambiguous network failure.
