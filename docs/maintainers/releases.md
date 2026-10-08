# Maintainer release process

The provider protocol and its Go, Java and Python bindings share the version in
the repository `VERSION` file. A release creates four tags at the same commit:

- `proto/vMAJOR.MINOR.PATCH`
- `sdk-go/vMAJOR.MINOR.PATCH`
- `sdk-java/vMAJOR.MINOR.PATCH`
- `sdk-python/vMAJOR.MINOR.PATCH`

Provider runtimes and containers have independent versions because their
source-specific behavior can evolve without changing the contract.

Run the **Provider contract release train** workflow with `publish=false` before
publishing. After the dry run succeeds and the release commit is on `main`, run
it again with `publish=true`. The workflow publishes the protocol to the Buf
Schema Registry, creates the coordinated tags, and publishes the Java and
Python packages to Maven Central and PyPI.

Java releases use `com.kubling:kubling-provider-grpc`. Python releases use
`kubling-provider-grpc`. Both contain provider bindings only and depend on the
shared types from `kubling-grpc`; imported shared classes and modules must not be
repackaged.

Publication credentials are maintained as GitHub Actions secrets. The release
workflow is the source of truth for required configuration. Language-specific
workflows may retry one registry publication from an existing coordinated tag
after a partial release.

Run the **Java signing check** before the first Maven Central publication and
after rotating the signing key. It signs and verifies every Java artifact
without uploading it.
