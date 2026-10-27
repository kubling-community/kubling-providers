# Maintainer release process

The provider protocol and its Go, Java and Python bindings share the version in
the repository `VERSION` file when the contract changes. A coordinated release
creates four tags at the same commit:

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

## Go SDK maintenance releases

The Go SDK may receive an independent patch when the provider protocol is
unchanged. Its release version is stored in `sdk-go/VERSION`. The maintenance
version must keep the contract's major and minor numbers and use a later patch
number.

Before tagging the SDK, update every provider module to require that SDK
version. The Go SDK workflow verifies that the protocol sources and dependency
lock still match the coordinated protocol tag, regenerates the bindings, and
tests the module. Push `sdk-go/vMAJOR.MINOR.PATCH` only after the change is on
`main`.

The next coordinated contract release must set `sdk-go/VERSION` back to the
same version as `VERSION`. Java and Python bindings remain part of the
coordinated release train.

Run the **Java signing check** before the first Maven Central publication and
after rotating the signing key. It signs and verifies every Java artifact
without uploading it.
