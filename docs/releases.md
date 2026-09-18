# Contract release train

The provider protocol and its Go, Java and Python bindings share the version in
the repository `VERSION` file. A release creates four tags at the same commit:

- `proto/vMAJOR.MINOR.PATCH`
- `sdk-go/vMAJOR.MINOR.PATCH`
- `sdk-java/vMAJOR.MINOR.PATCH`
- `sdk-python/vMAJOR.MINOR.PATCH`

Provider runtime and container versions remain independent because their
source-specific behavior can evolve without changing the provider contract.

Use the **Provider contract release train** workflow with `publish=false` to
run the complete release validation without changing any registry or tag. Set
`publish=true` only after that dry run succeeds and the release commit is on
`main`. The publishing path validates the protocol and all language packages,
then publishes the BSR label and coordinated tags before Maven Central and
PyPI.

Java releases use `com.kubling:kubling-provider-grpc`. Python releases use
`kubling-provider-grpc`. Both packages contain provider bindings only and
depend on the shared types from `kubling-grpc`; imported shared classes or
modules must never be repackaged.

## Registry configuration

Maven Central uses `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`,
`MAVEN_GPG_PRIVATE_KEY` and `MAVEN_GPG_PASSPHRASE`. PyPI uses
`PYPI_API_TOKEN` in the `pypi` environment. The language-specific workflows
can retry publication of an existing coordinated tag after a partial release.

Run the **Java signing check** before the first Central publication or after
rotating signing credentials. It signs and verifies every Java artifact
without uploading anything.

## Local Java consumption

Build and install the Java binding before testing a dbvirt change:

```sh
mvn --batch-mode --no-transfer-progress -f sdk-java/pom.xml clean install
```

Maven writes the JAR and POM to the local repository. dbvirt can depend on the
coordinates declared by `sdk-java/pom.xml` and resolve that local build before
the artifact exists in Central. This keeps provider code generation owned by
this repository and avoids copying imported protocol sources into dbvirt.
