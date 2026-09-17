# Kubling provider gRPC Java bindings

Generated messages and gRPC stubs for the Kubling provider protocol. This
artifact contains provider bindings only.

Coordinates: `com.kubling:kubling-provider-grpc`.

Shared value types come from `com.kubling:kubling-grpc`; they are not
repackaged here.

Build and install into the local Maven repository from the project root:

```sh
mvn --batch-mode --no-transfer-progress -f sdk-java/pom.xml clean install
```

Consumers such as dbvirt can then declare the artifact as a normal dependency;
they do not need copied protos or local code generation.
