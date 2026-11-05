# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM bufbuild/buf:1.59.0 AS buf

FROM --platform=$BUILDPLATFORM maven:3.9.11-eclipse-temurin-21 AS build

COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf

WORKDIR /workspace

COPY buf.yaml buf.lock ./
COPY proto ./proto
COPY sdk-java/buf.gen.yaml sdk-java/pom.xml ./sdk-java/

RUN buf generate --template sdk-java/buf.gen.yaml

COPY LICENSE ./LICENSE

RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --file sdk-java/pom.xml \
      -Dexec.skip=true -DskipTests install

COPY providers/oracle/pom.xml ./providers/oracle/pom.xml

RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --file providers/oracle/pom.xml \
      -DskipTests dependency:go-offline

COPY providers/oracle/src ./providers/oracle/src

RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --file providers/oracle/pom.xml \
      -DskipTests package

FROM gcr.io/distroless/java21-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling Oracle Provider" \
      org.opencontainers.image.description="Oracle Database provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0 AND LicenseRef-Oracle-FDHUT-1.0"

WORKDIR /app

COPY --from=build --chown=nonroot:nonroot \
    /workspace/providers/oracle/target/kubling-oracle-provider.jar \
    /app/kubling-oracle-provider.jar
COPY --from=build --chown=nonroot:nonroot \
    /workspace/providers/oracle/target/lib \
    /app/lib
COPY LICENSE /licenses/LICENSE
COPY providers/oracle/THIRD_PARTY_NOTICES.md /licenses/THIRD_PARTY_NOTICES.md

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["java", "-jar", "/app/kubling-oracle-provider.jar"]
