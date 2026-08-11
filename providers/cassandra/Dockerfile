# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.25.0-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/cassandra/go.mod providers/cassandra/go.sum ./providers/cassandra/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/cassandra && go mod download

COPY sdk-go ./sdk-go
COPY providers/cassandra ./providers/cassandra

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/cassandra && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" -o /out/kubling-provider-cassandra ./cmd/cassandra

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling Cassandra Provider" \
      org.opencontainers.image.description="Apache Cassandra provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-cassandra /usr/local/bin/kubling-provider-cassandra
COPY LICENSE /licenses/LICENSE

ENV KUBLING_CASSANDRA_CONFIG=/etc/kubling/provider.yaml

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-cassandra"]
CMD ["-listen", ":50051"]
