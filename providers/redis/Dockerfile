# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.25.0-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/redis/go.mod providers/redis/go.sum ./providers/redis/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/redis && go mod download

COPY sdk-go ./sdk-go
COPY providers/redis ./providers/redis

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/redis && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" -o /out/kubling-provider-redis ./cmd/redis

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling Redis Provider" \
      org.opencontainers.image.description="Redis provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-redis /usr/local/bin/kubling-provider-redis
COPY LICENSE /licenses/LICENSE

ENV KUBLING_REDIS_CONFIG=/etc/kubling/provider.yaml

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-redis"]
CMD ["-listen", ":50051"]
