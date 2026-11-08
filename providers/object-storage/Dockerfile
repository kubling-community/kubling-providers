# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/object-storage/go.mod providers/object-storage/go.sum ./providers/object-storage/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/object-storage && go mod download

COPY sdk-go ./sdk-go
COPY providers/object-storage ./providers/object-storage

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/object-storage && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" \
    -o /out/kubling-provider-object-storage ./cmd/kubling-provider-object-storage

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling Object Storage Provider" \
      org.opencontainers.image.description="Object storage provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-object-storage /usr/local/bin/kubling-provider-object-storage
COPY LICENSE /licenses/LICENSE

ENV KUBLING_OBJECT_STORAGE_CONFIG=/etc/kubling/provider.yaml

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-object-storage"]
