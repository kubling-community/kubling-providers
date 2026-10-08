# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/host/go.mod providers/host/go.sum ./providers/host/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/host && go mod download

COPY sdk-go ./sdk-go
COPY providers/host ./providers/host

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/host && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" -o /out/kubling-host-provider ./cmd/kubling-host-provider

RUN mkdir -p /out/state

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling Host Provider" \
      org.opencontainers.image.description="Dynamic host-fleet provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-host-provider /usr/local/bin/kubling-host-provider
COPY --from=build --chown=nonroot:nonroot /out/state /var/lib/kubling/host-provider
COPY LICENSE /licenses/LICENSE

ENV KUBLING_HOST_STATE_DIRECTORY=/var/lib/kubling/host-provider

EXPOSE 50051 50052

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-host-provider"]
