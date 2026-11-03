# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/bacnet/go.mod providers/bacnet/go.sum ./providers/bacnet/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/bacnet && go mod download

COPY sdk-go ./sdk-go
COPY providers/bacnet ./providers/bacnet

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/bacnet && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" \
    -o /out/kubling-provider-bacnet ./cmd/kubling-provider-bacnet

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling BACnet Provider" \
      org.opencontainers.image.description="BACnet/IP provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-bacnet /usr/local/bin/kubling-provider-bacnet
COPY LICENSE /licenses/LICENSE

ENV KUBLING_BACNET_CONFIG=/etc/kubling/provider.yaml

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-bacnet"]
