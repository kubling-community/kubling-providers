# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/openapi/go.mod providers/openapi/go.sum ./providers/openapi/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/openapi && go mod download

COPY sdk-go ./sdk-go
COPY providers/openapi ./providers/openapi

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/openapi && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" -o /out/kubling-provider-openapi ./cmd/openapi

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling OpenAPI Provider" \
      org.opencontainers.image.description="Configurable OpenAPI provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-openapi /usr/local/bin/kubling-provider-openapi
COPY LICENSE /licenses/LICENSE

ENV KUBLING_OPENAPI_CONFIG=/etc/kubling/provider.yaml

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-openapi"]
CMD ["-listen", ":50051"]
