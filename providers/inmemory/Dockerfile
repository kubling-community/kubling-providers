# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/inmemory/go.mod providers/inmemory/go.sum ./providers/inmemory/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/inmemory && go mod download

COPY sdk-go ./sdk-go
COPY providers/inmemory ./providers/inmemory

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/inmemory && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" -o /out/kubling-provider-inmemory ./cmd/inmemory

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling In-memory Provider" \
      org.opencontainers.image.description="In-memory reference provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-inmemory /usr/local/bin/kubling-provider-inmemory
COPY LICENSE /licenses/LICENSE

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-inmemory"]
CMD ["-listen", ":50051"]
