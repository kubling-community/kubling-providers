# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.25.0-alpine AS build

WORKDIR /workspace

COPY sdk-go/go.mod sdk-go/go.sum ./sdk-go/
COPY providers/kubernetes/go.mod providers/kubernetes/go.sum ./providers/kubernetes/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd providers/kubernetes && go mod download

COPY sdk-go ./sdk-go
COPY providers/kubernetes ./providers/kubernetes

ARG TARGETOS=linux
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd providers/kubernetes && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -ldflags="-s -w" -o /out/kubling-provider-kubernetes ./cmd/kubernetes

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Kubling Kubernetes Provider" \
      org.opencontainers.image.description="Kubernetes provider for Kubling" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kubling-provider-kubernetes /usr/local/bin/kubling-provider-kubernetes
COPY LICENSE /licenses/LICENSE

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kubling-provider-kubernetes"]
CMD ["-listen", ":50051"]
