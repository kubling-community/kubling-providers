# syntax=docker/dockerfile:1.7

FROM golang:1.24.8-alpine AS build

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z

RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOBIN=/out go install github.com/minio/minio@${MINIO_VERSION}

FROM alpine:3.22.2

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 10001 minio && \
    mkdir -p /data && \
    chown minio:minio /data

COPY --from=build /out/minio /usr/local/bin/minio

USER minio:minio

EXPOSE 9000 9001

ENTRYPOINT ["/usr/local/bin/minio"]
