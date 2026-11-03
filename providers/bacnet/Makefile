GO ?= go
BINARY ?= bin/kubling-provider-bacnet
IMAGE ?= kubling-provider-bacnet:local
PACKAGE := ./cmd/kubling-provider-bacnet

.PHONY: build build-check check container-build fmt fmt-check mod-check run test test-race vet

build:
	mkdir -p $(dir $(BINARY))
	$(GO) build -trimpath -o $(BINARY) $(PACKAGE)

build-check:
	$(GO) build ./...

container-build:
	docker build --tag $(IMAGE) --file Dockerfile ../..

run:
	$(GO) run $(PACKAGE) -config config.example.yaml

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

mod-check:
	$(GO) mod tidy -diff

check: mod-check fmt-check vet test-race build-check
