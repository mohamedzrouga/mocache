# Linux, macOS, WSL, and Windows (GNU Make: Git for Windows / MSYS2 / Chocolatey).
#
#   make help
#   make build
#   make test
#   make docker-build
#   make podman-build
#   make image              # podman if installed, else docker
#
# Override engine:  make image CONTAINER_ENGINE=docker

GO          ?= go
GOFLAGS     ?= -trimpath
LDFLAGS     ?= -s -w
CGO_ENABLED ?= 0

BIN_DIR ?= bin
IMAGE   ?= mocache:0.1.0

ifeq ($(OS),Windows_NT)
BIN ?= $(BIN_DIR)/mocache.exe
else
BIN ?= $(BIN_DIR)/mocache
endif

# Prefer Podman when both are installed (rootless, daemonless).
CONTAINER_ENGINE ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)

.PHONY: help build build-linux test test-python clean \
	image docker-build podman-build docker-run podman-run run

help:
	@echo "MoCache"
	@echo "  make build          native binary -> $(BIN)"
	@echo "  make build-linux    linux/amd64 binary (for copying into a FROM scratch image)"
	@echo "  make test           go test ./..."
	@echo "  make docker-build   docker build -t $(IMAGE)"
	@echo "  make podman-build   podman build -t $(IMAGE)"
	@echo "  make image          $(CONTAINER_ENGINE) build (override CONTAINER_ENGINE=)"
	@echo "  make docker-run     build with docker and run :8090/:8091"
	@echo "  make podman-run     build with podman and run :8090/:8091"
	@echo "  make run            run the native binary"
	@echo "  make clean"

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

build: $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN) ./cmd/mocache

# Static linux binary; same flags the Dockerfile uses.
build-linux: $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/mocache-linux-amd64 ./cmd/mocache

test:
	$(GO) test ./...

test-python:
	PYTHONPATH=sdk/python python3 -m unittest discover -s sdk/python/tests -v

clean:
	rm -rf $(BIN_DIR)

image:
	$(CONTAINER_ENGINE) build -t $(IMAGE) -f Dockerfile .

docker-build:
	docker build -t $(IMAGE) -f Dockerfile .

podman-build:
	podman build -t $(IMAGE) -f Dockerfile .

docker-run: docker-build
	docker run --rm -p 8090:8090 -p 8091:8091 $(IMAGE)

podman-run: podman-build
	podman run --rm -p 8090:8090 -p 8091:8091 $(IMAGE)

run: build
	$(BIN) -http :8090 -rpc :8091
