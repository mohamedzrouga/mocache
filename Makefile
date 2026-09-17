# Linux, macOS, WSL, and Windows (GNU Make: Git for Windows / MSYS2 / Chocolatey).
#
#   make help
#   make build
#   make test
#   make docker-build
#   make podman-build
#   make image              # podman if installed, else docker
#   make lab-up             # MinIO + FastAPI + 3 cache nodes (lab/)
#
# Override engine:  make image CONTAINER_ENGINE=docker
# Override compose: make lab-up COMPOSE="podman compose"

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

# Compose is used for the local 3-node stack and the lab; Docker Compose v2 syntax.
COMPOSE    ?= docker compose
LAB        ?= lab
BENCH_ARGS ?= --compare

.PHONY: help build build-linux test test-python clean \
	image docker-build podman-build docker-run podman-run run \
	compose-up compose-down \
	lab-up lab-seed lab-bench lab-sweep lab-stats lab-logs lab-down lab-clean

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
	@echo ""
	@echo "Local stacks (docker compose)"
	@echo "  make compose-up     3 cache nodes on :8090/:8092/:8094"
	@echo "  make compose-down"
	@echo "  make lab-up         lab/: MinIO + FastAPI + 3 cache nodes"
	@echo "  make lab-seed       upload random objects to MinIO"
	@echo "  make lab-bench      origin-only vs cache-aside benchmark (BENCH_ARGS=)"
	@echo "  make lab-sweep      concurrency sweep 1,8,64,256"
	@echo "  make lab-stats      API counters + per-node /metrics"
	@echo "  make lab-logs       follow lab logs"
	@echo "  make lab-down       stop the lab (lab-clean also drops the MinIO volume)"

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

# --- local stacks -----------------------------------------------------------

compose-up:
	$(COMPOSE) up -d --build

compose-down:
	$(COMPOSE) down

lab-up:
	$(COMPOSE) -f $(LAB)/docker-compose.yml up -d --build

lab-seed:
	$(COMPOSE) -f $(LAB)/docker-compose.yml run --rm seed

# make lab-bench BENCH_ARGS="--duration 60 --concurrency 128 --json /results/run.json"
lab-bench:
	$(COMPOSE) -f $(LAB)/docker-compose.yml run --rm bench $(BENCH_ARGS)

lab-sweep:
	$(COMPOSE) -f $(LAB)/docker-compose.yml run --rm bench --concurrency 1,8,64,256

lab-stats:
	@curl -sS http://127.0.0.1:8000/stats; echo
	@for p in 18090 18092 18094; do \
		echo "--- cache on :$$p ---"; \
		curl -sS http://127.0.0.1:$$p/metrics | grep -E '^mocache_(hits|misses|evictions|items|bytes)' ; \
	done

lab-logs:
	$(COMPOSE) -f $(LAB)/docker-compose.yml logs -f --tail 50

lab-down:
	$(COMPOSE) -f $(LAB)/docker-compose.yml down

# Also removes the MinIO volume: the next lab-seed re-uploads from scratch.
lab-clean:
	$(COMPOSE) -f $(LAB)/docker-compose.yml down -v
