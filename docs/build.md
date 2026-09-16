# Build

One `Makefile` for Linux, macOS, WSL, and Windows (GNU Make). Docker and Podman are both first-class: `make docker-build` / `make podman-build`. `make image` uses Podman if it is on `PATH`, otherwise Docker.

```bash
make help
make build              # bin/mocache  (bin/mocache.exe on Windows)
make test
make docker-build       # docker build -t mocache:0.1.0
make podman-build       # podman build -t mocache:0.1.0
make image              # auto-detect engine
make docker-run         # publish 8090/8091
make podman-run
make CONTAINER_ENGINE=docker image
```

On Windows, install GNU Make (Git for Windows, Chocolatey `make`, Scoop, or MSYS2) and run the same targets.

Image: multi-stage Dockerfile (`golang:1.25-alpine` → `scratch`), `CGO_ENABLED=0`, ports 8090/8091, user 65532. Tag default `mocache:0.1.0` (override `IMAGE=`).
