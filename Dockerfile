FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mocache ./cmd/mocache

FROM scratch
COPY --from=build /out/mocache /mocache
USER 65532:65532
EXPOSE 8090 8091
ENTRYPOINT ["/mocache"]
