FROM golang:1.25-alpine
WORKDIR /app
COPY go/go.mod ./go.mod
COPY go/main.go ./main.go
# go.sum is resolved at build time; this module exists only for the lab.
RUN go mod tidy && go build -o /usr/local/bin/compat .
ENTRYPOINT ["/usr/local/bin/compat"]
