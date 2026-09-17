// Separate module: go-redis must not appear in the MoCache module's go.mod.
// Nested modules are excluded from ./... so `go build ./...` and `go test ./...`
// at the repo root never see this.
module mocache-compat

go 1.25.0

require github.com/redis/go-redis/v9 v9.7.3
