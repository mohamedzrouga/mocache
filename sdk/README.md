# SDK layout

- `sdk/go` — Go client, sync + context + channel-async (`import "github.com/mohamedzrouga/mocache/sdk/go"`)
- `sdk/python` — `MoCacheClient` (sync) and `AsyncMoCacheClient` (asyncio)
- `sdk/examples/go` — sync, context, GetAsync, invalidate
- `sdk/examples/python/sync.py`
- `sdk/examples/python/asyncio_example.py`
- `sdk/examples/python/fastapi_app.py` — singleton + `app.state`

See [docs/sdk.md](../docs/sdk.md).
