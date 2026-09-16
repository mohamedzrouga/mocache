# Unary RPC protocol (`grpc` transport)

This is **not** `google.golang.org/grpc`. The SRS forbids third-party runtimes; the SDKs speak a small length-prefixed binary protocol over persistent TCP. Client option names: Go `ProtocolGRPC`, Python `protocol="grpc"`.

Default listen address: `:8091`.

## Frame

```
uint32be length     # payload size only; 1 .. 4 MiB
payload
```

Oversized frames are rejected so a client cannot OOM a node.

## Request payload

| Offset | Type | Field |
|---|---|---|
| 0 | 4 bytes | magic `MOC1` |
| 4 | u8 | version `1` |
| 5 | u8 | op: Get=1, Set=2, Delete=3, Health=4, Invalidate=5 |
| 6 | u32be | request id (echoed) |
| 10 | u16be | key length |
| 12 | bytes | key |
| 12+k | u32be | TTL seconds (0 = no expiry) |
| 16+k | u32be | value length |
| 20+k | bytes | value |

## Response payload

| Offset | Type | Field |
|---|---|---|
| 0 | 4 bytes | magic `MOC1` |
| 4 | u8 | version `1` |
| 5 | u8 | status: OK=0, Miss=1, Error=2 |
| 6 | u32be | request id |
| 10 | u32be | value length |
| 14 | bytes | value |
| 14+v | u16be | error length |
| 16+v | bytes | error message |

The protocol is **not multiplexed**: one request at a time per TCP connection. Clients hold a mutex per node. Idle connections are deadline'd at 60s on the server.

Invalidate (op=5): `key` is the prefix or regex; `value` is the ASCII kind `prefix` or `regex`. The response `value` is the decimal deleted count. Clients **broadcast** this op to every node.

On node restart the TCP session dies. SDKs retry **once** after reconnect so a rollout looks like a miss, not a hard error, when the new process is already listening.
