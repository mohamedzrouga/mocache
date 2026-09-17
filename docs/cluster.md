# Cluster

MoCache uses **Redis Cluster's key routing**: the keyspace is 16384 slots, a key's slot is `CRC16(key) mod 16384` (or of its `{hash tag}`), and each node owns a range. Stock cluster clients already implement this, so `RedisCluster` and `ClusterClient` shard across MoCache nodes with no MoCache-specific code.

> **Status.** Sharding and client routing work today. **Replication and automatic failover do not exist yet** — a node's slots are unavailable while it is down. The design for both is at the end of this document; it is the next thing to build.

## Configuring a cluster

Every node is given the **same** peer list and told which entry it is:

```bash
mocache -resp :6379 \
  -cluster-announce cache-0.cache-headless:6379 \
  -cluster-peer cache-0.cache-headless:6379=0-5460 \
  -cluster-peer cache-1.cache-headless:6379=5461-10922 \
  -cluster-peer cache-2.cache-headless:6379=10923-16383
```

`-cluster-peer` is repeatable and also accepts a comma-separated list in a single flag, which is friendlier when the value comes from one environment variable. Replicas are declared against their primary:

```bash
  -cluster-peer cache-3.cache-headless:6379=replica-of:cache-0.cache-headless:6379
```

The node refuses to start if two peers claim the same slot, if a replica names an unknown primary, or if `-cluster-announce` is not one of the peers — each of those would make clients see two different answers for the same key.

**Node IDs are derived, not random.** A node's 40-character ID is `SHA-1("mocache-node:" + announce address)`, so every node computes the same ID for every peer, and a restart keeps its identity. Redis gossips randomly generated IDs; deriving them is what lets a static configuration be consistent without a membership protocol.

Without `-cluster-peer`, the node is standalone: it answers for every key, reports `cluster_enabled:0`, and never redirects.

The announce address is what clients are redirected to, so it must be reachable **by them** — a pod DNS name inside the cluster, not `localhost`.

## What clients see

| Situation | Reply |
|---|---|
| Key belongs to this node | Normal reply |
| Key belongs elsewhere | `-MOVED <slot> <host:port>` — the client retries there and refreshes its slot map |
| Multi-key command spanning slots | `-CROSSSLOT Keys in request don't hash to the same slot` |
| Slot has no owner | `-CLUSTERDOWN Hash slot not served` |
| Read on a replica after `READONLY` | Served locally |
| Write on a replica | `-MOVED` to the primary, always |

`CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER NODES`, `CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER KEYSLOT`, `CLUSTER COUNTKEYSINSLOT`, `CLUSTER GETKEYSINSLOT` and `CLUSTER REPLICAS` are all served.

Keep related keys together with a hash tag, or multi-key commands will be refused:

```
{user:1}:name   {user:1}:email    -> same slot, MGET works
user:1:name     user:1:email      -> different slots, CROSSSLOT
```

## Two routing schemes, one cache

This is the sharp edge of the current design. A MoCache node can be reached three ways, and they do not agree on placement:

| Front end | Routing | Decided by |
|---|---|---|
| HTTP `:8090`, RPC `:8091` | MD5 virtual-node ring | The MoCache SDK (`sdk/go`, `sdk/python`) |
| RESP `:6379` | CRC16 slots | The Redis client |

Both reach the same LRU on the same process, and neither redirects the other's traffic. So a key written through the Go SDK and read through `redis-py` **may be on different nodes** and read as a miss.

Pick one scheme per keyspace. Mixing is safe only on a single node, or if you keep separate key prefixes per access path. The intended direction is for the SDKs to move onto CRC16 slots so there is one scheme; until then, `docs/architecture.md` still describes the ring as the SDK's routing and that remains true.

## Resharding

There is none yet. Changing the slot map means changing `-cluster-peer` on every node and restarting them; keys that move are simply lost (they were cache entries). Live migration needs `CLUSTER SETSLOT` with `MIGRATING`/`IMPORTING`, the `MIGRATE` command and `-ASK` redirects — listed below.

## Operating it today

- **A node down = its slots down.** Clients get connection errors for that third of the keyspace. Applications must already treat a cache error as a miss (`docs/operations.md`); that is what carries you through.
- **Rolling restarts** lose one shard at a time. `CLUSTER INFO` reports `cluster_state:fail` if the peer list ever leaves a slot unowned.
- Watch `mocache_resp_moved_total`. A steady rate means clients are routing on a stale map; a spike after a config change is expected and should settle.
- `mocache_cluster_slots_assigned` should be 16384 on every node.

---

# Design: replication and failover

What follows is **not implemented**. It is the plan for making a shard survive the loss of its primary, using Redis Cluster's model — gossip for failure detection, majority voting among primaries for promotion — as chosen for this project.

## Why this model

Automatic promotion without a consensus step is how split-brain happens: two nodes each decide they are the primary for a slot, both accept writes, and the answer to a key depends on which one you asked. Redis and Valkey solve it with an epoch-numbered majority vote among primaries. Matching their model has a second benefit: clients already expect the resulting behaviour, since `MOVED` is how they learn a slot changed hands.

**We need client compatibility, not server compatibility.** No Redis node will ever join a MoCache cluster, so the cluster bus does not have to use Redis's binary bus format — only the client-facing commands must match. The bus can reuse the framing style already in `internal/protocol`, which is far less work than reimplementing Redis's bus.

## Replication

**Stream, not snapshot-and-hope.** A replica connects to its primary over the bus, receives a full snapshot of live entries (key, value, absolute expiry), then a continuous stream of mutations from a bounded in-memory backlog. Each mutation carries an offset; the replica reports the offset it has applied, which gives both a lag metric and the ordering needed for promotion.

Decisions that matter:

- **Replicate effects, not commands.** `INCR` ships as the resulting value, not as "increment". Replaying a command on a replica whose state has diverged silently compounds the divergence; replaying an effect cannot.
- **Replicate absolute expiry times, not TTLs.** A relative TTL restarts its clock on arrival, so a key would outlive its expiry by the replication lag on every hop.
- **Propagate evictions as deletes.** The primary evicts under its own memory pressure; without propagation, a replica keeps entries the primary has dropped and a `READONLY` read returns data the primary would report as a miss. This is what Redis does.
- **The backlog is bounded**, like the OTLP queue: a replica too far behind gets a full resync rather than making the primary buffer without limit.
- **Acknowledged, not synchronous.** Replication is asynchronous; a promoted replica may be missing the last few writes. For a cache that is acceptable, and it must be stated rather than implied — MoCache still gives no durability guarantee, only availability.

New commands: `REPLICAOF`/`SLAVEOF` for manual re-pointing, `WAIT` for "how many replicas have my write", and `INFO replication` reporting `master_repl_offset`, `slave_repl_offset`, and `master_link_status` (which today always reports `down`, honestly, because no stream exists).

## Failure detection

Every node opens a bus connection to every other node (a full mesh; fine at the tens-of-nodes scale this targets) on the client port + 10000 — the port already advertised in `CLUSTER NODES`.

1. Nodes exchange `PING`/`PONG` carrying a gossip section: for a random subset of known nodes, their ID, address, flags, config epoch, and slot ownership.
2. A node that has not answered within `-cluster-node-timeout` is marked **PFAIL** (possible failure) locally.
3. PFAIL reports travel in the gossip section. When a node sees PFAIL reports from a **majority of primaries**, it marks the node **FAIL** and broadcasts that.

A single node's opinion never removes a primary; that is the property that keeps a network hiccup from triggering a failover.

## Promotion

1. A replica of a FAIL-marked primary waits a rank-based delay — the replica with the most complete replication offset waits least, so the best candidate usually goes first.
2. It increments `currentEpoch` and asks every primary for a vote for that epoch.
3. Each primary grants **at most one vote per epoch**, and only for a replica of a node it also considers failed.
4. With votes from a majority of primaries, the replica promotes itself, takes a new `configEpoch` higher than the old primary's, claims the slots, and broadcasts the new configuration. Clients discover it through `MOVED`.
5. Conflicting claims are settled by the highest `configEpoch`.

Consequences to design for, not around:

- **An even number of primaries has no majority in a split.** Three or more primaries, always.
- **A minority partition cannot promote**, and must stop serving its slots rather than serve stale data — `-cluster-require-full-coverage` decides whether the rest of the cluster keeps serving the slots it still owns.
- **A returning old primary must demote itself** when it sees a higher `configEpoch` for its slots.

## Resharding

Slot migration rides on the same machinery: `CLUSTER SETSLOT <slot> MIGRATING <node>` on the source, `IMPORTING` on the destination, `MIGRATE` to move keys in batches, and `-ASK` redirects (plus `ASKING`) for keys already moved while the slot is in flight. Clients implement `ASK` already; it differs from `MOVED` precisely in that it does not update their slot map.

## How this gets verified

Consensus code cannot be signed off by a smoke test. The plan is a deterministic in-process simulation: a fake network that drops, delays, reorders and partitions messages under a seed, driving real node state machines with a virtual clock. The properties to assert are:

- **Safety:** no two nodes serve the same slot with the same `configEpoch`; no slot has two primaries accepting writes in one epoch.
- **Liveness:** with a majority partition and messages eventually delivered, the cluster converges to full slot coverage.
- **Client-visible:** a killed primary leads to `MOVED` pointing at the promoted replica within a bounded time, and writes never succeed on both sides of a partition.

Plus a lab scenario: kill a primary under load from `lab/bench` and report the error window and the hit-ratio dip.

## Order of work

1. Bus transport, mesh connections, PING/PONG with gossip. Topology becomes a swappable immutable snapshot (the read API — `OwnerOf`, `Mine`, `Nodes` — already isolates the RESP layer from this change).
2. Replication stream: snapshot, backlog, offsets, `INFO replication`, `REPLICAOF`, `WAIT`.
3. PFAIL/FAIL detection and propagation.
4. Epochs, voting, promotion, demotion of a returning primary.
5. The simulation harness, run in CI with many seeds.
6. Slot migration and `ASK`.

Steps 1–2 are useful on their own: replicas that serve `READONLY` reads and a manual `CLUSTER FAILOVER` are real availability gains, and they are the foundation the voting work sits on.
