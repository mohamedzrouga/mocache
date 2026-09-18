# Cluster

MoCache uses **Redis Cluster's key routing**: the keyspace is 16384 slots, a key's slot is `CRC16(key) mod 16384` (or of its `{hash tag}`), and each node owns a range. Stock cluster clients already implement this, so `RedisCluster` and `ClusterClient` shard across MoCache nodes with no MoCache-specific code.

> **Status.** Sharding, replication and automatic failover all work. Nodes gossip over a private cluster bus, primaries stream their mutations to replicas, and a replica is promoted by majority vote of the primaries when its primary dies. What is *not* built: live slot migration (resharding) and `ASK` redirects — see [Resharding](#resharding).

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

A shard survives the loss of its primary only if it has a replica, and a promotion needs a **majority of primaries** to agree — so run **three or more primaries**, each with at least one replica. With two primaries a split has no majority on either side and nothing can ever be promoted.

The node refuses to start if two peers claim the same slot, if a replica names an unknown primary, or if `-cluster-announce` is not one of the peers — each of those would make clients see two different answers for the same key.

**Node IDs are derived, not random.** A node's 40-character ID is `SHA-1("mocache-node:" + announce address)`, so every node computes the same ID for every peer, and a restart keeps its identity. Redis gossips randomly generated IDs; deriving them is what lets a static configuration be consistent without a membership protocol.

Without `-cluster-peer`, the node is standalone: it answers for every key, reports `cluster_enabled:0`, and never redirects.

The announce address is what clients are redirected to, so it must be reachable **by them** — a pod DNS name inside the cluster, not `localhost`.

## The cluster bus

Beyond the client port, each node listens on **client port + 10000** — the port `CLUSTER NODES` already advertises. That is where nodes gossip, where replicas pull their stream, and where votes are cast. Open it between nodes; never expose it to clients.

| Flag | Default | Meaning |
|---|---|---|
| `-cluster-bus` | `true` | Run gossip, replication and failover (needs `-cluster-peer`) |
| `-cluster-bus-addr` | *(derived)* | Override the bus listen address |
| `-cluster-node-timeout` | `5s` | Peer silence before it is suspected; other timings derive from this |
| `-cluster-failover-delay` | `500ms` | Base wait before a replica stands for election |
| `-repl-backlog-bytes` | `32Mi` | Backlog per primary; a replica further behind resynchronises |

Setting `-cluster-bus=false` gives the earlier behaviour: sharding, but no replication and no failover.

## Replication

A replica opens one connection to its primary's bus and receives a snapshot followed by a live stream of mutations. It serves reads after `READONLY` and redirects every write to the primary.

What travels, and why it is shaped this way:

- **Effects, not commands.** `INCR` ships as the resulting value. Replaying a command against a replica that has diverged compounds the divergence; replaying an effect cannot.
- **Absolute expiry times, not TTLs.** A relative TTL would restart its clock on arrival and the key would outlive the primary's copy by the replication lag.
- **Evictions and lazy expiry propagate as deletes.** Otherwise a replica keeps answering `READONLY` reads with entries the primary has already dropped.
- **The backlog is bounded.** A replica that falls further behind than `-repl-backlog-bytes` is disconnected and does a full resync, rather than being allowed to grow the primary's heap. A reconnecting replica that is still within the backlog gets a partial resync instead.

**Replication is asynchronous.** A promoted replica can be missing the last few writes. That is the right trade for a cache, and it is why `WAIT numreplicas timeout` exists: it reports how many replicas have acknowledged everything written so far, and it is the only way a client can know its write survived the loss of the primary.

`INFO replication` reports the live stream — `master_link_status`, `master_repl_offset`, per-replica offsets — not the configured intent.

## Failover

1. A node that has not answered within `-cluster-node-timeout` is marked **PFAIL** locally.
2. Suspicion travels in the gossip section of every ping. When a **majority of primaries** agree, the node is marked **FAIL** and that decision is broadcast.
3. Replicas of a failed primary wait a delay ranked by replication offset — the most up-to-date replica waits least, so the best candidate usually stands alone — then claim an election epoch and ask every primary for a vote.
4. A primary grants **at most one vote per epoch**, and only to a replica of a primary it also believes has failed.
5. On a majority the replica promotes itself, takes a config epoch higher than the old primary's, claims the slots and broadcasts the new configuration. Clients find it through `MOVED`.

Two properties do the real work. **One vote per epoch** stops two replicas of the same primary from both winning. **Highest config epoch wins** settles every conflicting claim afterwards, including the one that matters most: a primary that returns after being replaced sees a higher epoch on its old slots, stands down, becomes a replica of whoever replaced it, and resynchronises. It never serves those slots again on its old data.

Slot claims are also voided by gossip, not only by the promotion broadcast: a node that missed the broadcast still converges, instead of advertising a second owner for slots it no longer holds.

**A promoted node that restarts takes its promotion back.** Nothing is written to disk, so it comes back with the role its `-cluster-peer` flags describe — a replica — while every other node still holds its promotion at a higher config epoch. It learns of it through gossip and resumes serving those slots at the epoch it was elected at, empty, refilling on misses. Without that, the cluster would redirect clients to a node that disowned the slots and sent them back, which no amount of waiting resolves.

### Manual failover

```
CLUSTER FAILOVER            # stand for election (the primary must be marked failed)
CLUSTER FAILOVER FORCE      # stand for election without waiting for that
CLUSTER FAILOVER TAKEOVER   # promote with no votes at all
CLUSTER REPLICATE <node-id> # make an empty, slot-less node a replica of another
```

`TAKEOVER` skips the majority. It is the one command here that can produce two owners for one slot, and it exists for the case where an operator knows something the cluster cannot — a datacentre that is genuinely gone. `REPLICAOF` is refused in cluster mode, exactly as Redis refuses it.

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

## Operating it

- **A primary with a replica survives being killed**; measured in the lab at roughly 3-4 seconds from kill to a promoted replica serving reads, with `-cluster-node-timeout=2s`. Lower the timeout for faster detection at the cost of promoting on transient network blips.
- **A primary without a replica takes its slots with it** until it returns. Applications must treat a cache error as a miss (`docs/operations.md`); that is what carries you through either way.
- **Rolling restarts** are safe shard by shard, but do not restart a primary and its replica together.
- Watch `mocache_resp_moved_total`: a steady rate means clients are routing on a stale map. A spike after a failover is expected and should settle within seconds.
- `mocache_cluster_slots_assigned` should be 16384 on every node, and `mocache_repl_link_up` should be 1 on every replica.
- `mocache_cluster_failovers_total` rising when nobody asked for it means the failure detector is too twitchy for the network — raise `-cluster-node-timeout`.

---

# Design notes

## Why this model

Automatic promotion without a consensus step is how split-brain happens: two nodes each decide they are the primary for a slot, both accept writes, and the answer to a key depends on which one you asked. Redis and Valkey solve it with an epoch-numbered majority vote among primaries. Matching that model has a second benefit: clients already expect the resulting behaviour, since `MOVED` is how they learn a slot changed hands.

**We need client compatibility, not server compatibility.** No Redis node will ever join a MoCache cluster, so the bus does not use Redis's binary cluster-bus format — only the client-facing commands have to match. The bus is a length-prefixed frame with a JSON header and a raw binary body: readable in a packet capture, and no base64 tax on replicated values.

## How it is verified

- `internal/cluster` holds the rules — who may vote, when a claim wins, when a node stands down — as a pure state machine with no I/O, unit-tested directly (`failover_test.go`). Those tests are deterministic.
- `internal/node` runs real nodes over real TCP on loopback with compressed timings: replication, snapshot resync, promotion on a majority, **no** promotion without one, a returning primary demoting itself, and manual takeover (`cluster_test.go`).
- `internal/cluster/sim_test.go` is a **deterministic partition simulation**: no network and no wall clock, just the real `Manager` state machines driven from one seeded event queue. It can do what loopback cannot — delay one message past another, cut the cluster in half and heal it while messages are in flight, crash and restart nodes, and run every node on a clock that disagrees with its peers. After *every* event it asserts that no two nodes serve the same slot at the same config epoch, that no view's slot owner ever goes backwards in config epoch, that no primary grants two votes in one epoch, and that no config epoch is claimed twice. A run is a pure function of its seed, so a failure replays.
- `lab/` runs six containers and the actual Redis clients, so a failover is checked the way a user would see it — kill a container, watch `redis-py` and `go-redis` keep working.

What that still does **not** cover: the bus framing and the replication stream under an adversarial network — the simulation models ownership decisions, not bytes — and corrupted or forged frames, which nothing here defends against. The simulation also *mirrors* `internal/node`'s loop rather than running it, so the two can drift; a change to the real loop means a change to the model.

## What is not built

- **Live slot migration.** `CLUSTER SETSLOT` with `MIGRATING`/`IMPORTING`, the `MIGRATE` command, and `-ASK` redirects (plus `ASKING`). Until then, changing the slot map means changing `-cluster-peer` everywhere and restarting; keys that move are lost.
- **Chained replication.** A replica will not serve as another replica's primary; it answers such a subscription with an error rather than handing out offsets it cannot honour.
- **Replica migration.** Redis moves a spare replica to a shard that has none. Here, a shard whose only replica is gone stays exposed until someone runs `CLUSTER REPLICATE`.
- **Persistence.** Unchanged and deliberate: a restarted node comes back empty and refills from its primary. Replication buys availability, not durability.
