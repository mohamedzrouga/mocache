# Cluster

MoCache uses **Redis Cluster's key routing**: the keyspace is 16384 slots, a key's slot is `CRC16(key) mod 16384` (or of its `{hash tag}`), and each node owns a range. Stock cluster clients already implement this, so `RedisCluster` and `ClusterClient` shard across MoCache nodes with no MoCache-specific code.

> **Status.** Sharding, replication, automatic failover and live resharding all work. Nodes gossip over a private cluster bus, primaries stream their mutations to replicas, a replica is promoted by majority vote of the primaries when its primary dies, and slots move between live nodes without taking keys offline. What is *not* built: chained replication, and persistence — see [What is not built](#what-is-not-built).

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
| `-cluster-replica-migration` | `true` | Move a spare replica to a shard that has none |
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

### Replica migration

A shard with no replica cannot fail over, so it is one crash away from taking its slots offline until someone intervenes. When another shard has a spare, that spare moves to cover it:

- Only a **spare** moves — a replica whose shard keeps at least one other healthy replica behind, so the move never creates the problem it is solving.
- Every replica decides this locally from the same gossiped view, and the choice is deterministic (lowest node ID stays, the next one moves), so several spares cannot pile onto one orphan at once.
- `-cluster-replica-migration=false` turns it off.

The node it moves to is recorded by that node itself and travels in its gossip — a replica is the authority on which primary it follows, and every other node has to agree, or a voter would refuse to vote for it when its new primary dies.

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

The SDKs can now be told to route by slots instead, which is what makes one keyspace safe to share:

```go
mocache.New(nodes, mocache.WithSlots(map[string]string{
    "http://cache-0:8090": "0-5460",
    "http://cache-1:8090": "5461-10922",
    "http://cache-2:8090": "10923-16383",
}))
```

```python
MoCacheClient(nodes, slots={"http://cache-0:8090": "0-5460", ...})
```

The ranges are the ones each node's `-cluster-peer` entry declares, and they must cover all 16384 slots exactly once — a gap or an overlap is refused at construction rather than silently misrouting.

The ring stays the default, so upgrading does not move an existing deployment's keys. Two limits are worth being explicit about: the slot map is **static**, so it does not follow a failover or a reshard — after either, the affected slots are served by a node the map does not name, and the SDK misses on them until it is updated. The ring has always had the same limitation, since neither knows anything about cluster membership. A client that must track topology changes should use the RESP port with a real cluster client, which follows `MOVED`.

`docs/architecture.md` describes the ring, which remains what the SDK does unless you configure slots.

## Resharding

Slots move between live nodes without taking keys offline, using the same commands and the same redirects Redis uses — so `redis-cli --cluster reshard` drives it, and stock cluster clients follow it without noticing.

Moving a slot cannot be atomic: its keys are copied one at a time while clients keep reading and writing. Two per-slot markers close that window.

| Marker | Where | Effect |
|---|---|---|
| `MIGRATING <dest>` | the current owner | a key that is still here is served here; a key that is gone gets `-ASK <slot> <dest>` |
| `IMPORTING <src>` | the destination | serves the slot only for a connection that sent `ASKING`; everyone else gets `-MOVED` back to the source |

Together they mean a key is always served by exactly one of the two nodes, and the client is told which one without being told the slot has moved. Ownership changes only at the final step.

```
# 1. Open the window — on the destination first, so no key is ever unreachable.
redis-cli -p 7002 CLUSTER SETSLOT 866 IMPORTING <source-node-id>
redis-cli -p 7001 CLUSTER SETSLOT 866 MIGRATING <dest-node-id>

# 2. Move the keys, in batches.
redis-cli -p 7001 CLUSTER GETKEYSINSLOT 866 100
redis-cli -p 7001 MIGRATE 127.0.0.1 7002 "" 0 5000 KEYS key1 key2 ...

# 3. Commit, on both nodes and on every other primary.
redis-cli -p 7002 CLUSTER SETSLOT 866 NODE <dest-node-id>
redis-cli -p 7001 CLUSTER SETSLOT 866 NODE <dest-node-id>
```

`CLUSTER SETSLOT <slot> STABLE` abandons a migration and leaves ownership where it is.

Things worth knowing:

- **`MIGRATE` moves data over the cluster bus**, not over the destination's client port. The command's arguments are Redis's, because that is what tooling sends; the wire format between the two nodes is ours, and the destination's client port is never used. The whole batch is one frame and one round trip, so it either all arrives or none of it does.
- **Keys arrive with their absolute expiry**, not a restarted TTL — the same rule replication follows.
- **A missing key is not an error.** `MIGRATE` skips keys that are gone and answers `+NOKEY` if none were found: in a cache an entry can expire or be evicted between the `GETKEYSINSLOT` that listed it and the `MIGRATE` that moves it.
- **Committing is refused while the source still holds keys in the slot.** They would be invisible at the new owner and unreachable at the old one.
- **Only the destination raises a config epoch.** That is the one new claim, and it reaches every other node by gossip exactly as a failover does — so a node that was never sent `SETSLOT NODE` still converges.
- **An abandoned reshard leaves no trace.** The markers are local to the two nodes and carry no config epoch; nothing else in the cluster ever learns about a move that was not committed.
- **Drive a reshard from one place.** Two running at once can draw the same config epoch. Redis has the same property and the same answer.
- Watch `mocache_resp_ask_total`: it should be non-zero only while a reshard is running.

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

- **Chained replication.** A replica will not serve as another replica's primary; it answers such a subscription with an error rather than handing out offsets it cannot honour.
- **Automatic rebalancing.** Slots move on demand, but nothing decides *when* they should: there is no equivalent of `redis-cli --cluster rebalance` choosing a plan. Resharding is an operator action.
- **Persistence.** Unchanged and deliberate: a restarted node comes back empty and refills from its primary. Replication buys availability, not durability.
