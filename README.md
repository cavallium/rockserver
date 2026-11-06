# Rockserver

Requires Java 27. Start it with `--enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector`: the
second flag lets hot paths use the Vector API (without it they fall back to scalar code).

## Workload admission

Rockserver schedules requests by the explicit `LATENCY`, `INGEST`,
`ANALYTICAL`, `BATCH`, `CONTROL`, and `PHYSICAL_MAINTENANCE` profiles. Data
profiles share hard read/write worker limits with borrowable reservations;
CONTROL and physical maintenance use isolated pools.

```hocon
database.parallelism {
  read = 20
  write = 36
  workload {
    latency-queue-capacity = 4096
    ingest-queue-capacity = 4096
    analytical-queue-capacity = 512
    batch-queue-capacity = 512
  }
}
```

Each queue rejects overflow immediately as `SERVER_OVERLOADED`; gRPC exposes it
as `RESOURCE_EXHAUSTED`. Admission metrics use bounded `database`, `resource`,
`profile`, and `operation` tags where applicable:

- `rockserver.workload.queued`
- `rockserver.workload.active`
- `rockserver.workload.queue.wait`
- `rockserver.workload.execution`
- `rockserver.workload.quantums`
- `rockserver.workload.outcomes`
- `rockserver.workload.cancellations`
- `rockserver.workload.rejections`
- `rockserver.workload.failures`
- `rockserver.workload.worker.failures`
- `rockserver.workload.storage.pressure`
- `rockserver.workload.retained.snapshots`

See [workload profiles](docs/workload-profiles.md) for admission rules and
[workload configuration](docs/workload-configuration.md) for every setting,
default, and startup invariant.

## gRPC overload regression benchmark

`GrpcOverloadBenchmark` is the opt-in, disk-backed whole-path complement to the
embedded `SevenProfileWorkloadBenchmark`. It sends real loopback gRPC traffic for
all six profiles during a harsh mixed phase. Every measured RPC is counted at
client-call start and exactly one terminal close; an independent integrity phase
writes unique INGEST/BATCH values and reads every acknowledged value back through
LATENCY. Scheduler snapshots prove all four pools drain with balanced
started/completed counters and no worker failures, while sampled sleeping-worker
state detects eligible queued work that leaves capacity idle for more than a short
wake-up streak. The result also gates foreground p99/throughput regression,
priority ordering, the eight-millisecond cooperative queue bound, cancellation,
profile progress, resources, native handles, errors, rejections, and shutdown.

This gRPC runner does not replace the release tuner: pressured-BATCH acceptance,
candidate selection, and controlled hardware comparisons still belong to
`SevenProfileWorkloadBenchmark` and its selector. A smoke run proves structure and
correctness only; it is not hardware acceptance.

Use the prepare/reopen workflow for a real cold page cache. The current command,
options, and acceptance rules are documented in
[`docs/grpc-mixed-workload-benchmark.md`](docs/grpc-mixed-workload-benchmark.md).
The dated
[`benchmarks/grpc-overload-2026-07-23.md`](benchmarks/grpc-overload-2026-07-23.md)
is historical two-profile evidence and is not comparable to the current schema.
The release hardware workflow and deterministic 5% throughput/10% p99 selector
are documented in [`docs/workload-tuning.md`](docs/workload-tuning.md).

`GrpcRawScanBenchmark` is the paired whole-gRPC raw-SST gate. It creates one
explicitly flushed dataset, runs the untouched and candidate production classpaths
in alternating order, validates every streamed key/value and gRPC terminal, proves
READ-pool saturation and bounded avoidable idle time, and evaluates paired 95%
confidence intervals for throughput, latency, CPU, allocation, memory, and exact
resource non-increase.
The exact one-shot command and acceptance boundary are documented in
[`docs/grpc-raw-scan-benchmark.md`](docs/grpc-raw-scan-benchmark.md).

`GrpcRetainedReadBenchmark` is the paired whole-gRPC non-regression gate for exact
counts, streamed ranges, split `existsMulti`, and long explicit-iterator continuations.
It runs ten predetermined baseline/candidate pairs for isolated and foreground-mixed
scenarios, validates every result and terminal resource count, and applies strict
log-space confidence bounds to throughput, latency, CPU, allocation, and memory.
The release-only workflow is documented in
[`docs/grpc-retained-read-benchmark.md`](docs/grpc-retained-read-benchmark.md).

The same-build explicit-iterator service-bound mechanism and its idle/non-idle causal ablation are
documented in [`docs/iterator-quantum-ablation.md`](docs/iterator-quantum-ablation.md).

Both read gates and the paired seven-profile gate share the v1.3.11 Pareto rules and
versioned result schemas documented in
[`docs/v1.3.11-performance-contract.md`](docs/v1.3.11-performance-contract.md).

## Client connections

`GrpcConnection` connects to a server over gRPC: `forHostAndPort` for one address, `forPath` for a Unix socket, and
`forHostsAndPorts` for several nodes serving the same database, such as a [replicated
cluster](docs/distributed/design.md), where any node serves any request.

```java
var connection = GrpcConnection.forHostsAndPorts("my-client", List.of(
		new HostAndPort("node-1", 5333),
		new HostAndPort("node-2", 5333),
		new HostAndPort("node-3", 5333)));
```

`Utils.parseHostAndPorts("node-1:5333,node-2:5333,node-3:5333")` gives that list from a string,
`ClientBuilder.setHttpAddresses` takes it, and `--database-url` accepts the same list:
`http://node-1:5333,node-2:5333,node-3:5333`. A host without a port uses 5333, and an IPv6 literal goes in brackets
(`[::1]:5333`). One address works as before.

The connection uses one node at a time, and moves within the one gRPC channel, so nothing has to be recreated:

- It uses the first address that accepts a connection, never shuffling the list, so all the clients with the same
  list sit on the first node. List the expected leader first, so that their calls do not take a forwarding hop
  through a follower. After a leader change they stay where they are.
- A replicated node names its cluster's leader in every response. If that address is in the client's list (same host
  name and port), the connection opens a second channel to it, and sends there every call the leader serves: writes,
  transactions, updates, iterators and ranges. Point reads (`get`, `exists`, `existsMulti`, `getMulti`), maintenance
  and the handshake stay on the node of the ordinary channel. The leader's channel is used only while it is
  connected; until then, and once a call on it ends with `UNAVAILABLE` or the node stops naming itself as the
  leader, calls go through the ordinary channel, and a follower forwards them as before. A read that the retry policy
  allows and that fails on the leader's channel before any answer runs again on the ordinary channel; any other call
  in flight on it when it fails fails as on any lost connection. List the nodes with the host names their peer configuration uses (`--peer`), or set the property below
  to `any`.
- When the connection to its node is lost, calls in flight fail as they do with one address: the reads that the
  automatic retry policy allows run again, on the next node, and the others surface `UNAVAILABLE`, because a write
  may already have been applied. On a replicated cluster, writes are sent again instead (see below). New calls wait
  for the next connection, which starts from the top of the list.
- It stays on its node when an earlier one comes back, until that connection is lost or the channel goes idle (30
  minutes without calls). So a client that failed over stays on that node until the node goes away, even if the
  first address returned long before. A server can end connections at a maximum age instead, which brings clients
  back to the first address, but that is off by default because of what it costs (see the table of properties).
- With every node down, calls wait for one full pass over the list, which takes up to the connect timeout for each
  node that does not answer, and from then on they fail with `UNAVAILABLE` at once. The reads that the retry policy
  allows also spend its backoff. The connection keeps trying the list, at least every 2 s, each client at its own
  pace. One address works the same way. Those attempts stop when the connection works again, or when the channel goes
  idle after 30 minutes without calls: a connection that works or is idle has nothing scheduled.
- Names are resolved again when a connection is lost. A name that stops resolving keeps its last addresses, and one
  whose lookup hangs does not hold back the others for more than 1 s: its host joins when the lookup answers.

| When a node | The client notices | New calls run on another node |
|---|---|---|
| Dies, its host stays up (crash, SIGKILL) | At once: the connection is reset | In milliseconds: the port refuses connections |
| Stops answering with its host down, cut off from the client or frozen (power loss, SIGSTOP) | By keepalive, in at most 15 s | After that, plus one connect timeout for each such node listed before the one that works |
| Is cut off from the Raft majority, and the client still reaches it | The node stops serving: it loses its leader within one election timeout, and after 3 maximum election timeouts without one, at least 6 s (about 8 s in all by default), it stops listening and sends GOAWAY. A node that restarts while cut off does the same, counted from its start. Calls running on it end as they do | In milliseconds. Until then calls fail with the errors of a node without a leader |
| Has a hung disk | The node's disk probe (an fsync in its data directory, once a second) does not end within the request timeout (10 s by default): the node stops listening and sends GOAWAY, as when it is cut off. Calls running on it wait for their deadline. A leader with such a disk also hands over its leadership after 3 maximum election timeouts, at least 5 s | In milliseconds after the pause. Until then calls wait |

| System property | Default | Effect |
|---|---|---|
| `it.cavallium.rockserver.grpc.client.connect-timeout-ms` | `3000` | With several addresses, the limit for connecting to one address and, separately, for hearing the first bytes from it. |
| `it.cavallium.rockserver.grpc.client.keep-alive-time-ms` | `10000` | Silence on a connection before a ping, with one address too. gRPC raises values below 10 s to 10 s. |
| `it.cavallium.rockserver.grpc.client.keep-alive-timeout-ms` | `5000` | Wait for the answer to a ping, with one address too. |
| `it.cavallium.rockserver.grpc.client.max-reconnect-delay-ms` | `2000` | The longest wait between two attempts on the list, or on the only address, while they all fail. Each client picks a period between half of this and this. |
| `it.cavallium.rockserver.grpc.client.handshake-wait-ms` | `30000` with several addresses, `0` with one | How long a new connection keeps asking for the capabilities of a server, before it fails, while the answer is `UNAVAILABLE` or no answer comes within 10 s. It ends at this time, or with the first attempt, which takes up to 10 s, if that is later: `0` means one attempt. |
| `it.cavallium.rockserver.grpc.client.max-inbound-message-size-bytes` | `67108864` (64 MiB) | The largest response message the client accepts, between 4 MiB and 2^31-1 bytes; a larger value fails the connection's construction. Raise it with the server's limit for very large `getMulti` pages or results. |
| `it.cavallium.rockserver.grpc.client.max-retry-attempts` | `5` | Attempts of gRPC's automatic retry of the stateless reads (`get`, `getMulti`, range pages, capabilities...) after `UNAVAILABLE`. `1` turns the retry off. Writes are never retried by gRPC. |
| `it.cavallium.rockserver.grpc.client.initial-retry-backoff` | `0.05s` with several addresses, `0.5s` with one | First pause of that retry, as a gRPC duration. With several addresses the retry goes to another node, so it need not wait for the failed one. |
| `it.cavallium.rockserver.grpc.client.max-retry-backoff` | `30s` | Longest pause of that retry. |
| `it.cavallium.rockserver.grpc.client.retry-backoff-multiplier` | `2` | Growth of that pause. |
| `it.cavallium.rockserver.grpc.client.write-retry-window-ms` | `30000` | On a replicated cluster, how long a write of a caller without a deadline is sent again after an unknown outcome. A caller's deadline bounds it otherwise. |
| `it.cavallium.rockserver.grpc.client.session-writes` | `true` | `false` turns off client sessions (below) for connections made afterwards: writes are sent once, as to a single server. |
| `it.cavallium.rockserver.grpc.client.leader-hint` | `listed` | With several addresses, which leader addresses named by the nodes the connection uses for the calls the leader serves: `listed` only those in its list, `any` any, `off` none. A server could otherwise send the client anywhere, and the address the members use among themselves may not be reachable from the client. |
| `it.cavallium.rockserver.grpc.server.max-connection-age-ms` | `0` (off) | On a server in front of another connection, such as a replicated node, the age at which a connection ends, +/-10%. The client then starts again from the top of its list, which is what brings it back to the first address. It has two costs: a silent first address costs every client the connect timeout at every cycle, not once, and calls that outlive the grace below fail with `UNAVAILABLE`, which includes `Compact` and long scans, on a healthy cluster too. Left off, a client stays on the node it failed over to until that node goes away. |
| `it.cavallium.rockserver.grpc.server.max-connection-age-grace-ms` | `600000` | With a maximum age, the time left to the calls running on a connection that reached it. |

On a replicated cluster, when the node that died was the leader, the new calls also wait for the election.

A replicated cluster advertises client sessions, and the connection uses them (the forwarding connections between
nodes do not). Its first write opens a session through the replicated log. Every write outside a transaction, every
transaction commit, the write that ends an update opened through the connection, `deleteRange` and schema change then
carries the session id and a number of its own. `putBatch` and `mergeBatch` (when the server also advertises
`numberedBatches`; otherwise they are streamed once, unnumbered, as before) are sent as numbered chunks of at most
4 MiB, one call per chunk, in order, within the call's deadline; once a chunk took effect, a later failure ends the
call with `INVALID_REQUEST` ("partly applied"), since resending the whole batch would apply the first chunks twice. When its outcome is unknown, because the connection was lost (`UNAVAILABLE`, or `CANCELLED` by a
server that stops) or the server answered `OUTCOME_UNKNOWN`, the connection sends the same numbered write again,
after a pause of 50 ms that doubles up to 1 s, to whichever node it reaches, until the caller's deadline. The cluster
applies a numbered write once and answers every copy with the first result. Only when the deadline ends that does the
caller get `OUTCOME_UNKNOWN`: the write may or may not be applied, and sending it again as a new write could apply
it twice. A deadline error counts as an unknown outcome unless the server flagged it "not applied" (in the
`rockserver-applied: no` trailer; the message text is not read when the trailer is there). A caller that cancels a
write's future stops the resends: the future ends with `OUTCOME_UNKNOWN` if a copy was sent, or is cancelled if
none was. A synchronous call waits at most its deadline plus 5 s. A write staged in a transaction is sent once, as
before: if its outcome is unknown, roll the transaction back and run it again, never send the staged write again.
The connection decides whether it uses sessions once, at its handshake, and logs it: recreate it after upgrading
the cluster. With a single server, which has no sessions, nothing changes.

A replicated node without a leader for that long serves again by itself: once it has known a leader for one maximum
election timeout it listens again, on its own or with every other node after a period when the whole cluster had no
leader. An election shorter than the bound stops nothing. A client with a single address has nowhere to go: while
its node does not listen its calls fail with `UNAVAILABLE`, and it reconnects within the 2 s above once the node
listens again. A node that restarts with a whole cluster that is down can stop listening too, if the cluster takes
longer than the bound to elect a leader, when no node could serve anyway. Only a node of a new cluster, which has never
known a leader, keeps listening while the cluster forms.

`GrpcConnection` asks the server for its capabilities before it returns, and no node may answer yet, for example during
a cold start. With several addresses it asks again while the answer is `UNAVAILABLE` or no answer comes within 10 s, as
with silent or frozen nodes, for 30 s by default, and then fails. A list where no host resolves waits too, because a name
can exist only once its node runs, so a list with a typo in every host also takes 30 s to fail. Any other answer fails
at once, such as a server of another version or one without the handshake. With one address there is no such wait:
the call's own retries run, which take about 8 s on a refused connection, and then it fails, so that a client that
starts with its server still connects and a plain client of a server that is down hears it soon. A silent single
address fails after 10 s. `ClientBuilder.setDeferred` skips the handshake until the first use, and closing that
connection ends a handshake in progress.

To replace a node of a replicated cluster, start the new node first with `--join`, and run
`admin --peer <the other members> add-peer <new node>` when you like: the node waits for it, also across restarts, and
leaving the old node out of `--peer` replaces it in one step. See "Replacing a node" in the [design
document](docs/distributed/design.md). Run every replicated node with `-XX:+ExitOnOutOfMemoryError` (it refuses to start
without it), and see "Heap" there for the heap that a snapshot send needs. A removed node that is started again exits
with 4: set `RestartPreventExitStatus=4` in its systemd unit. A replicated node flushes its memtables when all columns together
reach `database.global.write-buffer-manager` (128 MiB by default): it replaces every column's `write-buffer-size`
with that budget, because with atomic flush any flush writes every column (see "Memtables" in
[performance.md](docs/distributed/performance.md)).

Shorter keepalive times find a dead node sooner, but a pause of the server or of the client that outlasts the
timeout ends a working connection, with its calls in flight. Pings cost next to nothing, and a server accepts them
every 5 s. A short connect timeout moves past a dead node sooner, but one that is just slow to accept a connection
is skipped too. Each silent node at the start of a list costs a new connection one connect timeout of its handshake
wait.

## Merge operators

Columns can use a server-side `MergeOperator` instead of client-side
read-modify-write. See [merge operators](docs/merge-operators.md) for the write
path and how to update or hot-swap an operator.

## Fast unary GET

Embedded databases with `database.global.enable-fast-get=true` use the owned
native GET API from `it.cavallium:rocksdbjni:11.1.2.6`. The public synchronous
API always returns an independent heap `Buf`. Unary gRPC current-value reads may
instead retain a RocksDB pin only for synchronous response framing; transactions,
bucketed columns, and proxy backends keep the ordinary implementation.

The default gRPC strategy is `automatic`: it inspects the pinned result size and
either streams it directly or uses the JNI `copyAndReset` path for independently
owned heap output.
The measured default uses pinned streaming through 128 bytes, from 512 bytes
through 4 KiB, and at or above 32 KiB. The remaining bands use independently
owned heap output. Operators can replace that table with one cutoff by setting
`-Drockserver.fast-get.pinned-min-bytes=<bytes>` (read by the database, not the gRPC layer). The
`rockserver.grpc.fast-get.strategy` property accepts `legacy`, `exact-heap`,
`pinned`, and `automatic`; the non-automatic values exist for the
performance matrix and operational comparison.

Run the five-round real-RocksDB/local-gRPC release gate with:

```shell
mvn -DskipTests test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=it.cavallium.rockserver.core.impl.benchmark.GrpcFastGetBenchmark
```

## SST table properties

Read a column's persisted table statistics using an analytical context:

```java
var api = connection.getSyncApi(RequestContext.analytical(Duration.ofSeconds(30)));
var properties = api.getTableProperties(columnId);
long dataBytes = properties.dataSize();
long physicalEntries = properties.numEntries();
// Also available: getTablePropertiesAsync(columnId).
```

Embedded, gRPC and Thrift return the same immutable `ColumnTableProperties` model;
Rust exposes `get_table_properties`. Use `getAllColumnDefinitions()` to enumerate
columns. Each column is observed independently. The result contains SST counts,
data/index/filter and raw sizes, block/entry/deletion/merge counts, compression
estimates, timestamp bounds, and distributions of formats, fixed key lengths,
index flags, column-family IDs, compression and other table configuration names.
Distribution values count files; empty strings mean unspecified names. Sizes are
bytes and timestamps are Unix seconds (zero means unknown). Creation-time bounds
refer to RocksDB's oldest-ancestor timestamps, not filesystem creation times.
Compression estimates sum available samples only and can cover a subset of files.

This call does not flush, compact, or scan data rows. It excludes memtables, WALs,
blob files and custom collector byte strings. Entries can include tombstones and
obsolete versions, and bucketed columns count physical entries rather than logical
rows. Reading metadata can still perform I/O for every SST, so use ANALYTICAL for
interactive requests and BATCH for periodic work. Cancellation cannot interrupt a
native metadata read; its database and column leases remain held until it returns.

Optional background Micrometer collection is disabled by default:

```hocon
database.metrics {
  table-properties-enabled: true
  table-properties-interval-seconds: 300
}
```

`rocksdb.table.properties` exports numeric values tagged by `database`,
`column_family` and `property_name` (for example `data.size` or `num.entries`).
There are no per-file or arbitrary string-value labels. Collection runs sequentially
in the BATCH lane, at most once per configured interval (minimum 60 seconds), and
scrapes read cached values. Check `rocksdb.table.properties.collection.success`
and `rocksdb.table.properties.last.success.time` for failure and freshness. Failed
collections remove the old value series; successful collections remove dropped
columns. JMX and Influx exporters use the existing metrics configuration.

## Package fat jar
```shell
mvn -Pfatjar -Dagent -DskipTests clean package
```

## Package desktop UI

Select `desktop` instead of `fatjar`, `library`, or `native` (including in the IDE's Maven profiles).
It includes the server/client code, standalone logging, and the desktop UI in a shaded executable JAR:

```shell
mvn -Pdesktop -DskipTests package
java --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -jar target/rockserver-core-1.0.0-SNAPSHOT-desktop.jar
```

GUI sources and tests live under `src/desktop/java` and `src/desktop-test/java`.
`mvn -Pdesktop test` includes the GUI tests; use `xvfb-run -a` on a headless Linux machine.
The other profiles omit the GUI sources and runtime dependencies. Each build variant uses
its own class/test output directories, so switching profiles cannot reuse the wrong module descriptor.

## Package native
```shell
GRAALVM_HOME=/usr/lib/jvm/xx;JAVA_HOME=/usr/lib/jvm/xx mvn -Pnative -Dagent -DskipTests clean package
```

## Deploy the library
```shell
mvn -Plibrary -Dagent -DperformRelease=true -DskipTests -Dgpg.skip=true -Drevision=1.0.0-SNAPSHOT deploy
```
