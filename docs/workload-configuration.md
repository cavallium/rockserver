# Rockserver workload configuration

All workload tuning is under `database.parallelism.workload`. The old
`maintenance-write`, `foreground-write-queue-capacity`, and
`maintenance-write-queue-capacity` keys do not describe the seven-profile
scheduler and have been removed. There is no compatibility translation.

The effective database configuration logged at startup prints every value below.
Durations use ISO-8601 syntax and byte limits use binary units.

## Data pools and queues

`database.parallelism.read` and `database.parallelism.write` are hard worker
limits. Production values must each be at least three.

| Workload key | Default | Meaning |
| --- | ---: | --- |
| `latency-queue-capacity` | 4096 | Maximum queued LATENCY submissions per data pool |
| `ingest-queue-capacity` | 4096 | Maximum queued INGEST submissions per data pool |
| `cdc-queue-capacity` | 1024 | Maximum queued CDC submissions per data pool |
| `analytical-queue-capacity` | 512 | Maximum queued ANALYTICAL submissions in the read pool |
| `batch-queue-capacity` | 512 | Maximum queued BATCH submissions per data pool |
| `control-queue-capacity` | 256 | Maximum queued protected CONTROL submissions |
| `physical-maintenance-queue-capacity` | 16 | Maximum queued physical-maintenance submissions |
| `read-latency-reservation` | 1 | Borrowable LATENCY worker reservation in the read pool |
| `read-ingest-reservation` | 1 | Borrowable INGEST worker reservation in the read pool |
| `read-cdc-reservation` | 1 | Borrowable CDC worker reservation in the read pool |
| `write-latency-reservation` | 1 | Borrowable LATENCY worker reservation in the write pool |
| `write-ingest-reservation` | 1 | Borrowable INGEST worker reservation in the write pool |
| `write-cdc-reservation` | 1 | Borrowable CDC worker reservation in the write pool |
| `control-threads` | 2 | Workers in the isolated CONTROL pool |
| `physical-concurrency` | 1 | Workers in the isolated physical-maintenance pool |
| `analytical-active-limit` | 1 | Maximum active ANALYTICAL submissions in the read pool |

Every queue and worker capacity must be positive. Reservations must be
non-negative, must refer to a profile with a queue, and the reservation sum for
each pool must not exceed that pool's worker limit. ANALYTICAL has no write-side
operation family, so neither its queue nor its active limit is provisioned in the
write pool; its active limit must not exceed the read-pool worker limit.

The combined queue, worker, and worst-case outstanding bounds must fit the
scheduler's signed primitive counters. This is validated across each pool and
across the aggregate diagnostic views before any worker is created. Invalid values
fail configuration loading before the database opens rather than producing wrapped
negative telemetry later.

Reservations are borrowable: an idle reserved profile does not strand a worker.
When that profile has queued work, dispatch restores its guaranteed share.

## Dispatch policy

| Workload key | Default | Meaning |
| --- | ---: | --- |
| `latency-burst` | 8 | Maximum consecutive eligible LATENCY selections before guaranteed work is reconsidered |
| `ingest-drr-weight` | 4 | INGEST deficit-round-robin quantum |
| `cdc-drr-weight` | 4 | CDC deficit-round-robin quantum |
| `analytical-drr-weight` | 2 | ANALYTICAL deficit-round-robin quantum |
| `batch-drr-weight` | 1 | BATCH deficit-round-robin quantum |
| `competing-batch-read-maximum-active` | 4 | Read-side BATCH quantum cap while any non-BATCH request is queued or active |
| `competing-batch-write-maximum-active` | 1 | Write-side BATCH quantum cap while any non-BATCH request is queued or active |
| `competing-batch-write-interval` | `PT0.001S` | Minimum interval after a competing BATCH write quantum completes |
| `pressured-batch-maximum-active` | 1 | Maximum active BATCH quanta while storage is pressured |
| `pressured-batch-interval` | `PT1S` | Minimum idle interval after pressured BATCH completion |

Weights must be between 1 and 16. The latency burst and all BATCH maxima must be
positive. The competing read and write maxima cannot exceed their respective pool
capacity; the global pressured maximum cannot exceed the combined read/write capacity.
The competing caps are coordinated across the read and write pools. They preserve
the existing four-way raw-SST parallelism while preventing an otherwise idle write
pool from flooding RocksDB behind a saturated foreground read pool. They are removed
within one cooperative quantum after all non-BATCH work drains, so BATCH again
borrows every worker without mistaking ordinary client/server gaps for idle capacity.
The write interval is enforced only during that competing window; idle-only BATCH
writes are not paced.
The pressured interval must be a positive, representable duration and begins
after each pressured BATCH completion. Physical maintenance remains parked while
storage pressure is active.

Pressure fairness alternates only between data pools that can dispatch BATCH at that moment.
A peer with queued BATCH but every worker occupied by foreground work does not receive a turn and
cannot place the other pool into an indefinite wait. Worker and queue transitions publish
dispatchability while holding the owning pool lock; cross-pool wakeups are deferred until after
unlock. Once the peer has a free worker it becomes eligible for the next bounded fair turn.

## Retained analytical work

| Workload key | Default | Meaning |
| --- | ---: | --- |
| `retained-analytical-snapshots` | 1 | FIFO permit capacity for retained analytical snapshots |
| `retained-snapshot-maximum-age` | `PT60S` | Absolute maximum lifetime of a retained analytical snapshot or CDC WAL cursor |

The permit capacity must be positive; a zero maximum age is already expired. For
retained analytical work, the age is combined with the request and
transport/operation deadline, and the earliest deadline wins.

## Bounded work

| Workload key | Default | Meaning |
| --- | ---: | --- |
| `range-quantum-max-items` | 4096 | Maximum logical keys in one range/count quantum |
| `range-quantum-max-bytes` | `8MiB` | Maximum encoded key/value bytes in one range/count quantum |
| `range-quantum-max-duration` | `PT0.008S` | Maximum monotonic runtime of one range/count quantum |
| `raw-scan-file-concurrency` | 4 | Maximum simultaneously subscribed SST readers for one unsharded raw scan |
| `raw-scan-readahead-bytes` | `8MiB` | Sequential readahead reserved by each active raw-SST reader |
| `cdc-quantum-max-mutations` | 4096 | Maximum mutations parsed in one CDC quantum |
| `cdc-quantum-max-bytes` | `8MiB` | Maximum decoded WAL key/value bytes scanned in one CDC quantum |
| `cdc-quantum-max-duration` | `PT0.008S` | Maximum monotonic runtime of one CDC quantum |
| `latency-range-max-items` | 4096 | Server maximum items accepted for a LATENCY range page |
| `latency-range-max-bytes` | `8MiB` | Server maximum bytes accepted for a LATENCY range page |
| `latency-fan-out-max-items` | 256 | Server maximum items accepted for LATENCY fixed multi/fan-out commands |
| `latency-fan-out-max-bytes` | `2MiB` | Server maximum encoded input for LATENCY point mutations and fixed multi/fan-out commands |

Every item, mutation, and byte limit must be positive. Raw-scan concurrency must
be between 1 and 64, and raw-scan readahead must be positive. Quantum durations
must be positive and representable. A quantum stops at the first key, byte, or elapsed
time bound it reaches; continuations reacquire workload admission. Every bounded
range page is rejected if its explicit `RangeBudget` exceeds the public
4096-item/8-MiB ceiling, regardless of profile. LATENCY pages are also rejected
when they exceed `latency-range-max-items` or `latency-range-max-bytes`; those
settings may lower but never raise the public ceiling. Rockserver rejects
over-budget requests rather than silently clamping them. The 256-item/2-MiB
fan-out ceiling remains specific to LATENCY point and fixed multi commands.

Raw SST scans are work-conserving BATCH work. Each active SST keeps one reusable
cooperative scheduler task, iterator, readahead configuration, and partial wire buffer.
With no competing work in any scheduler pool it keeps producing the existing full 2 MiB/65,536-entry
wire batches without a duration cap or scheduler delay. Once any non-BATCH submission
is queued or active in any pool, the scan observes the same `range-quantum-max-duration` bound,
yields after the current indivisible RocksDB iterator call, and later resumes the same
task and partial batch. `raw-scan-file-concurrency` controls only unsharded scans; actual
simultaneous execution is also bounded by READ-pool admission. Higher concurrency and
readahead increase per-scan native reader and I/O memory pressure, so tune them together
on representative storage. Sharded scans retain their ordered two-item prefetch behavior.

Cooperative queue-wait and execution timers are published once per logical SST task at
terminal completion. Queue wait remains admission-to-first-dispatch latency; it does not
misclassify downstream backpressure or later cooperative redispatch as initial queueing.
Execution is total active time, and `rockserver.workload.quantums` retains the exact number
of scheduler quanta. Registry recording is therefore outside repeated cooperative yields.

CDC captures one conservative completed-mutation tail, then publishes the application
WAL through `CDC + FLUSH` before opening the logical poll. WAL parsing,
filtered-empty progress, latest-value resolution,
and every continuation remain in the CDC profile. An indivisible mutation larger than
the byte quantum is processed alone to guarantee cursor progress. The database-level
CDC lag gauge is the maximum across durable subscriptions, using each subscription's
furthest observed or durably committed cursor; it does not create per-subscription
meters.

The gRPC adapter validates and buffers a LATENCY fixed multi-operation within
the configured fan-out bounds before executing its first mutation. INGEST and
BATCH multi-operations remain streamed and are not constrained by LATENCY limits.

Tune only with the seven-profile hardware harness. Keep the smallest candidate
within the throughput/latency acceptance envelope, then verify it and its
adjacent candidates on the target storage class. CI results alone are not
hardware acceptance and must not be used to retune these defaults.
