# Rockserver workload profiles

Every generic Rockserver request carries a `RequestContext`. Its `WorkloadProfile`
describes the service guarantee the caller needs. It does not describe the database
operation or its cost: Rockserver derives and records the `OperationFamily` independently,
then rejects an invalid pair before admission.

Workload contract v3 stores a relative `timeoutNanos`, not an epoch timestamp. A reusable
API view is a timeout policy: unary calls bind it at invocation, cold streams bind it at
subscription, and client queueing consumes the resulting immutable monotonic budget.
Transports send only the remaining nanoseconds; each server hop binds that remainder once
and takes the minimum with its transport deadline. Retained continuations reuse the same
bound deadline. Missing or non-v3 wire contexts are rejected, and protobuf field 2 remains
reserved so a v2 epoch deadline cannot be reinterpreted as a duration.

The request context is the single execution deadline for range reduction, bounded pages,
streaming ranges, and multi-existence reads. Those APIs no longer accept a second
`timeoutMs`. Transaction and iterator lifetimes are separate positive `Duration` lease
TTLs; they do not affect EDF ordering or extend an operation deadline.

Only `LATENCY`, `ANALYTICAL`, `INGEST`, and `BATCH` are caller-selectable. `CONTROL`,
`CDC`, and `PHYSICAL_MAINTENANCE` are assigned by Rockserver and are rejected when they
arrive from a client.

## Canonical matrix

| Profile | Selection rule | Guarantee | Examples | Must not contain |
| --- | --- | --- | --- | --- |
| `CONTROL` | Short server-owned work required to release resources or preserve liveness | Isolated bounded capacity; never blocked by data work | Iterator close, rollback, cancellation, shutdown leases | User queries, scans, bulk mutations |
| `LATENCY` | Request-bound operation expected to finish inside the normal API latency budget | Fast admission or immediate overload | Point lookup, first/last seek, bounded multi-get, metadata estimate | Full counts, unbounded ranges, backfills |
| `ANALYTICAL` | Caller waits for an intrinsically expensive query | Capped but guaranteed progress; caller cancellation stops it | Exact count, full aggregation, operator-requested statistics | Periodic metrics, migrations, unattended jobs |
| `INGEST` | Sustained live pipeline whose lag affects primary freshness or durability | Reserved progress and bounded backlog | Crawler writes, transactional updates, live monitor persistence | Historical replay, cleanup, ad-hoc statistics |
| `CDC` | Any stage belonging to WAL discovery, publication, polling, or acknowledgement | Independent lag SLO and reserved progress across all stages | CDC poll pages, cursor resume, CDC commit, required internal WAL flush | Ordinary scans or generic maintenance |
| `BATCH` | Retryable or deferrable work with no waiting caller | Uses spare capacity, pauses under pressure, eventually progresses | Backfill, migration, cleanup, scheduled exact statistics | Live ingestion or synchronous operator queries |
| `PHYSICAL_MAINTENANCE` | Explicit physical database servicing | Serialized and admitted only when storage policy permits | Manual flush and compaction | CDC-required internal flush, logical cleanup |

Heavy exact counts are `ANALYTICAL` when a user or operator waits for the result and
`BATCH` when periodically calculated, exported, or otherwise retryable. A count is
`LATENCY` only when it uses a bounded metadata estimate such as `estimateNumKeys`.

## Concrete command validation

The family matrix is necessary but not sufficient. Before admission, Rockserver also
validates the concrete command and rejects these invalid caller/profile combinations:

- `ScanRaw` is `BATCH` only.
- Streaming `GetRange` is `ANALYTICAL` or `BATCH` only. `LATENCY` and `INGEST`
  callers must use the bounded page API.
- `PutBatch`, `MergeBatch`, and `DeleteRange` are `INGEST` or `BATCH` only.
- Column creation/deletion and merge-operator upload are administrative mutations and
  are `BATCH` only.
- `LATENCY` mutations are limited to `Put`, `Delete`, `Merge`, and their fixed
  `*Multi` forms. A fixed multi-operation contains at most 256 items, and every
  point or multi-operation contains at most 2 MiB of encoded input. Encoded input is
  the sum of logical key-component bytes plus mutation value bytes; response size is
  not part of this admission bound. Transaction commit is not a `LATENCY` point
  mutation.
- `LATENCY ExistsMulti` uses the same 256-item and 2-MiB encoded-key ceilings.
- A `LATENCY` iterator `Subsequent` call has `skip + take <= 4096`.
- Every `GetRangePage` request has an explicit positive `RangeBudget` no larger
  than 4096 items and 8 MiB for every profile. LATENCY must additionally fit the
  configured `latency-range-max-items` and `latency-range-max-bytes`; exceeding
  either limit is rejected rather than clamped.
- Exact range aggregates are never accepted as `LATENCY` or `INGEST` work.

All thresholds are inclusive. Commands one byte or item above a threshold are rejected
before scheduler admission. Point, fixed-multi, and iterator-advance limits apply only
to `LATENCY`; the public range-page ceiling applies to every profile. Other profiles
retain their family/command rules and are also bounded by their queue and quantum
contracts.

Rollback, failed-update close, iterator close, CDC lifecycle/poll/acknowledgement, and
physical flush/compaction remain server-owned. Their protected profile is derived from
the concrete command and cannot be changed by the caller's view context. Their transport
messages still carry the exact workload-contract version and reject v2 or missing values.
CDC creation also requires an explicit atomic precondition: metadata must either be absent
or have the supplied durable checkpoint. There is no unchecked create mode.

## Decision tree

Apply these rules in order:

1. CDC lifecycle or WAL work -> `CDC`.
2. Resource release, rollback, or cancellation -> `CONTROL`.
3. Explicit manual flush or compaction -> `PHYSICAL_MAINTENANCE`.
4. Live primary or derived pipeline -> `INGEST`.
5. Bounded request expected within the normal API deadline -> `LATENCY`.
6. Expensive operation with a waiting caller -> `ANALYTICAL`.
7. Retryable or unattended work -> `BATCH`.
8. If none applies, the call must not be merged until its service contract is defined.

## Server-derived operation families

Rockserver derives one of these independently from the client profile:

- `METADATA`
- `POINT_LOOKUP`
- `BOUNDARY_SEEK`
- `BOUNDED_FAN_OUT`
- `RANGE_PAGE`
- `FULL_SCAN_AGGREGATE`
- `WAL_PAGE`
- `MUTATION`
- `CONTROL`
- `FLUSH`
- `COMPACTION`

The authoritative compatibility matrix is `WorkloadAdmission`. It is deliberately
stricter than a priority hint. For example, `LATENCY + FULL_SCAN_AGGREGATE` is rejected;
an exact count cannot silently enter the chat-extremes lane. CDC-required internal flush
is `CDC + FLUSH`, while an explicit manual flush is
`PHYSICAL_MAINTENANCE + FLUSH`.

## Physical scheduler-pool routing

Profile validation and physical resource routing are separate, exhaustive decisions.
After `WorkloadAdmission` accepts a profile/family pair, `RWScheduler.resourcePool`
maps it exactly once:

| Accepted work | Pool |
| --- | --- |
| `CONTROL` | isolated control pool |
| `PHYSICAL_MAINTENANCE` | isolated physical pool |
| non-protected `MUTATION`, plus CDC-owned `FLUSH` | write pool |
| `METADATA`, `POINT_LOOKUP`, `BOUNDARY_SEEK`, `BOUNDED_FAN_OUT`, `RANGE_PAGE`, `FULL_SCAN_AGGREGATE`, `WAL_PAGE` | read pool |

`ANALYTICAL` currently has only read-side families. Its queue capacity and active limit
therefore exist only in the read pool; a small write pool must not lower the configured
analytical read concurrency or create dead write-side gauges. LATENCY, INGEST, CDC, and
BATCH have both read- and write-side families, so their configured queue capacity applies
independently to each pool.

Configuration validation rejects any combination whose aggregate queue, outstanding, or
worker bounds cannot fit the scheduler's signed primitive counters. Runtime aggregate
accessors use exact arithmetic as a second line of defense and fail loudly instead of
publishing wrapped negative telemetry.

The high-frequency pool telemetry API copies into a caller-owned primitive array. Its
scalar region includes worker/utilization and exact conservation counters; named profile
offsets then contain queued and active counts. Immutable pool snapshots remain authoritative
for per-outcome, parked, outstanding, drain, and conservation diagnosis. Gauges are created
only for profile/pool pairs that can actually route work, keeping permanent metric cardinality
bounded. Profile/family timers and counters must likewise use the single physical route above.

## Reactor scheduling and terminal ownership

The Reactor adapter retains two identities for each indexed submission: the original task owns
cost and terminal callbacks, while the `Schedulers.onSchedule` result owns execution context.
Instrumentation hooks may wrap the runnable but cannot erase `EstimatedWork` byte cost or redirect
`RejectionAwareTask` failures. Immediate overload and queued deadline, cancellation, or shutdown
are delegated to the original task exactly once, outside the executor lock, before generic disposal.
Cancellation that loses the dispatch claim cannot rewrite running ownership.

Workers created from an indexed scheduler are children of that scheduler. Disposing the parent
atomically rejects new workers and new worker submissions, disposes every registered worker, and
removes its queued tasks through the same indexed cancellation path. A submission that linearizes
before parent disposal may run or be cancelled; one attempted after disposal is never admitted.

The LATENCY burst counter is a bounded state, not a lifetime completion counter. It saturates at
the configured burst limit and resets when guaranteed work is selected, so multi-year LATENCY-only
uptime cannot wrap the counter and starve newly arriving INGEST, CDC, ANALYTICAL, or BATCH work.
