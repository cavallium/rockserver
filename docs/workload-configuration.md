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
| `analytical-queue-capacity` | 512 | Maximum queued ANALYTICAL submissions per data pool |
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
| `analytical-active-limit` | 1 | Maximum active ANALYTICAL submissions in either data pool |

Every queue and worker capacity must be positive. Reservations must be
non-negative, must refer to a profile with a queue, and the reservation sum for
each pool must not exceed that pool's worker limit. The analytical active limit
must not exceed either data-pool limit. Invalid values fail configuration loading
before the database opens.

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
| `pressured-batch-maximum-active` | 1 | Maximum active BATCH quanta while storage is pressured |
| `pressured-batch-interval` | `PT1S` | Minimum idle interval after pressured BATCH completion |

Weights must be between 1 and 16. The latency burst and pressured BATCH maximum
must be positive; the maximum cannot exceed the combined read/write worker
capacity. The pressured interval must be a positive, representable duration and
begins after each pressured BATCH completion. Physical maintenance remains
parked while storage pressure is active.

## Retained analytical work

| Workload key | Default | Meaning |
| --- | ---: | --- |
| `retained-analytical-snapshots` | 1 | FIFO permit capacity for retained analytical snapshots |
| `retained-snapshot-maximum-age` | `PT60S` | Absolute maximum lifetime of a retained snapshot |

The permit capacity and duration must be positive. The age is combined with the
request and transport/operation deadline; the earliest deadline wins.

## Bounded work

| Workload key | Default | Meaning |
| --- | ---: | --- |
| `range-quantum-max-items` | 4096 | Maximum logical keys in one range/count quantum |
| `range-quantum-max-bytes` | `8MiB` | Maximum encoded key/value bytes in one range/count quantum |
| `range-quantum-max-duration` | `PT0.008S` | Maximum monotonic runtime of one range/count quantum |
| `cdc-quantum-max-mutations` | 4096 | Maximum mutations parsed in one CDC quantum |
| `cdc-quantum-max-bytes` | `8MiB` | Maximum encoded CDC bytes in one quantum |
| `cdc-quantum-max-duration` | `PT0.008S` | Maximum monotonic runtime of one CDC quantum |
| `latency-range-max-items` | 4096 | Server maximum items accepted for a LATENCY range page |
| `latency-range-max-bytes` | `8MiB` | Server maximum bytes accepted for a LATENCY range page |
| `latency-fan-out-max-items` | 256 | Server maximum items accepted for LATENCY fixed multi/fan-out commands |
| `latency-fan-out-max-bytes` | `2MiB` | Server maximum encoded input for LATENCY point mutations and fixed multi/fan-out commands |

Every item, mutation, and byte limit must be positive. Quantum durations must be
positive and representable. A quantum stops at the first key, byte, or elapsed
time bound it reaches; continuations reacquire workload admission. Configured
LATENCY maxima may be lowered but cannot exceed the public 4096-item/8-MiB range
or 256-item/2-MiB fan-out contract ceilings.

Tune only with the seven-profile hardware harness. Keep the smallest candidate
within the throughput/latency acceptance envelope, then verify it and its
adjacent candidates on the target storage class. CI results alone are not
hardware acceptance and must not be used to retune these defaults.
