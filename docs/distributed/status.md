# Distributed Rockserver implementation status

This is an unreleased implementation on `codex/distributed-rockserver`. There is
one current encoding for each wire message and stored artifact. Development
checkpoints are not releases: change the current layout directly, without migration
readers, legacy query modes or mixed-version activation machinery. Semantic command
kinds, schema/configuration epochs and recovery phases remain correctness inputs.
Existing standalone operation and pinned third-party contracts remain separate.

The full target remains [architecture.md](architecture.md), with the identified
consumer behavior in [consumer-contracts.md](consumer-contracts.md). This checkpoint
does not establish complete distributed API parity or production acceptance.

## Implemented paths

- A few shared native storage pools host multiple range replicas. Per-group ordered
  apply installs effects, receipt/accounting metadata and completed A in one
  WAL-enabled `sync=false` batch. Ratis's durable log remains recovery authority;
  C, A and portable snapshot D are distinct watermarks.
- The canonical command envelope has five semantic kinds: DATA, SESSION_FLOOR,
  SESSION_CONTROL, COMMIT_ACCEPTED and COMMIT_REJECTED. Raw and bucket-member
  mutations share DATA. Ordered merge operands are retained. All commands use the
  same header and current decoder; receipts normally use the command hash.
- COMMIT_ACCEPTED carries complete physical scope, native snapshot/term/generation/
  owner audit and frozen successful result. The leader validates/reserves before
  submission; followers apply the canonical plan without ephemeral history or live
  predicate validation. Native result disagreement fails before user effects.
- COMMIT_REJECTED is a compact internal no-effects operation carrying the original
  accepted-command receipt digest. Missing/expired local authority can be resolved
  through consensus without rebasing the plan or abandoning an unknown outcome.
  Existing receipts and permanent expiry fences take precedence. An earlier
  successful retained entry therefore wins over a later rejection. Clients cannot
  directly submit internal rejections.
- Admission freezes one log form for every duplicate owner. Uncertain entry-bearing
  cancellation retains its proposal, receipt and exclusive-scope ownership, even
  with zero RPC waiters. Apply, verified recovered outcomes or a strictly newer
  ready/native-completed term settles it. Same-term generation changes alone do not.
- Indexed proposal footprints preserve ordinary overlapping-write pipelining and
  permit disjoint transaction plans to overlap. Complete exclusive scope guards
  block only intersecting user proposals. Session ACK/floor/authority metadata is
  conservatively excluded while candidates hold certification; an ACK-bearing
  transaction requires sole metadata ownership.
- Native scoped staging supplies an immutable read-your-writes prefix for point
  puts/deletes, finite physical range deletes, ordered native merge operands and
  collision-bucket member mutations.
  Point/member reads and forward/reverse physical pages combine its indexed batch
  with the original native snapshot. It installs no user effects or receipts and
  does not confer commit authority. Failed predicates/shape/result bounds discard
  the view. Its native use remains charged until close, even after parent
  certification invalidation. The current framed STAGE operation exposes these
  prefixes through routed scoped handles; GET/PAGE use the staged view. Identical
  retries return the cached result, extensions retain all earlier operands and
  predicates, and delayed/regressing prefixes cannot replace newer ones. Failed
  extensions preserve the previous view. PLAN requires the exact final business
  prefix, releases staging and accepts it under the real durable operation ID.
  Staging itself uses the ephemeral handle nonce and never appends a command or ACK.
  Range tombstones retain command order in a bounded index over the plan's own
  points/ranges. They erase earlier staged points and the snapshot base; later
  PUT/MERGE/member updates can resurrect a row. Page iteration seeks over deleted
  intervals while stopping at later staged points, in both directions, without
  enumerating the erased keyspace. Whole physical bucket rows obey range deletion;
  member updates preceding a later range cannot reappear during native apply.
  Canonical accepted records retain original range deletes and merge operands.
  Complete logical/frontend range and cursor adapters remain incomplete.
- Durable logical sessions now expose one-group scoped transactions over cached
  logical column IDs/keys. Logical GET decodes complete collision buckets; STAGE
  preserves ordered PUT/DELETE prefixes and original-image conditions, including
  key-only presence. COMMIT freezes one durable unaccepted intent, installs binding,
  obtains PLAN, forces accepted bytes and waits for applied execution. Equal retries
  retain that identity; differing commit plans fail before another allocation.
  Successful binding is remembered for the commit attempt so a lost PLAN reply
  cannot introduce a new metadata write behind its existing reservation. Unaccepted
  PLAN failures remain recoverable intents that may be explicitly abandoned and
  acknowledged; accepted entries retain ordinary exact retry after session reopen.
  Handles must close before the owning session releases its root lock. Read-only
  handles allocate no durable operation identity.
- Durable logical transactions expose bounded logical pages and borrowed cursors.
  Physical-key order is followed by unsigned original-member component order inside
  each collision bucket, in either direction. Full logical `resumeAfter` is exclusive,
  including absent/intermediate member keys. Physical rows are decoded completely;
  logical pages may end within a bucket and continue at its exact member key.
  Cursors copy bounds/continuations, borrow the same transaction cut/staging view and
  fail after owner loss or parent close. Cursor close owns no independent native pin.
  Logical null bounds cannot silently clip at a shard boundary: requests crossing
  the one participant are rejected pending the multi-group snapshot-fence path.
- Bounded mutation history detects ABA, same-value writes, absence deletes and range
  phantoms. Physical collision-bucket members share one stamp. Range deletion
  conservatively invalidates tracked observations. Exact native snapshots retain
  old values; there is no version CF over every stored user key.
- Scoped OPEN/GET/PAGE/PLAN/CLOSE use one framed query protocol for data and catalogue
  groups. OPEN requires a proven fresh linearizable barrier and ready native owner.
  Later owned operations use the fixed snapshot cut, not a new current read. Stale
  read dispatch rejects transaction-handle operations.
- OPEN retries identify the active opening by nonce; each new opening gets a fresh
  server lease ID. Stale reads/CLOSE cannot access a replacement, including when an
  expired nonce is reused at the same native position. Handles remain bound to
  incarnation, native term and leadership generation.
- TTL is absolute and monotonic. First write admission checks its sealed deadline
  independently of cleanup scheduling. Successful planning deliberately closes the
  native cut; an explicit planning phase protects publication against expiry races.
  A mismatched PLAN retry preserves the original frozen plan.
- One node-shared expiry service visits bounded group quanta on the existing reserved
  CONTROL lane. Timer callbacks only admit work; queued stop removes admission and
  running cleanup drains before machine/pool shutdown. Retired JNI/planning work
  retains its physical pin and payload charges until it actually returns.
- User session windows durably distinguish unaccepted intents, accepted commands and
  abandoned unaccepted intents. Base intents cannot execute or ACK. Acceptance
  verifies the exact original identity, route, ACK and ordered operands and forces
  accepted bytes before the first write RPC. Lost replies retry only those bytes.
  Unaccepted intents may be durably abandoned as local no-effects CONFLICTs, then
  explicitly consumed/ACKed; accepted entries cannot use that path.
- Durable grant issuance, logical-session handoff, bounded user/maintenance windows,
  catalogue clocks/propagation, immutable schema publication and static node plans
  have local recovery fixtures. The owned node launcher checks established storage,
  placement identity and resource ownership. INITIAL/PREPARING/ACTIVE and install
  phases are crash-recovery states, not format releases.
- Portable native images carry an exact completed cut, schema/ownership identity,
  user rows, retained receipts and session metadata. Current total accounting is
  exactly 36 bytes bound to A; per-session bucket accounting is a distinct 20-byte
  record. Missing/stale/malformed established totals are rejected, never silently
  upgraded or rebuilt. Genuine new empty ranges initialize zero accounting.
  Open/import validation checks accounting read-only, with bounded working memory.
  Interrupted snapshot installation retains its fencing/recovery ownership.

## Current bounds

Atomic commands have at most 256 mutations/predicates and 2 MiB encoded bytes,
including accepted scope/audit/result. Declared scopes have at most 256 spans and
2 MiB bound bytes. Physical pages retain the existing 4,096-item/8 MiB bound; query
framing allows a legal physical continuation larger than an atomic command.

Default history retention bounds 128 pins, 65,536 entries, 16 MiB history and
16 MiB scope accounting. Default proposal ownership bounds 1,024 ordinary owners,
64 candidates, 16,384 normalized spans and 16 MiB footprint accounting. Preparing
and retained command payloads share a separate 16 MiB bound; planners reserve the
maximum command size before constructing output. Original requests retain normal
input-slot/byte charges even when their chosen log form is a compact rejection.
A native staging prefix retains the bounded original command and result plus an
indexed native batch capped at 22 MiB (16 MiB bucket-plan allowance plus three
2 MiB command allowances for indexed effects, resets and hidden-point deletion). Construction shares the apply evaluator's bucket working bound.
The registry additionally reserves 64 MiB before constructing a replacement while
keeping the prior view charged. On success it shrinks to accounted original operands,
raw indexed effects, tombstone/point index, materialized buckets, result and bounded
entry overhead, with a 32 MiB maximum per retained view. The default aggregate staging allowance is 128 MiB
per group registry. Tiny prefixes retain their small accounted size rather than a
32 MiB fixed charge. Busy expired/closed handles retain both old-view and construction
charges until the actual native return and cleanup. These are payload/accounting
bounds, not measured heap/native allocator limits or global node/pool envelopes.
Rejected receipts reserve their actual bounded no-effects growth, preserving
own-ACK progress at the retention cap.

Logical output pages enforce 4,096 items and 8 MiB of original logical key/value
bytes, matching standalone accounting. Translation fetches at most 128 complete
physical rows/8 MiB per subrequest, with a total work quantum of 4,096 physical rows
and 16 MiB of physical pages per logical request. If that quantum cannot establish
an actual logical continuation, it fails explicitly instead of emitting an empty
continuing page or claiming end-of-data. This is a bounded one-group page adapter,
not the final retained cooperative stream/export implementation.

The handle registry defaults to 128 handles, 16 MiB scope accounting, 16 MiB retained
plans and a maximum 60-second TTL. A durable logical session additionally bounds its
open handles by `min(128, outstandingWindow)` (32 by default); opening calls and
failed/closed handles retain/release that admission explicitly. Logical expiry cannot
donate capacity still owned by blocked capture/planning/read work. Expiry defaults to 32 groups per
100 ms quantum; cleanup may lag under failure/queue pressure, while access rejects
expired ownership immediately. These are fixed accounting bounds, not measured
heap/SST residency or end-to-end transport-buffer bounds.

## Validation and evidence

Verification uses ordinary `test-compile surefire:test@default-test` selections,
not root `mvn test` (which binds fuzzing). Native fixtures use the XFS test-directory
factories. The excluded density fixture remains untouched and unselected.

The logical page/cursor regression gate passed 16 core and 89 adapter cases
(105 total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-logical-pages-regression.log`. After explicitly rejecting unbounded
logical requests that cross the participant boundary, the final logical/session/
staging selection passed 19 adapter cases, exit zero:
`/tmp/rockserver-logical-pages-final.log`. Native range/apply source is unchanged from
the preceding 126-case range-staging gate and its final 24-case selection.

The exercised native/controlled-dispatch workflows include OPEN/read/STAGE/PLAN,
forced acceptance before submission, applied native outcomes, lost PLAN/commit
replies, close/reopen, exact receipt retry and explicit ACK. They cover ordered
point/range deletes and native merges, overlapping tombstones, collision buckets,
key-only presence, forward/reverse continuation and inclusive-lower resurrection,
skipping erased oversized rows, result-overflow atomicity, immutable scope/prefix
checks, footprint ownership, history conflicts, strict images/install recovery and
session journal phases. Logical page fixtures additionally cover byte/item limits,
collision-member insertion order versus comparator order, continuation within a
bucket in both directions, empty physical rows, absent member continuations, key-only
presence, copied cursor buffers/bounds, fixed-cut reads after concurrent writes,
parent close and leader loss. The empty-row scan-quantum fixture uses synthetic
physical replies to isolate client work accounting; ordinary page fixtures use
native stores and controlled Ratis dispatch. A two-shard fixture verifies that an
unbounded request cannot report the first shard as the complete result.
Unaccepted intents can be reopened and explicitly abandoned;
missing accepted authority resolves through internal rejection. Logical-root/handle
and in-flight staging retirement fixtures verify that active ownership stays charged
until actual work returns. Native fixtures reported io_uring allocation warnings on
the test host; every selected case completed.

The patched Ratis producer remains based on released 3.3.1 with checksum-pinned
binary inputs. Its fresh-query dispatch hook is selected only after successful
linearizable read-index and wait-applied barriers; generic/non-linearizable/stale
paths remain distinct. The vendored upstream `RaftServerImpl` source has SHA-256
`a1b8cb4b41c8366985a94a2973f2b8926ffc0646556bdce9d135bf5f7a904b78`.
The packaged server jar still has SHA-256
`b7226ca8e54c05b157325f5c47b0fb0a4943d9389cbd8a1544941b2a6b21ad36`;
previous artifact verification established 159 unreplaced released classes were
byte-identical and ZIP entries unique. Producer source/inputs are unchanged by the
staging work; the package checksum was rechecked.

Controlled barrier/query/append fixtures prove only their exercised paths. No real
server process, network/quorum probe, power-loss run or deployment was performed.

## Required remaining work

- Complete public RocksDB API and transport frontend transaction/range/cursor parity,
  including consistent cross-group/unbounded pages, typed pin-loss errors and retained
  cooperative streams. The current logical page adapter is one-group and transaction-bound.
- Durable prepared locks, transaction-status groups, 2PC decision/finalization and
  consistent multi-group scoped snapshot fences.
- Topology controllers, lineage-aware routing/receipts, relocation, split/merge,
  placement authority and real operator-artifact activation.
- Complete bounded bulk/import, CDC, export/backup/restore and operational repair
  workflows; global node/pool/tenant resource envelopes.
- Representative performance/resource acceptance, independent quorum/restart/fault
  evidence and integration with the user's chosen final checkout.

Standalone remains supported. This branch has not been pushed, published, merged
or deployed.
