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
  puts/deletes, ordered native merge operands and collision-bucket member mutations.
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
  Logical transaction adapters and range-delete staging remain incomplete.
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
indexed native batch capped at 18 MiB (16 MiB bucket-plan allowance plus 2 MiB
command allowance). Construction shares the apply evaluator's bucket working bound.
The registry additionally reserves 64 MiB before constructing a replacement while
keeping the prior view charged. On success it shrinks to accounted original operands,
raw indexed effects, materialized buckets, result and bounded entry overhead, with a
24 MiB maximum per retained view. The default aggregate staging allowance is 128 MiB
per group registry. Tiny prefixes retain their small accounted size rather than a
24 MiB fixed charge. Busy expired/closed handles retain both old-view and construction
charges until the actual native return and cleanup. These are payload/accounting
bounds, not measured heap/native allocator limits or global node/pool envelopes.
Rejected receipts reserve their actual bounded no-effects growth, preserving
own-ACK progress at the retention cap.

The handle registry defaults to 128 handles, 16 MiB scope accounting, 16 MiB retained
plans and a maximum 60-second TTL. Logical expiry cannot donate capacity still
owned by blocked capture/planning/read work. Expiry defaults to 32 groups per
100 ms quantum; cleanup may lag under failure/queue pressure, while access rejects
expired ownership immediately. These are fixed accounting bounds, not measured
heap/SST residency or end-to-end transport-buffer bounds.

## Validation and evidence

All verification uses ordinary `test-compile surefire:test@default-test` selections,
not root `mvn test` (which binds fuzzing). Native fixtures use the XFS test-directory
factories. The excluded density fixture remains untouched and unselected.

The current-format focused gate passed 40 core and 82 adapter cases (122 total):
`/tmp/rockserver-current-format-focused-green.log`. It covers current semantic kinds,
strict encoding bounds, image/accounting corruption rejection, install recovery,
query framing, node identity, durable phases and accepted/rejected outcomes.

The broad selected gate then passed 137 core, 24 patched-server and 203 adapter
cases (364 total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-current-format-final-gate.log`. A final native/bucket/rejection
selection after removing obsolete format labels passed 14 core and 20 adapter
cases (34 total), exit zero: `/tmp/rockserver-current-format-native-final.log`.

The native staging regression gate passed 39 core and 31 adapter cases (70 total,
zero failures/errors/skips), exit zero: `/tmp/rockserver-native-staging-regression.log`.
It covers immutable-prefix replacement, snapshot isolation, staged forward/reverse
pages and limits, bucket collisions, ordered native merges, original-operand/result
parity through accepted native apply, rejection cleanup, pin retention, image/install
and existing scoped transaction RPC behavior. The five new staging cases exercise
the native primitive, not a completed public transaction adapter.

The staging RPC regression gate passed 39 core and 76 adapter cases (115 total,
zero failures/errors/skips), exit zero: `/tmp/rockserver-staging-rpc-regression.log`.
After adding strict staging-result cardinality validation, the final scoped/RPC/
bucket selection passed 33 adapter cases, exit zero:
`/tmp/rockserver-staging-rpc-final.log`. It covers lost staging replies, equal and
extended prefixes, delayed-prefix rejection, failed-extension preservation, staged
forward/reverse pages, exact final PLAN matching, forced acceptance/lost commit reply/
reopen/retry, logical collision buckets, aggregate admission, and expiry during native
replacement construction. No real server process or quorum was used. Native fixtures
reported an io_uring allocation warning on the test host; all selected cases completed.

The end-to-end local workflow includes native OPEN/read/PLAN, forced acceptance,
controlled append/native apply, lost reply, close/reopen, exact receipt retry and
ACK. It also covers failed PLAN followed by durable local abandonment, and missing
accepted authority resolved through an internal rejection. Ordering fixtures cover
reversed duplicate owners, original-versus-rejection precedence, same-term ambiguity,
receipt-cap ACK progress, snapshot-covered deferred resolution, deadline enforcement,
fixed-cut pages and stale-close replacement protection.

The patched Ratis producer remains based on released 3.3.1 with checksum-pinned
binary inputs. Its fresh-query dispatch hook is selected only after successful
linearizable read-index and wait-applied barriers; generic/non-linearizable/stale
paths remain distinct. The vendored upstream `RaftServerImpl` source has SHA-256
`a1b8cb4b41c8366985a94a2973f2b8926ffc0646556bdce9d135bf5f7a904b78`.
The packaged server jar has SHA-256
`b7226ca8e54c05b157325f5c47b0fb0a4943d9389cbd8a1544941b2a6b21ad36`;
159 unreplaced released classes were byte-identical and ZIP entries unique.
Producer source/inputs are unchanged by the current command-format consolidation.

Controlled barrier/query/append fixtures prove only their exercised paths. No real
server process, network/quorum probe, power-loss run or deployment was performed.

## Required remaining work

- Logical transaction adapters and range-delete staging; complete public
  RocksDB API/frontend transaction, range and cursor parity.
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
