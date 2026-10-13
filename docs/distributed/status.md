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

## Current acceptance (2026-10-13)

Earlier checkpoints below retain the limitations known at their validation time.
This section and the remaining gates at the end describe the current branch.

- The aggregate common connection routes the complete database domain roster,
  retains schema generations and uses durable point/bulk grants and rotating
  transaction allocations. The actual native two-domain factory workflow passed
  point/bulk writes, owned close and exact cold reopen. Factory-issued allocation
  also passed real snapshot capture, a cross-column transaction spanning two DATA
  groups, pin cleanup, connection-journal reopening without replacement grants and
  a subsequent captured abort; native stores stayed live in that reopen check.
  Launcher ownership tests
  passed with controlled listeners; real gRPC/Thrift listeners were not started.
- The fresh-cluster restore controller passed actual native import, catalogue
  activation, logical qualification and restoration without the original source
  directories, including sparse interleaved API IDs and an empty column across
  two domains. Independently archived operator bytes are staged and execute a real
  MERGE after restore; cold target reopening retains that value and requalifies
  serving routes. An actual native 65-group restore also passed two import batches
  (64+1), data in the second batch and qualification of every serving owner.
- The owned backup factory passed actual native catalogue grant issuance and
  HOME/DATA bindings, whole-cohort capture/export, completion, resource drain and
  same-root reopening. Gone-owner cleanup relies on the exclusive established
  node root and exact previous process identity.
- Fifteen native range-delete cases passed fixed bounds, shard clipping, collision
  members, repeated use, unknown live/cold replies, original terminal outcomes,
  complete same-format sidecar recovery and cancellation during result delivery.
  Final source review confirmed the reported recovery/delivery blockers are repaired.
- Native pool pressure now feeds shared background admission. Fresh native
  sampling, stale/failing signal handling, actual sampler drain and node lifecycle
  checks passed. This does not establish a complete node/pool resource envelope.
- Current node settings and the manifest directly declare ordinary and protected
  node/pool byte and owner limits. Proposal input and scoped-handle leases pass
  controlled accounting checks; manifest validation and HOME lifecycle fixtures
  pass together. Native staging and participant preparation now retain shared
  charges through actual drain, including cancellation. Other custom-record holds
  and retained buffers still need charges; the full volatile envelope remains open.
- HOME USER status rows now reserve their complete terminal growth within 1,024
  rows and 16 MiB, independently of protected recovery credits. Consumed rows stay
  charged until retirement. Native checks passed racing registration rejection as
  a saved no-effects conflict, protected completion at full capacity, cold/image
  reconstruction and exact retry after an unknown native write. This bounds HOME
  status metadata; other catalogue and per-role durable ceilings remain open.
- Replica-local maintenance passed actual flush/SST preview/execution/property
  checks, stale target rejection, cancellation with retained JNI ownership and
  node-owned shutdown. It requires explicit policy and process/pool/CF identities.
  Common distributed SST commands remain unsupported. Java response limits do
  not establish a bound on RocksDB's eager inventory allocation.

All evidence above uses controlled native/Ratis interface fixtures. No sockets,
independent quorum, production deployment or representative hardware/density
acceptance is claimed.

## Implementation checkpoints

- The current node runtime uses one native storage pool for multiple range replicas. Per-group ordered
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
  A tracked gate capture registers that history and its native snapshot under one
  apply-lane cut. Read-only participants use COMMIT_ACCEPTED with the complete
  original scope and a zero-value SUCCESS result, without invented predicates or
  mutations. PREPARE certifies history before transferring the full scope to a
  durable read lock; COMMIT records success without user effects. Routed scoped
  handles authorize these participants only when no staged effects would be lost.
  Owner-directed WRITE_CERTIFIED capture reserves snapshot metadata and scoped
  handle capacity before taking that cut. The scoped registry is its sole native
  owner; the snapshot registry keeps the original token and refunds its charge only
  after scoped work actually drains. Capture retries bind mode, fence, owner, nonce
  and original TTL without another HOME read or snapshot. The reply retains the
  original scoped handle, so routed GET/PAGE/STAGE/PREPARE reuse that cut and deadline.
  Immutable snapshot operations reject writable tokens; immutable journals reject
  writable requests before saving or dispatching them. Leadership/history loss
  invalidates certification without opening a replacement cut.
  The durable snapshot journal now freezes READ_ONLY or WRITE_CERTIFIED before its
  first HOME write, forces a writable token and scoped handle together, and restores
  the same request, owner, cut and handle after reopen. Unknown-reply cleanup retains
  that mode when recovering its original capture. The immutable client rejects a
  writable journal before remote dispatch; immutable leases cannot be promoted.
  `RatisTransactionPlan.captured` requires COMPLETE and every local gate RELEASE
  proof, then carries every participant in the original read projection into 2PC.
  Groups without writes use explicit zero-effect certification. Acceptance retains
  the original handles and complete scopes, rejects writes outside that projection,
  and prevents changed owner/generation/cut audits. No replacement OPEN is used.
  `RatisTransactionClient` now supplies bounded asynchronous writable leases over
  those captures. Logical points, buckets, pages and borrowed cursors use scoped
  handles and their read-your-writes views. STAGE takes the complete equal/extended
  prefix, retains one uncertain request and its per-group acknowledgements, and
  blocks reads and commit until that exact prefix resolves. Publication follows
  final deadline checks; a new staged revision invalidates old cursors.
  Commit freezes all captured participants and result slots, then uses
  `RatisAtomicClient.executeAccepted` without another OPEN or STAGE. Snapshot and
  commit identity blocks and journal paths are disjoint. Actual work retains
  ownership after observer cancellation; cleanup uses the same bounded task views.
  Mutable prefixes are ephemeral: frontend loss permits cleanup-only reopening,
  while a forced commit journal permits exact atomic recovery. Completion does not
  consume the transaction result. Explicit release closes settled commit ownership
  before snapshot cleanup. Legacy API/transport parity remains unfinished.
- `RatisRocksDBAdapter.api(context)` binds the common async API to bounded explicit
  transaction handles. Unknown-scope OPEN validates the complete cached database
  projection before identity allocation. Typed GET/FOR_UPDATE, ordered PUT/DELETE
  and multi results, EXISTS, cached column metadata and transaction pages use the
  same captured lease and write overlay. A definite CONFLICT returns `false` while
  retaining its opaque legacy ID and outcome; repeated close never rebases or
  reallocates it. Unknown commits resume their original operation, including during
  adapter shutdown. Inputs and mutable results are copied, and concurrent operations
  cannot install a prefix built from stale settled state.
  `RatisTransactionIdentities` exclusively reserves disjoint snapshot/commit blocks
  in an already-issued, adequately bound grant. Its current manifest is forced on
  the caller's workload executor before any capture; admission and close checks do
  not wait on checkpoint I/O. Cancellation cannot recycle reserved identities, and
  missing used state cannot be recreated. Exhaustion requires a renewed exclusive
  grant rather than wrapping sequence IDs.
  Implicit FOR_UPDATE now creates a ten-second update attempt. UPDATE-ID writes
  commit internally; prior-value callbacks keep their standalone restrictions.
  A definite single-write conflict cleans the old attempt and retains the alias;
  only the caller's next FOR_UPDATE reread opens a fresh cut and identity. Multi
  conflicts close the alias. Unknown attempts retain the exact write and commit
  journal, including CloseFailedUpdate cleanup.
  The optional `RatisRocksDBDirect` path uses strong point reads and durable
  single-group commands without snapshot fences or 2PC. Nontransactional lists
  remain ordered individual operations. One current frontend journal freezes the
  full request, callback, cursor and raw results before session acknowledgement;
  reopen resumes the unresolved item without repeating acknowledged effects. Late
  deadline failures preserve results, and known no-effects outcomes cannot wedge
  the next request. Input/result/task reservations are bounded, while borrowed
  sessions and infrastructure remain caller-owned.
  Advanced operations and full-capability `RocksDBConnection`/server transport
  registration remain unfinished.
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
- The transaction-home kernel adds a canonical immutable status record and routed
  expected-image CAS over a dedicated data-plane status column/directory. It freezes
  cluster/database/domain/schema, transaction ID, original business operation and
  global/participant plan hashes. PREPARING records monotonically collect reports
  of applied prepare positions; COMMIT requires every registered report. COMMIT or
  ABORT is immutable, and finalization ACKs and explicit client consumption advance
  separately. Ordinary CAS cannot delete a HOME; protected retirement now reclaims
  an exact consumed record after every DATA family has retired and HOME's business
  epoch floor has advanced. Local HOME leaders now drive bounded automatic retirement
  discovery on the node's reserved CONTROL lane.
  Every changed image advances one generation and one vote/decision/ACK/consumption
  step. Replica validators reject accepted-command predicate bypass, extra effects,
  altered definitions and noncanonical bytes. Receipt ACK/GC is independent of
  transaction-record consumption. Recovery CAS operations have their own identity
  and epoch; they do not replace the frozen business identity.
  This is a home-record kernel, not complete 2PC: reports are not self-verifying
  participant proofs. Protected status-role activation, public coordinator/frontend
  integration and daemon recovery discovery/admission remain.
- Finalization intents capture a fresh terminal home decision and its route together.
  Their canonical binding retains the frozen definition, chosen COMMIT/ABORT and
  prepare vote positions, while excluding later home generations, ACKs and client
  consumption. Recovery uses a separate operation identity. Matching a native
  preparation checks the original home, full definition and exact recorded vote;
  ABORT without a vote also matches an applied but unreported prepare. Decision bytes
  do not independently prove read provenance; the adapter must retain the verified
  read authority through submission.
- The native participant substrate now installs an immutable accepted business plan,
  frozen home/participant binding, complete exclusive scope and stable prepare
  position in the internal state CF, atomically with completed A and accounting.
  The durable prepared record is also its prepare receipt; no user effects or business
  success receipt are installed. Prepare and business operation identities occupy
  one shared namespace, including cross-role collisions. Duplicate native preparation
  retains its original position. Ordinary receipt/floor GC never removes this state.
  Lock ownership is rebuilt before activation and survives native reopen, fence/activate
  and portable image installation. Fresh user points, new tracked captures, planning,
  mutations and predicates reject intersecting prepared scopes; disjoint work remains
  available. Existing immutable cuts may finish reading their old values. Administrative
  captures remain available for export/recovery. Malformed prepared metadata blocks
  image export and activation.
- The trusted committed-native ABORT seam atomically persists the terminal fence,
  completed marker and accounting, then releases prepared scope ownership. It keeps
  any original positive vote and blocks reuse of business/control identities. ABORT
  before PREPARE creates a fence; a matching late committed PREPARE records its exact
  control identity, payload digest and original negative-outcome position without
  effects or locks. Retries return a typed negative vote, while PREPARE-first ABORT
  retains its original positive vote. Fences and negative receipts survive reopen,
  image installation and receipt GC. Recovery and pinned lookup recognize either
  vote so snapshot-covered and uncertain admissions can settle. Image and activation
  validation check the shared operation namespace and distinguish active locks from
  retained aborted plans. Native fixtures force this ordering directly; the Raft
  FINALIZE path described below now supplies admission authority for append.
- The trusted committed-native COMMIT seam verifies the exact native prepare vote
  against the immutable home decision, then atomically installs ordered raw/bucket/
  range-delete/native-merge effects, the finalization receipt, completed marker and
  receipt accounting. The original result remains in the retained prepared plan and
  is queryable by transaction plus accepted-plan digest, independently of user receipt
  GC. Finalization bypasses original business grant expiry and uses space reserved at
  PREPARE. Lock release follows native publication. Duplicate FINALIZE skips evaluation
  and effects, retaining the first receipt/result even after later writes. Positive
  PREPARE evidence also remains unchanged after finalization. COMMIT and ABORT cannot
  coexist; recovery/image validation require the retained matching plan and disjoint
  operation identities. Finalized plans may overlap new active scopes regardless of
  UUID order. Raft FINALIZE now connects this native seam to append admission, while
  durable coordinator integration remains unfinished.
- Transaction-home and preparation records use one current encoding without format
  release markers or compatibility readers.
- The Raft participant PREPARE path reserves both control/business identities, durable
  record capacity and the complete exclusive scope before append. A fresh home read
  captures the frozen registration and its route together. The registered plan hash
  binds the full accepted command, including original read cut, scope, owner and result;
  it cannot be rebased during preparation. The proposal hands its scope to native
  durable ownership only after publication. Uncertain submitted work retains its
  charges until native outcome, covered-snapshot recovery or a newer completed term
  establishes settlement. Applied retries return the original typed prepared or negative vote.
  A fresh linearizable participant-state query survives response loss and control-session
  receipt GC. The routed client freezes exact bytes and the original descriptor for
  retry. The caller-driven coordinator journal now retains them durably, and the
  the forward and recovery policies below complete chosen decisions. Public frontend
  integration and daemon discovery/admission remain unfinished.
- The current FINALIZE record carries the verified immutable home decision and its
  independent recovery identity in at most 8,010 encoded bytes. A privileged leader
  coordinator reserves a protected input slot before the fresh home read, validates
  native terminal ownership/capacity at one completed cut, and rechecks that cut and
  term/generation before publishing exact accepted bytes. New records without that
  live authority reject before append; exact native receipt evidence permits retry.
  FINALIZE shares the sixteen-entry protected lane with metadata controls and uses
  neither ordinary receipt quota nor volatile scope candidates. Native prepared locks
  protect COMMIT; ABORT installs its durable fence ahead of late PREPARE. Frozen client
  bytes and original descriptors survive retries and route refresh, but callers still
  must journal them durably before the first RPC.
  PREPARE and FINALIZE share one capacity hold for their transaction when necessary.
  Separate idempotent owner tokens prevent closing PREPARE from donating its slot.
  Verified native publication transfers that charge to durable metadata once, while
  remaining tokens retain identity ownership. A blocked invalidated home read retains
  its protected slot until actual return. Submitted unknown finalizers retain their
  ownership through response loss and same-term invalidation; exact native proof or
  a completed newer term establishes settlement. Covered-snapshot recovery settles
  the same holds before completing deferred duplicate suffixes. Applied replies retain
  the first finalization position; the client requires it to follow any registered
  prepare vote. The routed terminal query supplies saved results.
- `RatisDurableTransaction` adds an exclusive, caller-driven coordinator journal.
  Creation forces the initial home CAS, participant list, every accepted business
  plan/result, original routes and control identities before the first home RPC.
  Canonical participant planning images can now be captured without closing the
  original tracked cut or granting append authority. Actual PREPARE and FINALIZE
  requests are forced before dispatch; votes and terminal proofs are forced before
  returning resolved progress. PREPARE registrations may include other participants'
  votes. Each uncertain request retains its exact body, identity and descriptor.
  A saved FINALIZE append receipt without terminal proof remains unresolved: retries
  query proof without resending, reject null/contradictory replies, and force the
  matching outcome before success. A verified ABORT outcome may later gain a negative
  vote while preserving its original decision and finalization position.
  Home observations preserve the immutable definition and exact monotone generation
  steps. An old successful registration receipt cannot regress a newer observed
  decision. Introducing a home vote requires verified local native evidence; choosing
  COMMIT requires every positive vote; ACK and consumption require terminal proofs.
  Definite no-effects registration failure permits a fresh control identity for the
  same initial transaction image, while uncertain registration does not. New home
  CASes may use refreshed peer/configuration descriptors within the original home
  range incarnation; each request then retains that descriptor through uncertainty.
  Atomic checksummed files use a stable exclusive lock, file force, atomic replacement
  and parent-directory force. Persistence failure fences the instance until reopen.
  One RPC owner operates outside the journal monitor; close during an active RPC
  retains file ownership and rejects, so another owner cannot race it. Clients remain
  borrowed. This durable dispatch/state component feeds the recovery policy below;
  it does not itself establish cross-group snapshot gates or public frontend wiring.
- `RatisTransactionCoordinator` advances normal commit requests over the frozen
  journal. Each step performs at most one transaction write: registration, exact
  PREPARE, a verified vote report, decision, FINALIZE or ACK. COMMIT requires every
  local durable positive vote to match the complete home vote set. Native negative
  votes or unavailable original prepare authority permit an ABORT attempt; generic
  I/O stays uncertain. CAS action labels identify attempts, not a guessed decision.
  A competing ABORT therefore defeats COMMIT without exposing business effects.
  PREPARE authority providers retain the original pins and canonical accepted images.
  Exact live ready/inflight PREPARE acceptance can be borrowed after reply loss or
  journal reopen without allocating another reservation or recreating the read cut.
  A closed/conflicting original cut is never rebased. Authority tokens close after
  dispatch using their original ownership rules, and an active policy cannot close.
  Terminal completion shares the recovery path after the already-verified home read,
  avoiding a second home read. Completion requires all saved native outcomes and all
  home finalization ACKs; consumption remains an explicit client action. Saved terminal
  proof may authorize ACK while a participant is temporarily unavailable. The driver
  does not manufacture a consistent cross-group read cut or assemble legacy global
  result order by itself. Public atomic mutation result binding is supplied by the
  async facade below; arbitrary legacy read/write transactions remain separate work.
- `RatisAtomicClient` exposes asynchronous logical atomic mutations and predicates
  over one frozen schema/directory. It opens bounded original local cuts, stages
  exact partition prefixes and forces a transaction journal before HOME/PREPARE
  writes. Handle-bound remote authorization uses those original pins and a configured
  qualified HOME reader. Journals bind each server lease ID into the global plan;
  reopening restores that lease without OPEN, TTL extension or a replacement cut.
  Original unavailable authority permits ABORT; uncertain transport retains the
  request. Success waits for every native finalization and HOME ACK, preserving
  original logical return order and complete collision buckets. Ordinary success
  and close never consume results; consumption is explicit.
  Caller-supplied identities exclusively reserve 256 control slots in an issued,
  adequately bound grant. Grant issuance/window maintenance remains caller-owned.
  Work uses the existing workload scheduler and a bound request deadline. Client
  limits cap operation owners and retained bytes; each actual task admits at most
  16 waiter views. Cancelled views do not donate actual ownership or capacity.
  A failed condition at an original cut is a forced local CONFLICT with no HOME,
  PREPARE or DATA writes. Its compact rejection record reopens without remote work;
  it is not fabricated accepted-plan or terminal-HOME evidence. Rejection and accepted
  journals share the same stable path lock and cannot overwrite one another.
  This mutation API does not expose arbitrary multi-group reads as a shared snapshot.
  It currently uses the coordinator for every participant count; the existing
  one-group logical-session fast path remains available. Public legacy API/transport
  integration, automatic identity allocation and this facade's one-group optimization
  remain unfinished.
- `RatisSnapshotClient` exposes asynchronous scoped snapshot leases, logical GET,
  bounded forward/reverse pages and borrowed cursors over up to 64 DATA groups.
  Planning clips physical scopes before acquisition and retains whole collision
  buckets. A 512-slot caller reservation covers INSTALL/RELEASE and all HOME progress;
  the requested pin lifetime is forced before the first HOME write and survives reopen.
  All scoped gates become READY before capture. Chosen older transactions can be
  helped through native finalization before that barrier; their HOME ACK bookkeeping
  is independent of snapshot capture. The saved owner/token vector is immutable and
  ordinary reads use it without HOME, catalogue, current-leader or latest-value reads.
  Lost owners return explicit snapshot loss rather than replacement captures.
  Logical bounds accept complete keys or leading fixed-key prefixes. Full variable
  member bounds filter within complete boundary buckets; continuation remains an
  exclusive full logical key across bucket and group boundaries. One shared physical
  scan budget covers all partitions, including empty-owner and resume reads.
  Reader and cursor counts/bytes remain charged through actual work after observer
  cancellation. Cursor progress commits only after successful final checks;
  `pendingPage()` retrieves the last actual page without advancing or remote work.
  Returned page buffers cannot mutate that cached outcome or the private continuation.
  Explicit `release` first saves a whole-lease close direction. It resolves any pending
  HOME decision, aborts only undecided acquisition, releases gates, consumes HOME and
  releases the exact original owner pins. Unknown capture replies can be recovered
  under the consumed HOME without waiting for epoch GC. A known released/lost journal
  fails acquisition on reopen. `Lease.close()` detaches local journal ownership for
  recovery; it does not implicitly consume or release remote pins.
  Legacy API/transport integration and consistent cross-group read/write transaction
  certification remain unfinished; these are scoped immutable read leases.
- `RatisTransactionRecovery` implements explicit takeover in bounded steps. It reads
  the home freshly, retries an uncertain frozen home request when still relevant,
  proposes ABORT for otherwise-undecided PREPARING state, and completes a chosen
  COMMIT or ABORT through native outcome queries, current leader FINALIZE authority
  and evidence-gated home ACKs. It never submits a new business PREPARE, changes the
  frozen business identity/plan, overturns COMMIT or automatically consumes results.
  Each step issues at most one transaction write. Control identities come from the
  caller's advancing control allocator and are forced before dispatch. Participants
  rotate before work so an available group can finalize/ACK while a peer fails.
  A newer home image may durably retire an obsolete expected-image CAS: expected-null
  registration cannot overwrite the existing matching home, and a later generation
  cannot match the old expected image again. This semantic retirement stays separate
  from the exact unknown RPC result. Reads validate the descriptor captured with the
  status against the frozen home incarnation; peer/configuration refresh is allowed,
  but another group/range epoch cannot authorize retirement. Exact ready or submitted
  FINALIZE acceptance may be borrowed for retry without another protected reservation.
- Native terminal proof now retires matching volatile PREPARE scope ownership.
  Abandoned ready proposals close; still-running planners lose append authority but
  retain their input/identity ownership until actual return. Submitted unknown
  PREPARE releases only its obsolete scope/capacity when finalization is proven,
  retaining input and control identity until its actual vote or newer-term proof
  settles. Snapshot recovery makes the same distinction. No vote is invented merely
  because ABORT is durable, and a late admitted PREPARE can resolve negatively after
  later user writes without restoring locks or effects.
- `RatisTransactionRecoveryService` runs supplied takeover objects through the
  existing reserved CONTROL scheduler. One coalesced quantum visits at most 32
  recoveries from a bounded membership of 4,096; failed entries rotate and completed
  entries leave the active queue. Timer ticks do not accumulate dependent futures.
  Cancelled observer views do not cancel actual ownership. Stop removes queued work,
  lets the active step settle, skips later visits, and drains before borrowed owners
  may close. Journals, policies, clients and scheduler remain caller-owned. Startup
  journal discovery, qualified authority/control providers and node wiring are not
  supplied by this service.
- Fresh terminal-outcome queries return one native cut containing the immutable
  COMMIT/ABORT binding, first finalization position, optional original prepare vote,
  and COMMIT-only retained business result. ABORT-before-PREPARE initially has no
  vote; after a late rejected PREPARE it reports the durable negative vote. Prepared
  evidence remains positive after PREPARE-first ABORT. Both business and recovery
  grants may expire without disabling this lookup. Non-linearizable and stale query
  paths reject it. Routed replies are checked against the captured transaction,
  range/group/epoch, schema, cluster/database and domain; malformed and trailing data
  fail. Outcome bytes are bounded by one 2 MiB result plus at most 8,043 bytes of
  decision/vote/framing data. The current generic query frame is twelve bytes:
  magic, operation kind and body length, with no format release marker or legacy reader.
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

Durable participant metadata has a per-range maximum of 64 retained preparations
and fence-only transactions combined, 16 MiB of reserved record bytes and 16 MiB of
retained scope accounting. A preparation body fits within 2 MiB minus its eight-byte
Raft envelope; its stored receipt adds the 16-byte original apply position. Every
preparation reserves another 16,134 bytes for terminal metadata: independent negative
PREPARE and ABORT receipts, or a COMMIT receipt whose maximum charge is 8,051 bytes
including its key. Negative votes retain the exact preparation digest, frozen home
registration, control/business identities and first rejection position, without the
accepted business body. They may precede home ABORT; retries remain negative after
later writes, reopen and native image installation. The ABORT record contains only
its decision and first finalization position. A negative vote retains no scope lock
and does not manufacture a terminal home decision. Positive and negative evidence
cannot coexist. Either finalization can drain at the participant cap. Fence-only
ABORT and rejection-only transactions reserve the same combined allowance, counted
once even after ABORT follows rejection. These capacities
are separate from ordinary user-receipt GC and are not donated by lease expiration,
fencing or lock release. Raft PREPARE admission reserves the bounds before append;
native apply also checks them. Participant retirement now reclaims the complete
prepared/rejected/COMMIT/ABORT family atomically, including its capacity charge,
after an exact consumed HOME decision and a strictly newer DATA business-epoch
floor. Remote retirement uses the fixed HOME reader and protected admission, with
at most 16 shared control slots and 1 MiB of retained retirement input per group.
Retired retries return bound fenced-absence evidence rather than a fabricated vote,
finalization position or business result; old pinned readers retain their original
proofs. Unresolved positive preparations remain finalizable after business expiry.
A never-prepared expired participant can ACK ABORT through fresh atomic DATA
absence/floor evidence. Journals persist that evidence separately from native votes
and receipts, and explicit client consumption remains required. Missing consumed
COMMIT results raise a typed outcome-expired error. HOME retirement deletes the
consumed USER row and its protected recovery receipts atomically, after fresh fixed
DATA readers confirm every participant's fenced absence. Expired conflict-only
orphan receipts have a local cleanup mode; it never invents a consumed decision or
waits for DATA availability. A fresh control cannot register an expired business
anchor again. HOME expiry proofs remain separate from decisions and actual receipts,
and old journals terminate explicitly as HOME_RETIRED while preserving previously
proven COMMIT or ABORT results. New businesses may reuse a transaction UUID without
being adopted or deleted by old retries. HOME retirement shares the 16 protected
slots and retains at most 1 MiB of input per group through actual RPC termination.
Snapshot HOME retirement now has a separate dedicated role context and fixed DATA
proof reader. It requires the exact consumed USER status, a strictly newer HOME
business floor and fresh covering gate-retirement evidence from every participant.
Its request stores one shared fence definition and compact participant cuts; the
native batch deletes only the old USER row and updates history/completed accounting.
Ordinary CAS receipts remain historical. Qualified image checks validate USER shape,
placement and catalogue authority, including empty images, before installation.
Journals force HOME expiry separately from cached captures and RELEASE proofs; missing
or newer same-UUID HOME families cannot trigger re-registration or recapture.
A DATA fenced-absence query also settles obsolete never-appended INSTALL ownership,
while active native gates remain protected and busy inputs keep their actual charges.
Snapshot HOME GC shares 16 protected slots with a 16 MiB retained-input bound.
Local ready HOME leaders scan at most 16 rows and 2 MiB per discovery page, returning
at most one consumed family or expired conflict-only orphan. These local hints do
not grant authority: every cleanup RPC independently checks qualified native proofs.
Each CONTROL visit sends at most one mutation, rotating sources and participant
selection so unavailable peers do not monopolize discovery. Consumed active snapshot
gates are RELEASEd before DATA retirement; HOME deletion still waits for all DATA
families. The driver never consumes outcomes or chooses transaction decisions.
Before dispatch it forces the exact current native request into a node journal,
bounded to 16 unresolved target groups and 32 MiB. Lost replies retain that frame
across restart; a newer proof cut cannot replace an unresolved request. Startup
discards only bounded unpublished sidecars under the exclusive journal lock, and
shutdown drains actual cleanup RPCs before closing journals, storage or clients.
Client owners can explicitly reclaim a transaction journal only after saving both
terminal consumption and a matching HOME retirement proof. Cached outcomes remain
available through ordinary close/reopen until that explicit discard. Snapshot
journals additionally require durable release ACK or typed owner loss for every
possible captured pin; gate/HOME retirement and a failed capture do not establish
pin release. `releaseCapture(group)` freezes the original release intent before
its single owner RPC, preserves uncertain replies and works after HOME retirement.
If the original capture reply was lost, a forced matching terminal consumed HOME
or HOME retirement proof permits replaying only that frozen capture request. An
existing pin returns its original token; a missing pin is rejected before native
capture by consumed HOME or retired DATA floors. The method returns RECOVERED, then
a later call releases that token. RELEASED and typed LOST are the settled outcomes.
Explicit release invalidates future capture publication, and unresolved COMPLETE
requests must settle before their pins can be released.
Payload reclamation verifies the exact canonical file with bounded streaming,
removes its bounded unpublished sidecar and payload, then forces the directory.
The stable lock inode survives reclamation, and actual file/RPC work retains
ownership until return. Missing reclaimed files cannot be reopened as fresh work.
Unreachable capture owners, unconsumed outcomes, session/grant journals and global
client-journal count bounds still require their remaining lifecycle contracts.

The native snapshot-gate kernel freezes the complete participant set, directory
and schema identities, local physical scopes, and distinct install/release controls.
The home progress value requires every READY proof before any capture proof and
every capture before COMPLETE. Installed gates reject new intersecting PREPARE with
a durable negative vote and ordinary committed writes with a retained no-effects
conflict. Existing COMMIT/ABORT finalization can drain prior preparations. Native
capture checks the exact active gate and absence of intersecting unresolved plans
under the apply lane. Old retained native pins remain historical after release.
Release requires a matching completed/aborted home decision; an ABORT release before
INSTALL suppresses delayed installation. Recovery and native image validation retain
gates and terminal receipts; restart or expiry never implies release.

`RatisSnapshotFences` persists one monotone home progress/decision step through a
dedicated-column physical DATA CAS. Every replica validates the frozen home
identity, transition and separate control identity; reads return the descriptor
captured with that same status. Initial registration reserves room for the largest
future expected/next status pair, rejecting a definition that would later exceed
the command limit. The home value alone does not prove participant gate or pin
provenance; the coordinator must verify those proofs before reporting them.

Committed snapshot commands dispatch to the native gate kernel through Ratis,
including verified snapshot-covered replay. Routed operations retain the exact
body, identity and original route. Fresh gate queries validate the complete frozen
fence and work independently of expired control grants. Leader admission requires
fresh privileged home authority or an exact retained native outcome; a suppressed
delayed INSTALL is distinct from an applied RELEASE.

Privileged INSTALL claims a gate lease before the home read, blocks new conflicts,
and admits its append only after earlier volatile proposal owners drain. RELEASE
verifies the matching terminal home decision and can overtake an uncertain INSTALL
using the same per-fence storage reservation. Both control identities remain owned
through blocked home reads and uncertain append lifetimes. Same-term invalidation
does not donate a submitted gate lease. Verified native publication or a completed
newer term retires obsolete scope ownership, while actual RPC/input owners remain
charged through return. Capture requires a fresh INSTALLING home with every READY
proof, the exact local installed position, and no unresolved prepared blocker.

Snapshot input shares the protected sixteen-slot budget with other controls.
INSTALL has an eight-owner/eight-MiB sublimit; all snapshot preparing, ready and
submitted input is bounded to sixteen MiB. Retained INSTALL retries use the same
sublimits and preserve RELEASE headroom. Ready or blocked RELEASE work does not
reject an exact native INSTALL retry belonging to that same fence.

Gate conflicts now certify an explicit negative PREPARE request under the same
admission monitor as both control/business identity claims. This request owns no
candidate lease, prepared scope or business effects. It uses protected control
slots and an independent eight-MiB input allowance, so ordinary or candidate quota
saturation cannot prevent a negative vote. Native rejection admission reserves only
compact negative/terminal metadata and checks existing positive evidence first.

The chosen request kind, complete original preparation and route are frozen before
RPC and preserved by the coordinator journal across reopen. A frozen positive
request is never converted into an explicit negative one, and negative requests
cannot accept positive replies or recovered proofs. A gate may release or fail
after certification without changing that negative request into a positive plan.
An original accepted image can be rejected in a newer term without reopening its
read cut. Existing positive preparation owners retain their original rights.

Negative payload ownership transfers to submitted input and can be shared with
FINALIZE; native publication releases storage reservation without donating payload
still owned by an RPC or finalizer. Fresh negative authority requires the matching
PREPARING home and a reserved or installed gate at the claimed native cut. Exact
retained negative outcomes remain retryable independently of expired grants.

Per range, gates have an independent bound of 16 retained fence records and 16 MiB
of reserved metadata, including future release growth. Each fence now freezes an
original business operation, independent of its INSTALL/RELEASE controls. Explicit
terminal consumption permits retention cleanup but does not certify RELEASE ACKs
or snapshot delivery. DATA gate retirement requires the exact released gate, a fresh
matching consumed HOME status and a strictly newer durable business-epoch floor.
It atomically deletes the gate and refunds capacity; late INSTALL/RELEASE and READY
queries return bound retirement evidence without invented apply positions or a
suppressed-INSTALL vote. Active expired gates remain retained and releasable.
Consumption does not change the frozen RELEASE request or decision digest. A fence
contains at most 64 participants and an aggregate definition below 2 MiB minus
8 KiB, leaving bounded room for progress and envelopes. The ephemeral admission
index can reserve a gate before earlier proposals drain, with independent protected
limits. Coordinated multi-group capture/recovery and frontend snapshot leases
remain unfinished; these participant seams alone do not establish a consistent
cross-group read cut.

Participant snapshot pins now have bounded local ownership. The coordinator supplies
an expected owner incarnation and capture ID before invoking capture. An exact
fence/ID/TTL retry returns the original token and native cut without another home
read or deadline extension. Changed declarations are rejected. Released, expired
or storage-invalidated captures retain small tombstones and return SNAPSHOT_LOST;
they never reopen at the current cut. Closed tombstones can be reclaimed after the
durable DATA floor fences their original business epoch. Unknown expired capture
requests fail before entry allocation; live exact retries preserve their original
cut and deadline even after the floor advances. A new owner rejects an old expected incarnation
before allocating a native pin.

The pin registry retains at most sixteen capture identities and sixteen MiB of
accounted frozen definition/scope bytes. Actual native owners keep their complete
charge through a busy capture/read and successful native close, then compact to a
136-byte tombstone. Immutable reads and scoped forward/reverse pages survive
leadership loss on the same live owner. Storage pause invalidates these leases
before the existing native-reader drain; old tokens stay lost after reload, while
new fence/capture identities may proceed. Shutdown and expiry use the same deferred
cleanup rule. Point values remain bounded to two MiB and physical pages to eight
MiB. These are payload/count bounds, not measured pinned-SST or node memory limits.

Capture creation is exposed through qualified local methods and a frozen-owner RPC.
The remote request retains the full fence, expected process incarnation, capture ID
and finite TTL before its first RPC. DATA participants use only an operator-bound
home reader installed before initialization; request bytes never select connectors.
The node runtime validates the reader's cluster/database identity before bootstrap
writes and installs it before server creation can initialize or expose DATA groups.
Catalogue groups remain unbound, and the reader remains caller-owned.

Exact remote retries preserve the original token after leadership loss or home
completion. Unknown captures still require the configured reader, fresh all-READY
home proof, matching local gate and current leader. Wrong owner or expired requests
return bound SNAPSHOT_LOST without allocating a replacement. Blocked home reads
retain registry charges until actual completion.

Owner discovery uses a fresh linearizable query and freezes the original route,
exact voter descriptor and owner incarnation. Snapshot capture/GET/PAGE/RELEASE target that
peer explicitly through the Ratis stale-read API at minimum index zero; this path
uses no healthy-leader discovery or fallback and validates the reply peer/group.
Owner and ordinary connections share the same bounded client cache and drain rules.

Each pinned request and reply binds the complete capture token to the frozen fence
ID and definition digest. Crossed targets from two captures on the same owner cannot
read or release one another. These immutable requests may serve after leadership
loss, and explicit lost replies remain readable during storage pause. Capture and
owner discovery retain their fresh-authority checks; arbitrary stale transaction
queries and current-index constraints remain rejected.

Durable multi-group capture orchestration, production home-role/CLI activation,
frontend cursor binding remain unfinished. Clients
must freeze owner/capture identity before RPC and must not recreate a missing pin.
Owner transport is covered by controlled participant and SDK-call fixtures; no live
network, process or quorum failover acceptance is claimed.

Each coordinator journal freezes at most 64 participants and 16 MiB of aggregate
accepted commands (including their result images), plus 16 MiB of frozen routes and
reserved pending-home routing. It reserves future registration/decision/proof metadata
before the first home RPC; the complete current snapshot is capped at 64 MiB including
its checksum. Accepted business/result bytes are stored once, with compact request
and outcome metadata reconstructed against that immutable intent. Snapshots are
rewritten atomically per durable state change; unchanged fresh home/participant
observations reuse the already-forced state, and saved terminal proofs avoid another
participant download before ACK. Write amplification and global journal
storage/tenant envelopes still require performance/resource qualification.

A transaction-home record has at most 64 frozen participants and 7,926 encoded bytes.
Both expected and replacement images fit comfortably within the 2 MiB atomic command
bound. This is a per-record bound; global active-decision retention/admission and
protected status-pool execution envelopes still need implementation.

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

The PREPARE regression gate passed 32 core and 85 adapter cases (117 total,
zero failures/errors/skips), exit zero: `/tmp/rockserver-raft-prepare-regression.log`.
The final focused selection passed eight core and ten adapter cases (18 total),
exit zero: `/tmp/rockserver-raft-prepare-final.log`. Independent review identified
an ownerless-admission settlement lookup and a home-record/route refresh race;
both were corrected. The registered hash now binds the full accepted plan.
Review found no remaining concrete finding at the implemented PREPARE boundary.
Native io_uring allocation warnings occurred without test failures.
After removing transaction-home/preparation format release markers directly, the
codec/native-image/home/PREPARE selection passed 24 core and 12 adapter cases
(36 total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-current-transaction-format.log`. No legacy reader was added.

The finalization-binding selection passed 11 core and 12 adapter cases (23 total,
zero failures/errors/skips), exit zero: `/tmp/rockserver-finalization-binding-final.log`.
It covers stable terminal identity across ACK/consumption, independent recovery IDs,
exact native vote matching, unreported-prepare ABORT, malformed canonical encodings
and fresh home reads before/after progress. Independent read-only review found no
concrete issue at this binding boundary. That earlier gate does not establish native FINALIZE or ABORT-fence acceptance.

The native ABORT/typed-vote regression gate passed 39 core and 86 adapter cases
(125 total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-abort-fence-regression.log`. After image-ownership checks and added
recovery coverage, the final selection passed 32 core and 14 adapter cases (46 total),
exit zero: `/tmp/rockserver-abort-fence-final.log`. Review found and confirmed fixes
for unchecked negative control ownership and UUID-order-dependent recovery of
released overlapping scopes; no further concrete finding remained in that review.
The final cases include ABORT-before-admitted-PREPARE, lost negative replies,
reinitialize, stable exact retry, no effects/locks, full-cap ABORT, positive evidence
retention, reversed UUID reopen/install, cross-role/fence/ordinary receipt collisions,
and malformed ABORT metadata rejected by export and activation. ABORT itself enters
through the committed-native seam; this is not Raft FINALIZE or full 2PC acceptance.

The native COMMIT regression gate passed 53 core and 87 adapter cases (140 total,
zero failures/errors/skips), exit zero: `/tmp/rockserver-native-commit-regression.log`.
This includes six native COMMIT fixtures and two codec cases, plus native merge
finalization branches with and without range deletion. Checks cover ordered multi-CF
point/bucket/range effects, exactly-once merge operands, original result retention,
immutable old readers, conflicting old write-cut rejection, original-grant expiry,
term changes, finalization at the preparation cap, changed-plan/vote/recovery-ID
rejection, duplicates after later writes, retained positive votes, reopen and portable
install with reversed UUID overlap order, and corrupt/missing finalization bindings
rejected by export and activation. Independent read-only review found no concrete
issue at this native boundary. This gate does not establish Raft FINALIZE admission,
coordinator recovery, a complete multi-group transaction or quorum behavior.

The terminal-outcome/current-query-frame regression gate passed 55 core and 89
adapter cases (144 total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-participant-outcome-regression.log`. The preceding focused gate
passed 17 core and 23 adapter cases (40 total): `/tmp/rockserver-participant-outcome.log`.
New checks cover terminal/vote/result shape and codec bounds, COMMIT and both ABORT
orders, lost negative replies, fresh-only lookup, business/control/recovery grant
expiry, snapshot/reinitialize, captured route/transaction checks and malformed replies.
Independent read-only review found no concrete issue at this query boundary. Finalize
writes still enter through trusted native seams in these fixtures; this gate does not
establish Raft FINALIZE admission or a complete coordinator workflow.

The protected FINALIZE regression gate passed 55 core and 97 adapter cases (152
total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-finalize-admission-regression.log`. The preceding focused gate passed
15 core and 22 adapter cases (37 total): `/tmp/rockserver-finalize-admission3.log`.
Eight new controlled fixtures cover lost replies and exact frozen-route retries,
expired business/recovery grants, COMMIT and both ABORT orders, 63 durable plus one
pending preparation with shared capacity retained after close, ABORT overtaking a
blocked prepare planner, uncertain late PREPARE, full 1,024-entry user admission plus
an overlapping candidate, blocked invalidated home reads, same-term unknown suffixes
and newer completed terms, and snapshot-covered finalization with deferred duplicates.
Independent read-only review found no concrete remaining issue in this seam. One
fixture initially tried SESSION_FLOOR while PREPARE still held its metadata guard;
that expected rejection was corrected by moving the floor after finalization/close.
After dropping the settled finalizer's reference to its former shared payload hold
and adding reply-position binding to the registered vote, the final lifecycle
selection passed 23 adapter cases, zero failures/errors/skips, exit zero:
`/tmp/rockserver-finalize-admission-final2.log`.
No real network, server process or quorum acceptance is claimed.

The coordinator-journal regression gate passed 55 core and 123 adapter cases (178
total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-coordinator-journal-regression.log`. The focused gate passed 39
adapter cases: `/tmp/rockserver-coordinator-journal-final.log`. Eleven new fixtures
cover lost registration/PREPARE/FINALIZE replies with close/reopen and exact recovery,
nonregressing old home receipts, fabricated vote/ACK rejection, forced-file failure
before participant RPC, active-RPC ownership through rejected close, checksum and
client-identity binding, two groups with a later voted registration and refreshed
home route, ABORT gaining a delayed negative vote, aggregate rejection before any
home effect, and registration grant expiry recovered under a fresh control identity.
Independent review found a saved-receipt/terminal-proof short circuit; it is fixed.
Its fault test forces the receipt, loses proof, reopens, returns null then a wrong
position, and succeeds only after matching proof is forced, with one FINALIZE append.
Review confirmed that fix and found no further concrete issue in the examined
boundary. These fixtures remain controlled RPC/native runs, not power-loss, quorum
or automatic recovery-scheduler acceptance.

The recovery-policy/service regression gate passed 55 core and 141 adapter cases
(196 total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-transaction-recovery-regression.log`. After the per-visit stop guard
and captured-home-incarnation check, the final selection passed 53 adapter cases,
zero failures/errors/skips, exit zero: `/tmp/rockserver-transaction-recovery-final2.log`.
New fixtures cover reopened unregistered intent, abandoned ready PREPARE, lost COMMIT
receipt retired by fresh home proof, another coordinator's winning terminal proof,
competing COMMIT versus recovery ABORT, unavailable grant/invalid authority, same-term
unknown FINALIZE reattachment, and one unavailable data group while another finalizes
and ACKs. Service fixtures cover timer completion without consumption, completed-entry
retirement, cancelled observers, stop/drain while authority blocks, skipping later
visits after stop, failure fairness and queued removal. Terminal-snapshot recovery
releases unknown PREPARE scope while retaining its input until a real negative apply.
Independent review found and confirmed the FINALIZE reattachment fix; no remaining
concrete issue was found in that reviewed boundary. The final home-route fixture
rejects a status read from another incarnation without manufacturing an RPC receipt.
These are controlled fixtures; process/quorum/fault-disk and daemon wiring acceptance
remain separate and unperformed.

The forward-coordinator regression gate passed 55 core and 152 adapter cases (207
total, zero failures/errors/skips), exit zero:
`/tmp/rockserver-transaction-coordinator-regression.log`. After cached terminal-proof
reuse, the final focused selection passed 59 adapter cases, zero failures/errors/skips,
exit zero: `/tmp/rockserver-transaction-coordinator-last.log`. Ten coordinator fixtures
cover normal completion without consumption or duplicate home reads, lost PREPARE
reply, ready and unknown submitted reattachment, unavailable versus uncertain authority,
original-cut conflict without rebasing, competing ABORT, lost final ACK, cancelled
observer ownership, two-group commit with a lost reply and partial-finalization locks,
and ACK from forced proof while the participant transport is unavailable. A journal
fixture blocks atomic replacement to prove unchanged observations do not write, while
new intents still force and fence before dispatch. Independent read-only review found
no concrete issue in the examined forward/cached-proof boundary. This does not prove
cross-group snapshot consistency, public API parity, runtime role wiring or real quorum
acceptance.

The exercised native preparation cases cover unchanged user values, complete read/
absence/range/bucket lock scope, disjoint access, old immutable readers, new capture/
planning/admission rejection, stable duplicate prepare positions, separate ordinary
receipt accounting, session-floor independence, count capacity, native pool reopen,
portable image installation and corrupt-record fencing. Controlled Raft fixtures
also cover lost replies, both identity reservations, capacity held during blocked
home reads, invalidation, frozen route restoration, snapshot-covered deferred apply,
unknown same-term suffixes and fresh-only state queries. Two data groups now report
actual controlled applied votes to a home, choose COMMIT, apply both Raft FINALIZEs,
ACK each participant and consume the home record. While only one group is finalized,
the other's lock prevents stale user reads. Both values become readable after local
finalization. This establishes the exercised protocol path, not participant quorum
behavior, scoped cross-group snapshot consistency or the public transaction frontend.

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

The October 12 checkpoint adds an authoritative database DATA-domain roster.
Initial directory publication guards and updates that roster atomically; aggregate
bootstrap advertises every declared domain and rejects incomplete catalogue reads.
The node plan, manifest, service and runtime now carry all declared DATA domains.
They share one pool, catalogue, clock, retirement coordinator and recovery registry.
The controlled node fixture writes distinct domains, cold-reopens the service and
verifies the complete catalogue roster; it passed with the focused integration
selection in `/tmp/rockserver-multi-domain-cohort-focused-repair-gate.log`.
Native aggregate bootstrap, operator evolution and cold metadata recovery passed
31 unique cases in `/tmp/rockserver-multi-domain-cohort-backup-repair-gate.log`.
The native preflight reads default and STATE with strict WAL recovery, retaining
full live-CF birth/operator validation without requiring USER merge callbacks.

Relocation source transfer now retains the exact prepared native image across
restart and forces request-bound release receipts before artifact cleanup. The
source-service/artifact/RPC/client/host selection passed 23 controlled cases in
`/tmp/rockserver-source-cold-release-host-repair-gate.log`. The host fixture includes
failed lease construction and verifies that its native pin drains. The
relocation controller now drives image transfer, native tail readiness, conditional
membership CAS and catalogue publication. Actor-journal durability/sidecar recovery
and CAS call ownership passed the 59-case repair gate plus seven host cases in
`/tmp/rockserver-relocation-actor-durability-repair-gate.log` and
`/tmp/rockserver-relocation-actor-sidecar-guard-gate.log`. Qualified SOURCE/TARGET
joint memberships now recover without weakening peer
identity checks. Historical COMPLETE retries preserve newer routing. Current
completion also refreshes the local schema/topology replica role while in-flight
schema jobs retain their captured context. The registered receiver recovery
controller now retains the exact later image through unknown preparation replies,
transfer, native import, joint/stable membership and five-voter replacement. Its
controlled workflow passed in the focused integration selection above; direct
node-dispatcher acceptance for semantic 109 remains pending.

Native learners retain their original image evidence and can install an explicitly
authorized later image without rolling back a completed tail. Receiver preparation
can create its own operator/CF generation for a newer-schema
image without claiming historical participation. The 49-case native gate passed
in `/tmp/rockserver-behind-schema-native-gate.log`, including 13 learner cases.
The registered-receiver client/controller adapter is implemented and exercised
with controlled transport and actual native preparation/import. This does not
establish a running endpoint or independent-node/quorum recovery.

Snapshot coordination can stop with all gates READY, or all captures reported
while HOME remains INSTALLING. The six coordinator cases passed after repair in
`/tmp/rockserver-held-barrier-host-fixture-repair-gate.log` (the separate host case
in that run failed and passed in the later source-transfer gate above). The
global driver/journal and runtime cohort service/client now enforce original
process/pool/member identities, all-gates-before-capture, one physical pool cut,
global-vector force before HOME reports, finite original deadlines and actual
cleanup settlement. Controlled service/client and driver fixtures passed in the
focused integration selection, including 65 groups across two native pools.
The owned public backup factory was subsequently implemented and validated as described in the current acceptance section.
The affected consumer adapters passed their selections in
`/tmp/rockserver-multi-domain-consumer-adaptation-gate.log`; topology fixtures
were then corrected to declare their actual four-group scope. The final 28-case
selection in `/tmp/rockserver-multi-domain-topology-capacity-gate.log` passed,
including rejection before native birth when local capacity is exhausted. Native pool cohorts now share one physical snapshot and admission charge;
45 native cases passed, including 65 groups, in
`/tmp/rockserver-cohort-relocation-backup-combined-gate.log`. Copied archive assets
and fresh-cluster restore planning passed their ten cases in
`/tmp/rockserver-relocation-image-backup-repair-gate.log`; full restore execution
remains incomplete. READ_ONLY capture requests now permit a finite maximum of
24 hours, further restricted by the configured native pin age; defaults remain
60 seconds. WRITE_CERTIFIED stays capped at 60 seconds. Retries retain their
original deadline. The 119-case controlled adapter selection passed in
`/tmp/rockserver-retention-backup-export-api-repair-gate.log`, covering retention
validation, cold journal reopen, and role rebind. The global driver now passes copied-graph completion after pin expiry, uncertain
force/reopen, exact archive qualification and repeated terminal cleanup fixtures.
These fixtures use controlled HOME/subjournal adapters; full runtime backup and
restore execution remain incomplete.

SST imports now freeze declared count, byte, metadata and JNI batch limits in the
current manifest. The 34-case native selection passed in
`/tmp/rockserver-large-import-policy-jni-diagnostic-gate.log`; the actual 65-SST
single-prefix fixture covers interrupted batching, cold replay, preserved neighbours
and saved-receipt retry. Distributed quota/planning alignment and preflight I/O
isolation are being validated. These fixtures do not establish large-data throughput
or a globally admitted node/pool/tenant envelope.

Topology has bounded definitions, staged quorum proofs and catalogue CAS for
reservation, PREPARED, FENCING and explicit pre-fencing abort. Directory publication
and schema/import starts mutually exclude active topology intents through exact
head guards. A fresh catalogue query binds the definition, intent and source
directory at one completed cut and rechecks leadership/configuration ownership.
The trusted native FREEZE seam waits for actual writable owners, prepared work and
snapshot gates to drain, then atomically persists the original parent cut. Readonly
owners, outcome lookup and explicit receipt/epoch accounting remain available.
Cold reopen and images preserve the fence; images cannot discard it.
Combined focused validation passes 149 tests in 19 classes. Unchanged passing
results from `/tmp/rockserver-topology-freeze-fixture-gate.log` are reused with the
four repaired authority fixtures in `/tmp/rockserver-topology-authority-fixture-gate.log`.
DATA Ratis FREEZE now qualifies a bounded whole-column admission hold, drains
accepted predecessors before native preparation, and waits for follower-local
readiness without parking the proof lane. Runtime wiring supplies the actual local
replica and borrowed catalogue authority. Snapshot replacement drains actual actions,
closes volatile gates and fences stale callbacks; covered replay returns the original
artifact and an uncovered suffix recreates preparation. Controlled adapter validation
passes 68 tests in seven classes (`/tmp/rockserver-topology-ratis-freeze-final-gate.log`),
with 12 unchanged native/schema tests reused from the first compile gate. The received
snapshot regression failed before repair (`/tmp/rockserver-topology-freeze-snapshot-before-fix.log`).
Local split/merge seeding now forces WAL and one bounded metadata batch before
publishing RETIRED sources and FENCED children. USER rows remain in their existing
CFs; no parent applied marker, configuration, receipts, floors or CDC rows are copied.
Cold recovery restores topology ownership before ordinary range registration.
Birth pointers retain the original seed artifact; children have absent Raft progress
until their own bootstrap. Retired metadata/outcome lookup and already-open readonly
pins survive the handoff. Controlled `withSessions` adapter startup verifies that a
child's `(1,0)` Raft cut is separate from its `(0,0)` birth proof and the parent cut.
Combined validation passes 71 tests across seven classes; passing unchanged model/
ownership cases from `/tmp/rockserver-topology-local-seed-fixture-gate.log` are reused
with `/tmp/rockserver-topology-local-seed-startup-gate.log` and the extended adapter
case `/tmp/rockserver-topology-local-seed-session-startup-gate.log`.
Qualified activation, dynamic group registration, publication and lineage handoff
remain required. Retired reopen after later USER-CF deletion and activated-child/
remote snapshot provenance remain unfinished. Pending children and retired sources
cannot export/install USER images; their adapter snapshots are disabled until the
appropriate recovery/retirement workflow exists. No server sockets or quorum were exercised.
Relocation currently admits preparation/abort while its source stays writable.

## Remaining architecture gates

- Complete restore quota/unknown-force acceptance, plus public backend
  capture/transaction/export workflows.
- Declare and implement explicit pool/SLO placement. Reserve configured durable
  replica ceilings before opening/voting, reconstruct retained claims on recovery,
  and share volatile proposal/staging/retained-buffer budgets across node/pool scopes.
- Complete and validate replica-local maintenance integration without presenting
  physical SST identities as cluster-wide files.
- Compose snapshot plus CDC tail gap recovery and export-based consumer migration;
  establish adoption by the intended external consumers.
- Verify all documented topology, schema, import, retirement and repair workflows
  together under the final source/configuration, then reconcile contracts and status.
- Independent quorum/restart/fault and representative performance/resource gates
  remain outside the currently authorized controlled-fixture validation. Final
  integration with the user's chosen checkout has not occurred.

Standalone remains supported. This branch has not been pushed, published, merged
or deployed.
