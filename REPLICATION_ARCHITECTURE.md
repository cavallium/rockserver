# Rockserver Replication Architecture

This document defines the target architecture for replicated Rockserver deployments using Apache Ratis as the Raft consensus library.

The core change is that Rockserver owns the replicated database state machine. Ratis provides Raft groups, log replication, leader election, client routing primitives, snapshots, membership changes, and transports. Rockserver provides partitioning, placement policy, command encoding, RocksDB application, dedupe, schema, merge semantics, CDC, backup, and operational control.

## Source Assumptions

The design is based on these Apache Ratis facts, checked on May 15, 2026:

- Apache Ratis is a Java Raft protocol library, not a standalone database server.
- Applications use Ratis directly from code and provide their own state machine.
- Ratis artifacts are published to Maven Central.
- Apache Ratis 3.2.2 is listed by the Apache Ratis downloads page as released on April 3, 2026.
- The getting-started guide shows `ratis-server` plus one transport, such as `ratis-grpc` or `ratis-netty`.
- Ratis state machines implement transaction application and snapshots.
- Rockserver's Java 25 JPMS build cannot place both `ratis-server` 3.2.2 and `ratis-server-api` 3.2.2 on the module path as production modules because they are automatic modules with split `org.apache.ratis.server*` packages. Treat Ratis as requiring an explicit classpath or adapter boundary until a JPMS-safe packaging strategy is proven.

Primary references:

- https://ratis.apache.org/downloads.html
- https://ratis.apache.org/getting_started.html
- https://central.sonatype.com/artifact/org.apache.ratis/ratis-server
- https://apache.googlesource.com/ratis/+/refs/heads/master/ratis-server-api/src/main/java/org/apache/ratis/statemachine/StateMachine.java

## Existing Baseline

Current relevant pieces:

- `EmbeddedDB` is the local RocksDB-backed engine.
- `RocksDBAPICommand` classifies API commands and exposes read-only metadata.
- `GrpcServer` exposes the current API over gRPC.
- `GrpcConnection` is the remote client transport.
- `ClusterConnection` is experimental routing glue, not a correctness boundary.
- Local CDC is WAL-driven and resolves merge and bucketed-column behavior.
- `ColumnSchema` and `ColumnInstance` define fixed keys, variable-tail hashed keys, and bucketed value layouts.
- Current column IDs come from local RocksDB `ColumnFamilyHandle` IDs. Replicated mode needs cluster-assigned logical column IDs.
- `scanRaw` reads live SST files and partitions work by SST filename hash. It is embedded/admin tooling, not a distributed query or replication primitive.
- The old custom Raft and MicroRaft spikes have been removed from main sources and from the public protobuf API. Shared RocksDB checkpoint snapshot support lives in the neutral `it.cavallium.rockserver.core.cluster.snapshot` package so Ratis snapshot and backup code do not import retired spike namespaces.

## Goals

- Multiple Rockserver instances on separate servers.
- Strongly consistent writes per replicated partition.
- Linearizable reads by default, with explicit stale or local read modes where safe.
- No acknowledged write loss while a Raft quorum and its durable state survive.
- No permanent divergence after crashes, partitions, restarts, compaction, snapshot transfer, or lagging replicas.
- High throughput through partition-level parallelism, async APIs, batching, bounded queues, and backpressure.
- Predictable resource use across heap, direct buffers, RocksDB memory, Raft logs, snapshots, network queues, and compaction debt.
- Clear operational recovery for disk loss, corrupted replicas, hot partitions, node replacement, backup, restore, and disaster recovery.

## Non-Goals

- The first production cut will not provide atomic multi-partition transactions.
- The first production cut will not allow direct local RocksDB writes in replicated mode.
- The first production cut will not let `*_NO_WAL`, local SST ingestion, or local admin APIs bypass replicated command semantics.
- Ratis will not be hidden behind a second in-house consensus implementation.
- Ratis will not provide a key-value database, range placement service, or schema system. Rockserver owns those layers.

## Core Decision

Use Apache Ratis as the only production consensus engine, but isolate it behind a narrow Rockserver adapter boundary.

The first production implementation must not put Ratis automatic modules directly on Rockserver's production JPMS module path. The selected packaging track is a dedicated shaded adapter artifact that owns and relocates all Ratis classes; the existing classpath-isolated process adapter remains the runtime rehearsal and fallback boundary. The rest of Rockserver talks to that adapter through Rockserver-owned interfaces and command payloads. Production core sources must not import raw `org.apache.ratis.*`; only adapter implementation sources may do so before shading. This keeps Ratis as the consensus engine without letting packaging constraints leak through the core API.

The Phase 8 dependency/license scan is recorded in `REPLICATION_RATIS_DEPENDENCY_LICENSE_SCAN.md`. It finds no new Ratis-profile license blocker in the resolved local metadata, but it keeps the adapter boundary mandatory: Ratis server artifacts remain automatic-module/split-package risks, `ratis-metrics-default` cannot form a valid automatic module name, `ratis-thirdparty-misc` requires bundled notice preservation, and Rockserver's visible Netty version mediation must be resolved before a production fatjar or runtime image.

Rockserver maps each replicated partition to one Ratis Raft group. A partition is a logical key range or explicit salted key range. Each partition group runs a `RockserverRatisStateMachine` that applies committed command entries to the local RocksDB store.

Write flow:

1. A client sends a typed Rockserver command with `clientId`, `requestId`, schema epoch, durability/read policy, and read/write semantics.
2. The gateway applies deadline, retry-budget, admission, and backpressure checks before the command becomes a replicated payload.
3. The gateway encodes affected storage keys and resolves the owning partition.
4. The gateway submits the deterministic command payload to the Ratis group leader.
5. Ratis replicates the command to a quorum and commits the log entry.
6. The Rockserver state machine applies the committed entry to RocksDB using a deterministic local write batch.
7. The same apply step records dedupe metadata and any response-producing result.
8. The response is returned after the required commit/apply/read point is reached.

Deadlines, retry budgets, observed route hints, and other gateway-only admission data must not affect state-machine apply and must not be required to replay the log. If they are recorded for diagnostics, they are non-semantic metadata.

This means Rockserver must implement what a distributed KV substrate would otherwise provide:

- partition metadata;
- route lookup;
- split and merge policy;
- placement policy;
- membership workflows;
- snapshot integration;
- distributed scans;
- anti-entropy;
- backup manifests;
- command idempotency.

## Topology

Terms:

- Cluster: one replicated Rockserver deployment.
- Node: one Rockserver process hosting zero or more partition replicas.
- Partition: one logical key range or salted key range.
- Ratis group: one Raft group for one partition.
- Leader: the Ratis peer that accepts writes for a partition.
- Follower: a voting peer that receives log entries and applies committed state.
- Observer/listener: a non-voting catch-up role if the selected Ratis version supports the required production workflow; otherwise use temporary voters only with explicit reconfiguration safety checks.
- Metadata group: a small Ratis group that owns cluster metadata: nodes, partitions, placement epochs, schema, column IDs, and split plans.

Defaults:

- 3 voting peers per partition for normal production deployments.
- 5 voting peers for higher failure-domain requirements.
- Voters placed across independent failure domains.
- One metadata group per cluster, with stricter operational controls than data groups.
- Partition leadership balanced across nodes.

Metadata bootstrap is explicit and file-backed only for the first formation step:

- A new cluster starts from an identical bootstrap manifest on every initial metadata peer.
- The manifest contains `clusterId`, metadata group ID, initial peer IDs, peer endpoints, failure domains, and a bootstrap epoch.
- Each node persists its node ID and cluster ID before joining any Ratis group. A node ID must not be silently reused with a different disk or cluster ID.
- After the metadata group commits its initial record, all node registration, peer changes, schema changes, partition creation, and placement changes are metadata-group commands.
- Disaster restore creates a new cluster ID and new partition epochs unless an operator explicitly performs a documented same-cluster recovery.

Runtime replicated-mode configuration is exposed under `database.replication` and is disabled by default. When enabled, a node must provide its node ID, advertised internal Ratis endpoint, public API endpoint, data and metadata paths, failure domain, and initial metadata peers. Public API endpoints and internal Ratis endpoints remain separate configuration fields so later lifecycle wiring cannot accidentally expose Raft traffic on the client API surface.

Replicated-mode configuration and metadata registration reject identical public API and advertised Ratis endpoint URIs. Metadata peers carry both endpoints explicitly, and `RegisterNode` metadata commands preserve the separation before node records can enter cluster metadata.

Rockserver server processes own the Ratis adapter lifecycle through a narrow `RatisServerLifecycle` boundary. Public API servers start the Ratis lifecycle before accepting client traffic and close it after the public endpoint has stopped. If either Ratis startup or public endpoint startup fails, the lifecycle is closed before the failure is returned. The lifecycle boundary records the adapter boundary mode so direct production JPMS integration cannot be introduced silently. `RatisProcessAdapterLifecycle` is the first concrete external-process lifecycle: it launches an explicit classpath-only JVM command, rejects caller-supplied module-path/classpath overrides, reports `EXTERNAL_PROCESS`, and escalates shutdown from graceful destroy to forceful termination when the adapter process does not exit. Forceful termination receives a separate capped 100-millisecond confirmation grace after the graceful deadline, including explicitly configured immediate-escalation mode.

Public API server lifecycle cleanup marks the Ratis lifecycle closed only after delegated cleanup succeeds, so a transient adapter-close failure remains retryable by a later server close. Startup rollback suppresses checked or runtime close failures onto the original startup fault instead of replacing the primary failure, and later server cleanup retries the failed rollback.

Adapter process startup records the launched child before health verification. If failed-start health rollback cannot terminate that child even after forceful shutdown, or if either termination signal throws an unchecked fault, the lifecycle preserves the primary startup failure, attaches cleanup diagnostics, and retains the live process handle so a later close call can retry cleanup instead of losing ownership of the process. After a ready health snapshot, startup rechecks child liveness before returning so a process that exited during verification cannot leave public endpoints accepting traffic against a dead adapter.

Public endpoint shutdown and Ratis lifecycle cleanup run through one aggregated close boundary, so endpoint cleanup failures cannot skip adapter close. The endpoint failure remains primary, any adapter-close failure is suppressed onto it, and failed adapter cleanup remains retryable.

Public endpoint startup and Ratis lifecycle startup also share one rollback boundary. If either startup phase fails, already-constructed public endpoint resources are stopped before adapter cleanup; endpoint or adapter rollback failures are suppressed onto the original startup fault, and failed adapter cleanup remains retryable.

Shared Ratis lifecycle ownership tracks delegated cleanup obligation independently from active public-endpoint references. A failed first delegate start publishes no active reference, but endpoint-first startup rollback can still invoke delegate close; if that cleanup fails, later close calls retry it and new starts fail until cleanup succeeds. Final endpoint release also publishes zero active references before delegated shutdown, so a failed adapter close cannot masquerade as a live endpoint and allow unsafe reacquisition; cleanup remains retryable and successful cleanup permits later reuse.

Once a public server instance has closed its Ratis lifecycle, that lifecycle state is terminal for the instance. A later endpoint start attempt fails before accepting traffic instead of silently skipping adapter startup with stale `started` state.

External-process adapter startup treats checked exceptions, runtime exceptions, and `Error`s from health verification as failed startup. The lifecycle runs bounded graceful and forceful child termination before propagating the probe failure, clears ownership after confirmed exit, and retains the child handle when bounded cleanup cannot terminate it so a later close can retry.

gRPC public endpoint shutdown drains the gRPC server, Netty event-loop group, and request scheduler as independent aggregated resource phases before Ratis lifecycle cleanup. Forced server termination and event-loop termination are bounded and fail explicitly when they cannot drain; scheduler graceful-disposal failures force disposal and propagate without skipping later cleanup.

gRPC public endpoints track request-scheduler ownership explicitly. Endpoints backed by an internal database connection borrow its scheduler and leave it running across endpoint shutdown or failed-start rollback; endpoints that construct their own scheduler dispose it during endpoint cleanup.

Thrift public endpoints inject and own their request-worker executor instead of leaving it implicit inside the library server. Shutdown aggregates listener stop, bounded serve-thread termination, and worker-executor draining before Ratis lifecycle cleanup; the configured library worker drain is bounded, close-before-start still terminates the owned pool, and a closed Thrift endpoint rejects later starts. Constructor rollback preserves the original startup failure and suppresses transport or worker cleanup faults, so rollback diagnostics cannot replace the failure that made the endpoint unusable.

Public server lifecycle helpers aggregate checked exceptions, runtime exceptions, and `Error`s across endpoint rollback and teardown. Endpoint-start failure still closes constructed public resources before Ratis lifecycle cleanup, endpoint close cannot skip adapter cleanup, and resource teardown continues through later sibling resources while preserving the original failure and suppressing subsequent cleanup faults.

Shared read/write scheduler forced disposal aggregates read scheduler, write scheduler, read executor, and write executor cleanup so an earlier failure cannot skip later phases. Executor shutdown bounded-waits before and after `shutdownNow()`, preserves interruption, and fails explicitly if a worker pool still cannot terminate.

The classpath adapter entrypoint owns runtime, legacy partition clients, RPC server, and health server through a reverse-order resource scope. Scope cleanup continues across checked exceptions, runtime exceptions, and `Error`s, clears ownership after attempting every resource, preserves the first fatal fault, and suppresses later cleanup failures so partial construction rollback cannot strand sibling adapter resources.

Classpath data-partition state machines use the same resource-scope cleanup for their RocksDB instance and options. Failed-open rollback preserves the primary startup failure while suppressing RocksDB/options cleanup faults, and terminal close still attempts `BaseStateMachine` cleanup after RocksDB cleanup faults.

Classpath adapter-runtime startup, live reload rollback, destructive partition removal, and terminal close also aggregate checked exceptions, runtime exceptions, and `Error`s across partition clients and servers. Cleanup attempts every sibling resource, clears unpublished ownership, preserves the primary fault, and suppresses later failures. Partition-server failed-start rollback likewise closes the built Ratis server across fatal startup faults before rethrowing.

Classpath partition-server wrappers keep their published `RaftServer` handle until close succeeds, so a transient close fault remains retryable instead of losing cleanup ownership. Adapter-runtime startup and reload staging publish each wrapper into their rollback maps before invoking `start()`, allowing runtime cleanup to retry a built server whose own failed-start close also failed.

Classpath adapter runtimes keep failed retired client and server cleanup obligations in private queues after routes are unpublished. A later fail-closed shutdown or cleanup pass retries those resources without republishing stale routes, and partition-client slots publish closed state only after the underlying client close succeeds.

Classpath adapter-runtime startup drains privately retained cleanup obligations before constructing any fresh partition resources. If cleanup still fails, startup stays stopped and publishes no new routes; a later retry may proceed only after the older retired resources close successfully.

Classpath adapter-runtime reload also treats staged-add rollback cleanup failure as a fail-closed condition. If an unpublished new partition resource cannot close after a later addition fails, the runtime retires its previously published routes, retries the staged cleanup during shutdown, preserves the original reload failure, and suppresses the rollback close fault.

The classpath adapter process handles the adapter-level `RELOAD_METADATA` RPC by rereading its generated config and metadata files. Reload validates the new metadata before mutation, refreshes unchanged Rockserver state machines in place, starts added partitions before exposing their clients, closes removed partitions, and rejects descriptor changes for already running partitions until an explicit partition membership workflow owns the transition.

Generated adapter publication stages metadata first and atomically replaces config last as the activation file. Config carries the canonical semantic SHA-256 of the metadata properties. Adapter startup and reload require both metadata epoch and digest equality before constructing fresh resources, so interrupted publication or tampering fails closed instead of loading a mixed config/metadata pair.

Generated adapter config-pair publication also coordinates distinct parent JVMs through a no-follow sidecar file lock under a reference-counted, lock-path-scoped JVM monitor. This prevents overlapping supervisors or rolling restarts that share generated paths from interleaving metadata staging and config activation without serializing unrelated generated config pairs during startup or reload activation.

Config-pair publication reads the existing sidecar once inside that file lock and rejects cluster changes or metadata-epoch regression before staging. Factory-owned refresh and process startup run their same-epoch adapter-partition admission in the same critical section, so a stale plan that waited behind a newer supervisor cannot overwrite the newer published pair.

The same config-pair lock remains held through post-publication activation. Process startup retains it through child launch and health confirmation, while live metadata refresh retains it through the child `RELOAD_METADATA` RPC and resolver partition refresh. Another supervisor therefore cannot interleave a newer pair between config activation and the parent state that consumes it.

Configured restart also reads the generated config and metadata sidecar together under that lock. It falls back to bootstrap only when both artifacts are absent and accepts generated metadata only when config's activated canonical semantic digest matches the sidecar. Metadata-only staging, config-only remnants, and digest-divergent sidecars fail closed rather than being promoted during restart; committed live refresh remains the repair path for interrupted staging.

The forked external-process rehearsal tampers a live node's metadata sidecar with a valid same-epoch but digest-divergent snapshot, verifies the real `RELOAD_METADATA` RPC rejects it while existing reads remain available, restores the matching sidecar, and then verifies reload succeeds.

Forked external-process rehearsal partition-client helpers retain the RPC adapter factory alongside each temporary client and close both together, so repeated proof phases do not leak factory-owned socket transports.

Committed adapter metadata refresh remains replayable across both publication phases. A retry repairs metadata staged before failed config activation, and a published pair whose adapter reload failed can be replayed before resolver exposure.

Synchronous metadata joins preserve fatal collaborator severity: adapter metadata refresh and metadata-log command submission rethrow `Error` causes directly instead of hiding them inside `CompletionException`, while runtime causes retain their existing direct propagation and checked causes remain wrapped.

Adapter-backed route refresh publishes replacement clients before best-effort stale-client cleanup, so a cleanup fault cannot make a committed route change appear to fail. Failed staged or stale client cleanup remains privately retained for later retry. Terminal resolver close separates closed-for-use state from cleanup completion, retries retained clients and factory shutdown independently, and avoids re-closing resources whose cleanup already succeeded. Process adapter client factories publish their own terminal admission state before transport shutdown, so new partition clients, metadata reloads, and retained partition-client operations fail closed after factory close even if a transport implementation does not enforce closure itself. Resolver construction failure also invokes this complete close path, giving retained staged clients a final cleanup retry before the failed resolver becomes unreachable.

Process-backed replicated-connection construction centralizes resolver ownership transfer. If final connection assembly fails after resolver creation, the resolver closes before the failure escapes, draining partition clients and the adapter factory while attaching any cleanup fault as suppressed context on the primary construction failure.

The process adapter RPC boundary caps modeled binary payloads at 47 MiB before defensive copying and rejects oversized encoded payload text before Base64 decoding. Canonical Base64 expansion therefore remains below the default 64 MiB socket frame with more than 1 MiB reserved for the line-oriented RPC envelope. Request IDs, partition IDs, error types, error messages, and health messages share line-safe validation that rejects NUL or any separator recognized by the codecs' Java `\R` grammar. RPC and health codecs iterate source ranges for those separators without regex split arrays, reject non-canonical empty logical lines, bound separator discovery to the admitted line range, match allowed keys without copying, reject unknown keys before retaining them, reject duplicate keys before replacement-value materialization, enforce field-specific character budgets before value substring creation, and bound malformed-line or unknown-key diagnostics. Health request framing requires an explicit newline terminator and rejects EOF-truncated request lines before dispatch. Health snapshots also cap diagnostic messages at 256 characters before codec or socket materialization, and health probes report malformed codec responses or invalid UTF-8 peer frames as terminal typed I/O failures instead of unchecked exceptions or retryable transport noise. Shared socket framing preflights the exact UTF-8 byte length before allocating output, then encodes and decodes strict UTF-8, rejecting oversized or malformed local Unicode text before connection and malformed peer bytes before codec parsing. Inbound readers grow cap-aware buffers that never exceed their configured frame limit and decode directly from those buffers without duplicating the complete frame first. Gateway RPC transports dispatch socket work to bounded idle-expiring daemon workers and fail returned futures immediately when client capacity is saturated instead of blocking callers or creating unbounded socket work. Canceling a gateway RPC future closes its active socket across connect, write, or read handling so worker capacity is released promptly instead of waiting for the socket deadline. Resolver close propagates through the process client factory to transport shutdown, stopping the client executor, closing active sockets, failing pending futures, and rejecting later sends. Process partition clients publish refresh-driven close state across threads before rejecting later submissions through stale retained references. Resolver refresh publishes its replacement client map before best-effort stale-client cleanup, logs cleanup failures without reporting a committed route change as failed, and rejects factory null returns while closing earlier clients staged by the failed refresh. Adapter and health failure messages are normalized into bounded streaming buffers and truncated before correlated response construction. Operation-specific request and response shape checks run before payload cloning, committed adapter responses require a positive commit index instead of accepting the zero-valued no-commit sentinel, and the process client relies on the request model's ownership copy instead of cloning once before construction and again inside the model.

Gateway RPC transport shutdown now interrupts client workers, closes registered active sockets, fails pending futures, and bounded-waits for the owned client executor to terminate before returning. Resolver close therefore does not merely publish transport closure; it verifies that gateway socket work has drained or fails explicitly. Repeated transport close calls rerun shutdown and the bounded executor drain, so an interrupted or timed-out cleanup attempt remains retryable while later submissions stay rejected.

Gateway RPC transport teardown aggregates unchecked faults across client-executor shutdown, each pending request's active-socket close and exceptional future completion, and the bounded executor drain. One abnormal socket close cannot prevent sibling pending futures from being failed or skip owned-executor termination verification.

Gateway RPC submission completes its registered response exceptionally before rethrowing an unexpected client-executor handoff runtime exception or `Error`. The normal response-completion callback therefore removes the pending registry entry even when asynchronous worker submission itself fails and no future can be returned to the caller.

Adapter-side RPC and health socket server shutdown also reruns listener closure and bounded executor drain on repeated close calls. A first interrupted or timed-out cleanup attempt leaves the endpoint terminal for new traffic but does not discard the ability to retry owned worker cleanup.

Shared adapter socket listener teardown aggregates checked exceptions, runtime exceptions, and `Error`s across listener close, accepted-socket close, accept executor shutdown, request executor drain, and operation executor drain. One cleanup fault remains primary and later faults are suppressed, while later sibling resources still receive shutdown.

Accepted adapter socket registries also aggregate checked, unchecked, and fatal close faults per socket and attempt every tracked socket before rethrowing the first fault with later failures suppressed. Successful closes leave the registry, while abnormal close failures remain tracked so repeated endpoint shutdown retries unfinished cleanup. Registration races with shutdown use the same retain-on-failure rule. One abnormal socket close cannot skip sibling accepted connections or discard its own retry obligation. The registry owns direct close semantics rather than reusing the quiet writer-timeout close helper, because accepted-socket `IOException`s must remain visible and retryable.

Adapter-side RPC and health listener startup closes the newly bound server socket and resets endpoint running state when accept-loop handoff throws a runtime exception or `Error`. A failed handoff therefore does not leak its port or prevent a corrected startup retry.

Adapter-side RPC and health accept loops also treat unexpected request-executor handoff runtime exceptions and `Error`s as terminal listener failures. The just-accepted socket is deregistered and closed before propagation, endpoint running state is cleared, health or `awaitClosed()` reports the failure, and later close releases the bound port instead of retaining a socket behind a dead accept worker.

The adapter RPC handler normalizes synchronous collaborator runtime exceptions and `Error`s into bounded `ADAPTER_ERROR` responses, preserving its `CompletableFuture` response contract for both socket-backed and direct callers. Null adapter contract results, including missing partition-operation commits or payloads, are rejected as correlated `ADAPTER_ERROR` responses before they can cross the socket boundary. Process adapter client factories classify null transport results, mismatched response IDs, and operation-incompatible successful response shapes, including non-empty metadata-reload acknowledgements, as typed `ADAPTER_ERROR` contract failures. Partition read/write submitters then translate process adapter RPC exceptions into typed `QUERY_FAILED` or `RETRY_EXHAUSTED` route failures instead of leaking runtime exceptions past the replicated public boundary. The classpath adapter metadata reload callback likewise returns failed futures for checked, unchecked, and fatal faults so the handler owns response normalization consistently.

Adapter-side RPC timeout cancellation also remains attached across synchronous handler publication. If a timed-out or interrupted request's handler ignores interruption and publishes its response future later, the socket server cancels that late future immediately. Handler response mapping propagates cancellation to the underlying partition or metadata operation future, so timeout responses do not abandon cancellable work behind an already released request-worker slot.

Timed-out or interrupted adapter RPC work also publishes operation-capacity risk before cancellation. Health reports degraded immediately when interrupt-ignoring canceled tasks retain every bounded operation slot, without waiting for another client request to be rejected, and recovers automatically after those retained workers drain.

If late response cancellation itself throws an unchecked or fatal fault, the adapter RPC server publishes request-worker failure before rethrowing from the operation worker. The endpoint therefore fails closed with degraded health and a closed listener even when the already-canceled executor wrapper can no longer deliver that cancellation fault to its original request worker.

Adapter-side RPC request workers retain the first unexpected unchecked or fatal request-processing fault, expose degraded RPC health through the separate health endpoint, stop RPC running state, and close the RPC listener to wake `accept()` instead of continuing traffic after a worker failure. Failed RPC server instances reject restart. Health request workers treat an unexpected `Error` as terminal: they publish the failure, stop running state, close the listener to wake `accept()`, and release `awaitClosed()` so the adapter process fails visibly instead of hanging behind a live port.

RPC and health worker-failure listener close also aggregate checked, unchecked, and fatal cleanup faults. The worker failure remains primary and listener-close faults are suppressed, so abnormal listener teardown cannot replace the diagnostic that initiated fail-closed shutdown.

RPC and health listener startup publish the bound listener before handing accept work to its executor. Unexpected accept-loop failure closes that stable local listener immediately, releasing the port before owner shutdown while retaining the accept failure as the endpoint diagnostic. Terminal accept and request-worker publication remains lock-free with volatile and atomic state so concurrent synchronized shutdown cannot wait on an executor worker that is blocked reacquiring the server monitor.

Gateway RPC cancellation uses a cancellation-aware response future: successful cancellation closes the active socket directly from `cancel()` after normal pending-request bookkeeping runs, while unexpected unchecked or fatal cleanup faults propagate synchronously to the canceling caller instead of disappearing inside an ignored completion-stage dependent. Gateway cleanup retains sockets privately until `Socket.isClosed()` confirms completion across cancellation, fail-closed shutdown, retry draining, and worker unwind, so checked close failures cannot discard the transport-shutdown retry handle. Transport shutdown drains retained sockets before and after request-worker termination and reports incomplete cleanup if any obligation remains.

Process client-factory response validation and public operation mapping also retain cancellation ownership. Canceling proposal, commit-wait, read, or metadata-reload futures propagates through each mapping layer to the gateway transport future, so the transport can close its active socket and release bounded client-worker capacity promptly.

Blocking partition submission keeps that ownership chain intact at its synchronous boundary. Timed-out or interrupted write proposals, point reads, and range-scan segment reads cancel their active gateway future before returning an error, so retry or caller unwind does not abandon socket-backed work until its independent RPC timeout. Cancellation cleanup faults remain suppressed on the primary timeout or interruption for diagnosis. Completed partition proposal, point-read, and range-read futures preserve direct fatal `Error` severity instead of converting it into route exceptions; checked completion causes remain contextual route failures. Failure-injection proposal mapping uses the same cancellation-preserving helper, keeping test and rehearsal clients behaviorally aligned with production clients.

Synchronous metadata stages use the same cancellation ownership rule. Metadata command proposal commit waits, committed metadata snapshot queries, and live adapter metadata reloads await their active future interruptibly. Interruption cancels that future before propagating the failure and restores the thread interrupt flag, so public async cancellation can release gateway socket work even while metadata routing is blocked.

Experimental `ClusterConnection` replica-read failover retains the same cancellation ownership while its active endpoint changes. Canceling the caller-visible future cancels the currently published endpoint request, prevents normal retry progression after terminal publication, and cancels a fallback request if cancellation races with that request's future publication. Unexpected endpoint cancellation cleanup faults remain visible to the canceling caller. Cluster connection shutdown attempts every endpoint close across checked, unchecked, and fatal failures, preserving the first cleanup fault and suppressing later endpoint-close failures.

Public gRPC future bridges preserve cancellation ownership across response error mapping. Canceling the caller-visible future cancels the underlying reactive or Guava RPC future rather than stopping at an intermediate completion stage. Guava response-mapper failures complete the public future exceptionally before fatal faults are rethrown to the callback executor, so malformed or unexpected response handling cannot leave a caller waiting indefinitely.

gRPC client shutdown now reports channel forced-shutdown and event-loop termination failures instead of logging or swallowing cleanup diagnostics. Close still attempts event-loop cleanup after channel cleanup faults, preserves the first close failure, and suppresses later cleanup faults for diagnosis.

Shared async convenience composition also retains cancellation ownership while its active stage changes. Merge-operator ensure cancels its initial remote existence check or its later upload, including an upload future published after a cancellation race. Large no-result gRPC put and merge chunking preserve cancellation through their final empty-list mapping, so canceling the aggregate response still stops the active reactive chain.

Synchronous gRPC single-request routing uses the same interruptible cancellation rule. An interrupted caller cancels its active RPC future before returning the interruption failure and retains the thread interrupt flag. Runtime and fatal asynchronous RPC failures preserve their original severity instead of being hidden by a completion wrapper.

The legacy Thrift client boundary now treats its framed protocol client as a serialized resource. Asynchronous calls run through one bounded daemon worker instead of an unbounded cached pool, while synchronous entry points serialize access to the shared transport-backed client. Canceling async calls cancels the queued or running worker task, canceling a `getRangeAsync` subscriber cancels its submitted stream worker, and close interrupts pending client work after closing the transport. Client close still shuts down the async worker and base connection if transport close throws an unchecked or fatal fault, preserving the transport failure and suppressing later cleanup faults. Key-level delete operations are not present on the generated Thrift surface, so sync and async delete/deleteMulti calls fail closed locally with `NOT_IMPLEMENTED` rather than recursing through default command dispatch.

Gateway RPC transport shutdown uses the same retained socket-cleanup queue for pending-request close failures. Completing pending responses still fails callers promptly, but unchecked or fatal active-socket close faults retain a retry handle after response bookkeeping removes the pending request; repeated transport close attempts continue cleanup.

Shared listener startup rollback also aggregates checked, unchecked, and fatal listener-close faults. The bind or accept-executor handoff failure remains primary and rollback-close faults are suppressed, while the listener still gets its close attempt before startup returns.

Health request workers own accepted sockets with try-with-resources so unchecked or fatal socket-close faults cross the same terminal worker boundary. If request processing and socket close both fail, the processing fault remains primary and the close fault is suppressed before `awaitClosed()` reports the worker failure.

RPC and health request workers remove accepted sockets from the shared registry only after `Socket.isClosed()` confirms worker cleanup completed. An unchecked or fatal try-with-resources close fault still fails the listener closed, but the unfinished socket remains tracked so owner shutdown can retry cleanup.

RPC request-executor saturation BUSY responses and health request-executor rejection closes follow the same accepted-socket ownership rule. The accept loop leaves rejected sockets registered until `Socket.isClosed()` confirms cleanup completed, so an abnormal rejection-path close cannot discard the owner-shutdown retry handle.

Unexpected RPC and health request-executor handoff failures also keep accepted sockets registered through cleanup. Socket-close faults remain suppressed on the handoff failure, and deregistration occurs only after `Socket.isClosed()` confirms completion, so owner shutdown can retry unfinished cleanup.

Accepted-socket registration racing owner shutdown follows the same ownership rule. `rejectIfClosed` tracks a newly accepted socket before attempting its immediate close, and a close failure leaves the socket registered for a later owner-shutdown retry. The registry exposes only confirmed-close deregistration to request workers.

RPC and health owner shutdown likewise retain listening-socket handles until `ServerSocket.isClosed()` confirms closure. If listener close fails before completion, a later owner close retries it while each shutdown pass still drains accepted sockets and executors.

RPC and health startup publish listener handles before binding and preserve them when rollback close does not confirm closure. Completed rollback still permits retry after bind or accept-executor handoff failure; incomplete rollback rejects restart until owner close retries cleanup, preventing a new listener from overwriting the residual handle.

Shared socket writes schedule a bounded timeout close before entering potentially blocking output-stream work. The single deadline scheduler only dispatches daemon virtual close tasks, so one slow socket close cannot delay later write deadlines. A timed writer grants its own close attempt up to 100 milliseconds of diagnostic grace before returning; completed checked, unchecked, or fatal timeout-close faults are retained as suppressed diagnostics on the primary `SocketTimeoutException`, while an unfinished close contributes an explicit suppressed incomplete-cleanup diagnostic rather than pinning the writer indefinitely.

Gateway health-probe retries preserve interruption as the primary abort signal and restore the caller thread's interrupt flag. If a transport failure triggered the interrupted retry delay, that failure remains attached as suppressed diagnostic context instead of being discarded.

Adapter health-supplier admission uses the accepted socket's monotonic request budget before submitting work. Already-expired requests return a degraded timeout response without entering the supplier executor; budget exhaustion in the narrow post-check race cancels the admitted invocation and returns the same degraded timeout response.

Adapter-backed resolver construction and refresh rollback drain staged partition clients and close the adapter factory even when client open or cleanup throws an `Error`. Terminal resolver close publishes closed state before draining owned clients, continues through later clients after a cleanup fault, still closes the factory, preserves the primary failure, and suppresses later cleanup faults.

The retired custom Raft and MicroRaft spikes have been removed from main sources, module descriptors, and the public gRPC protobuf surface. Public API servers now expose only the data API while the internal Ratis adapter remains behind the `RatisServerLifecycle` boundary.

Replica membership metadata is construction-validated before route caches, planners, or adapter metadata generators consume it. Partition descriptors and peer-change metadata commands reject duplicate peer node IDs, and non-null leader hints must target voting peers before the applier constructs replacement descriptors. Peer-change application requires the current partition route group and updates it atomically with the partition descriptor, so membership changes cannot silently mutate adapter partition metadata while leaving client route metadata stale. Each member's `appliedIndex` must not exceed its `matchIndex`, so routing and catch-up policy never see ambiguous peer ownership or a replica that claims to have applied log entries it has not matched. Cluster metadata rejects overlapping live partition key ranges and duplicate partition Ratis group IDs before generated metadata or adapter config can map ambiguous ownership or two logical partitions onto one Raft group. When a `ClusterMetadata` snapshot carries both a partition descriptor and a route group for the same partition ID, the route group must describe the same peers and leader hint as the descriptor, preventing adapter-consumed partition metadata and client route metadata from diverging inside one snapshot. Once a snapshot carries registered node metadata, every partition peer and route-group member must reference a registered node before routing or adapter config materialization can use the snapshot.

Partition routing uses a metadata-epoch-aware route cache. A cached `RouteTarget` includes the metadata epoch used to compute it, and route requests with a stale or future expected epoch fail instead of silently using a mismatched route. Updating cluster metadata to a newer epoch or changing same-epoch shard routing data clears cached routes. Same-epoch, route-neutral refreshes, such as CDC token-translation publication, update the metadata view without discarding cached targets. Epoch regressions and cluster ID changes are rejected, and failed pre-commit metadata listeners leave the previous metadata and routes active.

Write routing resolves the owning partition from the command's first mutation using the same deterministic key encoding used by state-machine validation, then validates the entire command against that partition before selecting the leader. This preserves the first-cut single-partition rule for multi-key writes and prevents a cross-partition payload from being routed to a leader before it is rejected.

The first gateway submitter keeps retry handling outside deterministic command payloads. `NotLeader` responses may carry refreshed metadata whether they are returned through the proposal future or thrown synchronously by the proposal client. Stale route epochs are retried against the current route cache epoch, future route epochs fail before sealing or route resolution, rejected refreshed metadata remains a typed route failure, proposal timeouts retry the same encoded payload while the retry budget remains, synchronous fatal proposal failures retain direct `Error` severity, and expired deadlines fail before route resolution. Duplicate retry safety comes from reusing the same `(clientId, requestId)` and encoded command payload across attempts.

Current Phase 4 gateway admission scaffolding is per partition: `PartitionWriteAdmissionController` resolves the target partition through deterministic routing, admits writes into bounded `PartitionWriteAdmissionQueue` instances, drains FIFO batches up to a configured maximum, and reports backpressure before proposal when a partition queue is full. Deadline and route-epoch checks stay in admission/routing code, while the admitted replicated payload remains a defensive copy of the deterministic command bytes.

Gateway write submission and per-partition admission now accept commands rather than independently supplied command-plus-payload pairs. The trusted boundary seals admission-time calculated keys and routing context, derives the checksummed bytes internally, reuses those stable bytes across retries, and rejects direct queue insertion of unsealed commands. A caller cannot route or queue one command while proposing unrelated bytes.

The public `ReplicatedConnection` async single-request facade owns a bounded zero-core daemon executor whose idle workers expire instead of submitting blocking routing work to the JVM common pool. Saturation fails the returned future promptly. Canceling a returned public async future interrupts its owned executor task, allowing a blocked partition submitter to cancel its active gateway future and release socket-backed work promptly. Connection close publishes terminal admission state once and rejects later sync or async submissions, while separately tracking executor shutdown, owned adapter resolver cleanup, and bounded executor termination confirmation. Repeated close calls retry only unfinished cleanup phases, so a transient resolver or drain fault cannot discard the resolver's retained adapter cleanup obligations.

Real classpath data-partition state machines also validate leader-side Ratis proposals before append: `startTransaction(...)` requires a sealed write whose admitted routing context still matches the active partition. Historical log replay remains on the separate replay validator, allowing legacy unsealed entries only while their original schema epoch is still active.

Real classpath metadata-group state machines now reject malformed metadata payloads in `startTransaction(...)` and validate stateful metadata transitions in Ratis' strictly ordered `preAppendTransaction(...)` hook. Accepted in-flight metadata commands advance a leader-local projected metadata view, which is reset from committed state across leadership changes, so two concurrent commands for one expected metadata epoch cannot both enter the log. Metadata proposals are capped at 1 MiB below the explicitly configured 4 MiB Ratis append buffer.

Metadata-group startup without a Ratis snapshot is limited to a bootstrap replay seed: metadata epoch 0, no columns, no data partitions, no CDC token translations, no schema changes, and no backup manifests. Any replica groups carried in that seed must still be bootstrap topology only, with term/config index zero and serving voters at zero match/applied progress. Generated live metadata or progressed route state must start from a Ratis snapshot instead of replaying older log entries against advanced state.

The obsolete main-source `ReplicatedWriteSyncAPI` facade has been removed. Its raw-mutation proposal behavior remains only in the test-only `LegacyReplicatedWriteSyncFixture` for classifier and transport-boundary coverage; production wiring cannot bypass metadata routing, sealing, request-ID dedupe, or persisted response semantics through that early-phase API.

The replicated public write boundary preflights commands through `ReplicatedCommandClassifier.requireSafeForReplicatedLog(...)` before mutation encoding or proposal. Local transaction/update/iterator/range handles, embedded CDC handles, local admin commands, `*_NO_WAL` modes, and direct SST ingestion modes fail with explicit public errors and do not reach command encoding or the replicated log client.

Phase 4 includes an in-JVM three-node local cluster smoke test over the adapter-boundary/state-machine scaffolding. It builds metadata from typed registration, column, and partition commands; routes a partition write through the metadata-aware submitter; applies the encoded command to three local state machines; reads the materialized value; restarts the leader core against its retained log and state; and commits another write. This is local lifecycle evidence for the Rockserver-owned routing/apply contracts, not a replacement for the later production Ratis process adapter.

## Keyspace And Partitioning

Rockserver uses sortable binary keys. A recommended encoded key:

```text
rk:v1 | kind | columnId | calculatedKey | suffix
```

Key kinds:

- `u`: user value.
- `s`: schema metadata.
- `m`: merge operator artifact and activation metadata.
- `d`: request dedupe and response cache.
- `c`: CDC cursor and event metadata.
- `a`: anti-entropy checksum metadata.

Rules:

- Preserve lexicographic ordering for range scans.
- Prefix user keys by cluster-assigned column ID.
- Never persist local RocksDB column-family handle IDs as cluster identifiers.
- Preserve `ColumnInstance.calculateKey` behavior exactly. For bucketed columns, the partition key is the calculated hashed key, not the original variable-tail key material.
- Never split a bucketed-column collision bucket across partitions. A partition boundary may fall between calculated keys only.
- Original variable-tail key bytes remain bucket payload data. Range scans over variable-tail hashed portions are ordered by calculated hash bytes, not by the original variable-length key value.
- Keep request dedupe records in the same partition as the affected user or metadata keys whenever a command must be atomic.
- Use range partitions by default. Add salting only for explicitly configured write-hot collections, and document the scan fanout cost.
- Split by bytes, entry count, write rate, scan cost, and recovery time.
- Partition metadata must scale with the number of active partitions, not total key count.

## RocksDB SSTs And Ratis Partitions

RocksDB SST files are local physical storage units. Ratis partitions are distributed ownership and consensus units.

SST files can change during compaction without cluster metadata changes. They must not become routing, membership, or failover boundaries. Partition boundaries are logical key ranges stored in replicated metadata and are used for client routing, Raft group membership, snapshot transfer, repair, and placement.

Rockserver should use RocksDB features inside each partition replica:

- a dedicated RocksDB database path per partition replica for the first production cut;
- column families inside that partition-local database where they remain useful locally;
- checkpoints;
- compaction;
- block cache and write buffer management;
- external SST workflows only when represented as replicated manifests;
- backup metadata and checksums.

The first production cut intentionally avoids a single shared RocksDB instance for many Ratis groups. A partition-local RocksDB instance makes snapshot install, peer replacement, corruption handling, and backup manifests align with the Ratis group boundary. Shared block-cache and memory budgets may still be configured at the process level, but partition data files, checkpoints, and local `lastApplied` metadata belong to one partition replica.

No RocksDB local operation may bypass the partition leader or the replicated metadata rules in replicated mode.

## Command Model

Every replicated command is a deterministic state-machine entry.

Command envelope fields:

- command version;
- `clientId`;
- `requestId`;
- target partition ID;
- schema epoch;
- column ID;
- encoded keys or key range;
- operation type;
- request type or response shape;
- durability/read policy;
- optional payload compression marker.

Command payload compatibility is explicit. `PartitionWriteCommandCodec` currently writes version 2 payloads and accepts version 1 payloads by defaulting the absent response-mode field to `NONE`; versions outside that range fail before command materialization. `ReplicatedMutationCodec` is exact-versioned at version 1 and rejects both older and future payload versions. Metadata commands currently use typed Java records rather than a separate metadata binary codec, so the compatibility surface is guarded by append-only `MetadataCommandKind` ordering, the sealed `MetadataCommand` permit list, and the `ClusterMetadata` record component schema.

Gateway-only fields are not part of the deterministic state-machine command:

- absolute deadline;
- retry budget;
- route cache hit or leader hint;
- admission queue timestamps;
- client transport metadata.

Command families:

| Rockserver command | Replicated handling |
| --- | --- |
| `put`, `delete` | Single-partition Ratis command applied through RocksDB write batch. |
| `merge` | Deterministic read-modify-write inside the partition state machine. |
| `putMulti`, `deleteMulti` | First production cut allows only bounded requests where all keys route to one partition. Cross-partition multi-key writes are rejected until an explicit bulk API with partial-success semantics exists. |
| `mergeMulti` | First production cut allows only bounded requests where all keys route to one partition. |
| `putBatch`, `mergeBatch` | First production cut allows only bounded, completed `WRITE_BATCH`/`MERGE_WRITE_BATCH` publishers that validate as one partition after expansion. SST ingestion and no-WAL modes require replicated manifests and are otherwise rejected. |
| `createColumn`, `deleteColumn` | Replicated metadata-group command. |
| `uploadMergeOperator` | Rejected until replicated artifact distribution and activation preflight can make operator uploads deterministic. |
| `checkMergeOperator` | Read-only metadata lookup over active merge-operator schema activations; returns an active version only when the committed artifact hash matches the current column schema. |
| `get`, `exists` | Linearizable by default through Ratis read path or leader query; explicit stale/local mode can query a replica after policy checks. |
| `getRange`, `reduceRange`, iterators | Partition-aware fanout with resumable cursors and backpressure. |
| Transactions and local handles | Rejected in replicated mode unless redesigned as explicit cluster-safe transactions later. |
| CDC | Emitted from the replicated apply path, not local WAL alone. |
| `flush`, `compact` | Local admin actions coordinated by replicated manifests when cluster-visible. |
| `scanRaw` | Embedded/admin only; not a replicated data API. |

Read-modify-write commands must execute under the partition state machine's serialized command semantics. A gateway must not pre-read a value, compute a result, and submit a later blind write that can overwrite concurrent committed state.

Replicated mode must reject local handles at the public routing boundary before they reach a Ratis adapter. This includes transaction IDs, `FOR_UPDATE` update IDs, server-side iterator IDs, and embedded CDC subscription handles. Replicated scans use resumable stateless cursors instead of local iterator handles.

Schema changes use a prepare/activate/fence protocol:

1. The metadata group allocates a logical column ID and schema epoch.
2. A prepare record publishes the schema without making it routable for writes.
3. Partition replicas preflight local resources and merge-operator availability for the new epoch.
4. An activate record makes the schema epoch routable.
5. Delete-column first fences new writes, waits until affected partition groups have applied the fence, then removes local data through replicated partition commands.

## Idempotency And Dedupe

Retryable writes must carry stable `(clientId, requestId)` identifiers.

`requestId` is monotonically increasing per client. The gateway derives per-partition subrequest IDs only for future explicit bulk APIs; the first production cut rejects cross-partition multi-key writes, so a retryable write has exactly one target partition.

The state machine must store enough dedupe metadata to:

- detect duplicate committed requests;
- return the original response for response-producing writes;
- avoid applying a duplicate mutation;
- expire old records safely by client, partition, and applied index.

For response-producing commands, the apply path must produce a non-null response list with exactly one response per mutation before response encoding; dedupe metadata, the encoded response, and the user mutation must be committed in the same partition entry. Cross-partition response dedupe is not allowed until a transaction protocol exists.

Dedupe expiry is lease and watermark based, not just time based. A client lease records the lowest request ID that may still be retried. The state machine may delete dedupe records only below the client's acknowledged watermark or after the client lease has expired. If a duplicate arrives after its dedupe record was legitimately expired, replicated mode returns an explicit `DEDUPE_EXPIRED`/unknown-outcome error instead of applying the mutation again.

## Read Consistency

Read modes:

- Linearizable: default. The read must observe all writes committed before the read starts.
- Session-consistent: may use the client's last observed applied index and wait locally until that index is applied.
- Stale/local: explicit opt-in for monitoring, analytics, or best-effort reads.

The Ratis read implementation must be verified in Phase 0 for the selected version and transport. If direct Ratis read-only requests do not meet latency targets, Rockserver may use leader-routed queries or a read-index-backed local query path, but only after tests prove the consistency contract.

Point reads and single-partition range reads can be linearizable by using that partition's verified Ratis read path. Multi-partition scans are not linearizable in the first production cut. They are partition-consistent and metadata-epoch consistent: each partition is read at a valid per-partition read point, and the scan fails or restarts if the metadata epoch changes incompatibly. A future globally consistent scan may add a metadata-group read barrier and per-partition read indexes, but the first production API must label multi-partition scans as non-atomic.

Current Phase 5 point-read scaffolding uses `PartitionPointReadCommand` as a deterministic query envelope, routes through `PartitionPointReadRouter` using metadata epoch, column schema epoch, and calculated-key partition ownership checks, and calls `RatisPartitionClient.queryLinearizable(...)` through `PartitionPointReadSubmitter`. The default point-read path therefore uses the verified Ratis linearizable read boundary rather than proposing a write-log command. Real classpath data-partition state machines apply the same core point-read validator before RocksDB access, so an opaque adapter caller cannot query a valid key through the wrong partition replica.

Linearizable reads are tied to metadata epochs. A route cache update to a newer metadata epoch clears previous leader targets; subsequent default reads must supply the new expected epoch and are resolved against that epoch's leader. Stale and future expected read epochs fail before adapter dispatch with direction-specific route diagnostics. Tests cover a committed value read from leader N1 at epoch 7, a metadata update moving leadership to N2 at epoch 8, and the next linearizable read observing the post-change committed value through N2 without using the write proposal path.

Explicit non-default point reads use the same command envelope and partition validation but require a caller-supplied `RoutingPolicy`. Follower and bounded-staleness reads route only to replicas that satisfy the requested minimum applied index and call `RatisPartitionClient.queryStale(...)`; local reads call `RatisPartitionClient.queryLocal(...)`. The router honors `RoutingPolicy.preferFollowers()`: follower-preferred policies try fresh followers before falling back to any fresh serving replica, while local or custom non-follower-preferred policies choose the freshest serving replica directly. These modes are not selected implicitly by the default point-read API.
If a selected adapter does not implement stale or local query support, the gateway reports a typed `QUERY_FAILED` point-read route failure with the requested policy name instead of leaking a raw unsupported-operation exception.

Single-partition range scans use `PartitionRangeReadCommand` as a deterministic, checksummed query envelope with column schema epoch, optional start/end keys, direction, and `maxEntries`. `PartitionRangeReadRouter` converts present range bounds to calculated-key bounds, requires the half-open range to be fully contained by exactly one partition at the expected metadata epoch, and routes through `ROUTED_READ` with `RoutingPolicy.linearizable()`. `PartitionRangeReadSubmitter` then calls `RatisPartitionClient.queryLinearizable(...)` and never proposes the scan to the write log. Unsupported linearizable range-query implementations become typed `QUERY_FAILED` route failures instead of raw unsupported-operation exceptions. Ranges that cross partition boundaries, use stale metadata, mismatch schema epoch, or expire their deadline fail before a Ratis client is resolved.

Public replicated range reads now decode ordered per-partition payloads through a dedicated checksummed `PartitionRangeReadResponseCodec`. `ReplicatedConnection` supports fixed-key `getRange(..., RequestType.allInRange())`, `reduceRange(..., RequestType.entriesCount())`, and `reduceRange(..., RequestType.firstAndLast())`, including async `getRangeAsync(...)`, while preserving explicit local-handle rejection. The command contract caps `maxEntries` at the same hard entry ceiling used by response decoding, so a malformed range command cannot authorize an unbounded segment scan or an over-large response decode. The classpath data-partition state machine distinguishes point and range query envelopes, scans only the addressed user-data column key range, validates schema epoch and partition-local bounds before RocksDB access, rejects variable-tail columns until bucket metadata is replicated, and fails instead of truncating when a segment exceeds `maxEntries`; the gateway also rejects any adapter segment response that returns more entries than requested.

Current multi-partition range scans use the same command envelope but plan partition-local segments at a single expected metadata epoch. The first segment keeps the user start bound, the last segment keeps the user end bound, and interior segment bounds stay open because partition boundaries are calculated-key bytes and may not be convertible back into logical `Keys`. Each segment is sent to its owning partition group through the linearizable Ratis query path and decoded in ordered per-partition sequence. This is explicitly partition-consistent and metadata-epoch consistent, not globally atomic; stateless cursor resume, split-state rejection, ordered segment collection, and fanout/resource limits are part of the Phase 5 implementation.

Distributed scan cursors are stateless client tokens. `PartitionRangeScanCursor` stores only the metadata epoch, original range command, and next partition segment index; `PartitionRangeScanCursorCodec` serializes that state with a versioned checksum. Resuming a cursor replans the partition segments from current metadata, verifies the cursor epoch still matches the route cache, and dispatches up to the caller's requested segment limit. Cursor tokens therefore survive client disconnects without server-side iterator lifetime and without exposing local transaction, iterator, or RocksDB handles. The current cursor resumes at partition-segment boundaries; finer intra-partition continuation remains tied to the later result/cursor response shape and fanout-bound work.

Scan resource limits are explicit at the replicated read boundary. `PartitionRangeScanLimits` caps partition-segment fanout per cursor page and caps bytes returned by each segment response. Non-cursor wide scans fail with `RESOURCE_LIMIT_EXCEEDED` if they would touch more partitions than the configured page fanout. Cursor pages clamp requested segment count to the configured maximum, and oversized segment responses fail before they are retained in the page result. Because cursors are stateless tokens and each segment is a finite Ratis query, replicated scans do not allocate server-side iterators and therefore have no server-side iterator lifetime to expire.

Range scans require every touched partition to be `ACTIVE`. If metadata shows `SPLIT_PREPARING`, `SPLIT_SOURCE`, `SPLIT_TARGET`, or `SPLIT_FINALIZING`, single-partition scans, wide scans, and cursor resumes fail before a Ratis client is resolved. Clients must refresh metadata and reopen or retry the stateless cursor after the split commits a stable post-split epoch. This keeps the first production cut from claiming a scan cut across moving ownership.

Wide scans must be partition-aware:

- split scan work by partition;
- bound memory and outstanding RPCs;
- preserve merged calculated-key order where the API requires it and where the key encoding supports it;
- expose resumable cursors;
- tolerate partition split/merge by validating metadata epochs.

## State Machine And Durability

The Ratis log is the authoritative replicated history. RocksDB is the local materialized state for each replica.

State-machine apply requirements:

- apply committed entries deterministically;
- persist `lastApplied` and local data atomically enough for replay and snapshot correctness;
- never acknowledge a replicated write before the selected Ratis commit/apply condition is satisfied;
- make local RocksDB WAL policy explicit per mode;
- reject embedded-only unsafe modes in replicated mode.

Current Phase 3 implementation evidence:

- failure injection exists at append, commit, apply, RocksDB write, response persistence, and response send boundaries;
- failures before RocksDB persistence do not advance in-memory `lastApplied` and do not leave partial user/dedupe/`lastApplied` state;
- duplicate `(clientId, requestId)` retries replay the stored response and advance only `lastApplied`, without applying the user mutation again;
- a retry after a committed write whose response send failed returns the originally persisted response;
- replaying the same committed command sequence into fresh partition RocksDB instances produces identical user data, dedupe state, client watermark state, and `lastApplied`;
- a partition RocksDB checkpoint and reopen preserves user data, dedupe responses, client dedupe state, and `lastApplied`.

`WriteDurability` mapping:

- Embedded mode keeps existing local RocksDB meanings.
- Replicated mode maps client-visible durability to a verified matrix of Ratis log sync, quorum commit, leader apply, RocksDB WAL, explicit flush, and response persistence.
- `QUORUM_COMMIT` is allowed only for non-response-producing writes. Exact commit-only acknowledgement requires an adapter proposal/watch path that observes quorum commit before leader apply; until that path exists, the Ratis-backed production cut conservatively acknowledges no earlier than `QUORUM_COMMIT_APPLY`.
- `QUORUM_COMMIT_APPLY` acknowledges after quorum commit and leader state-machine apply, with user data, `lastApplied`, and dedupe metadata persisted in one RocksDB write batch.
- `QUORUM_COMMIT_APPLY_FLUSH` additionally performs the configured explicit RocksDB flush/checkpoint action before acknowledgement. It is a local materialization guarantee, not a replacement for Ratis quorum durability.
- Response-producing writes always wait for leader apply because the response is created by the state machine.
- Local RocksDB WAL tuning is an implementation detail and must not weaken the replicated acknowledgement contract. If Ratis log sync or RocksDB persistence cannot be proven for a durability mode, that mode is disabled in replicated deployments.

## Snapshots, Backup, And Restore

Ratis snapshots compact Raft logs and transfer state to peers. Rockserver snapshots must integrate RocksDB checkpoints with state-machine metadata:

- partition ID;
- term/index;
- schema epoch;
- column metadata epoch;
- dedupe watermark;
- CDC watermark;
- file checksums;
- RocksDB checkpoint manifest.

The Phase 6 Ratis snapshot implementation wraps the existing RocksDB checkpoint snapshot chunks with a `RatisPartitionSnapshotManifest`. The Ratis manifest records partition ID, partition epoch, Ratis term/index, metadata epoch, schema epoch, dedupe watermark, CDC watermark, checkpoint file count/size/chunk size, a checksum of the RocksDB checkpoint manifest, and a checksum of the Ratis manifest fields. Install validates the Ratis manifest before delegating to the RocksDB checkpoint installer, rejects wrong partition or index targets, then relies on per-file chunk CRC32C checks during checkpoint materialization. After install, the Ratis state machine must initialize its in-memory `lastApplied` from the restored partition metadata before accepting replayed log entries; replay at or below the snapshot index is skipped, and later committed entries advance both RocksDB metadata and in-memory state.

Backup is a cluster workflow, not a local file copy:

- create a replicated backup manifest;
- create a metadata backup epoch that fences incompatible metadata changes while the manifest is assembled;
- snapshot each partition at a recorded Ratis term/index after it has observed the backup metadata epoch;
- export RocksDB checkpoints or SST metadata per partition;
- verify checksums;
- make restore produce new partition metadata and Raft groups deliberately.

The replicated backup manifest is represented as typed metadata: `BackupManifest` records a stable backup ID, the metadata backup epoch, the source metadata epoch, and canonical per-partition cuts sorted by partition ID. The manifest model rejects a backup epoch older than its source metadata epoch, rejects duplicate partition cuts, and requires every cut to observe the manifest backup epoch before command wrapping, generated metadata decode, export, or restore planning. When a `ClusterMetadata` snapshot carries a backup manifest from the snapshot's current metadata epoch, every cut must reference a current partition at the same partition epoch; older retained manifests are allowed to outlive later topology changes. Each partition cut records partition ID, partition epoch, Ratis term/index, and the metadata backup epoch observed by that partition. `RecordBackupManifest` still applies the command's metadata epoch fence.

Partition backup export is coordinated by `PartitionBackupExecutor`. It consumes the replicated manifest, rejects manifests above a configured partition-count limit, snapshots partitions through a rolling window capped by the configured concurrency limit, validates every Ratis snapshot manifest against its partition cut, enforces a per-partition byte budget, validates advertised file-chunk CRC32C values against the chunk bytes, and records an aggregate CRC32C over the Ratis manifest checksum, checkpoint-manifest checksum, and file-chunk checksums. Export artifacts are returned in partition-ID order for deterministic audit and restore planning. When export completes or fails, the executor interrupts peer exporters and bounded-waits for their termination before returning; cleanup diagnostics are suppressed onto the primary export failure when both paths fail. Fatal exporter `Error`s retain their original severity after sibling exporters are stopped instead of being hidden inside completion wrappers.

Backup restore is deliberately planned before any partition installer runs. `BackupRestorePlanner` accepts the source manifest, source cluster ID, target cluster ID, target metadata epoch, target partition epochs, and exported artifacts. Restore targets validate partition epoch entries before copying so null partition IDs, null epoch values, and negative epochs fail at the constructor boundary. The planner recomputes artifact byte totals and aggregate CRC32C values from supplied snapshot chunks, including file-chunk byte/CRC consistency, before planning installer work. A restore is allowed when it targets a new cluster ID, or when a same-cluster restore advances both metadata epoch and every restored partition epoch. This prevents silent local overwrite of the backed-up partition epoch. `BackupRestoreExecutor` executes only a validated plan and returns that plan with the source manifest and per-partition cuts intact for audit. Non-fatal installer failures are wrapped with target partition and source index context, while fatal installer `Error`s retain their original severity.

Restore operates on the same partition checkpoint artifact used for snapshot transfer, so schema metadata keys, user data keys, dedupe watermark keys, and replicated CDC position keys are preserved by the RocksDB checkpoint install. The restore planner still treats the result as a deliberate new cluster or new partition epoch, not a local overwrite of the original partition identity.

Because the first production cut has no atomic multi-partition writes, a backup manifest with one recorded cut per partition is an acceptable consistent cluster backup. It is not a single global transaction snapshot. Restored clusters must expose the manifest's per-partition term/index and metadata epoch for audit.

Disaster restore must not silently reuse stale node IDs, peer IDs, or partition epochs.

## Membership And Placement

Rockserver owns placement policy.

The metadata group stores:

- node identity and advertised addresses;
- failure domains;
- partition descriptors;
- group membership;
- leader preferences;
- split/merge state;
- migration plans;
- schema and column metadata.

`NodeHealthReport` is the first placement input. Each report carries node identity, health status, report time,
available and total disk, used and maximum memory, current partition replica count, and process CPU load. A node is
eligible for new replicas only when the report is fresh, the status is `HEALTHY`, available disk satisfies the
placement reserve, and memory utilization remains under the policy limit. Stale or unavailable reports cannot accept
new replicas.

`ClusterNodeMetadata` stores node identity, public endpoint, Ratis endpoint, and failure domain in cluster metadata.
`VoterPlacementPolicy` selects voters only from nodes with fresh eligible health reports, first satisfying the minimum
failure-domain count and then filling remaining voters with the least loaded candidates while keeping domain counts
balanced. The same policy validates existing voter sets against the target voter count and minimum failure-domain
spread, so unsafe same-domain voter layouts are rejected before membership changes are proposed.

Leader balancing starts as an advisory planner. `LeaderBalancePolicy` reads partition descriptors and health reports,
counts current `leaderHint` ownership, and recommends bounded leader transfers only when the source leader is overloaded
relative to an eligible target. Targets must be voting peers, fresh and healthy, serving reads, and caught up to the
current leader's match/applied indexes. Partitions that are splitting or merging are skipped, and the recommendation
contains the partition epoch so the eventual metadata/Ratis workflow can fence stale transfers.

Partition split and merge workflows are planned by `PartitionTopologyWorkflowPlanner`. A split plan validates the current
metadata epoch, source partition epoch, active source state, unused child partition IDs, a strict split key inside the
source range, and a source CDC end token before producing `SplitPartition` and `RecordCdcTokenTranslation` commands. A
merge plan validates active adjacent parents, unused merged partition ID, matching parent memberships, parent CDC end
tokens, and then emits `MergePartitions` plus a merge token translation for the resulting metadata epoch. This planner
does not copy physical RocksDB data; it builds the fenced metadata command sequence that a later admin workflow executes
after data movement has been proven complete.

Membership changes must be explicit workflows:

- add replica;
- catch up replica;
- promote if safe;
- remove old replica;
- transfer leader when needed;
- verify snapshot/install progress;
- update metadata epoch.

`PartitionMembershipWorkflowPlanner` turns those operations into fenced metadata plans. New replicas are added only as
learners, and add-replica planning rejects unregistered target nodes when the cluster metadata snapshot carries
registered node metadata. Catch-up plans target the current leader's match/applied indexes, learner promotion requires
the learner to be caught up, voter removal must preserve the old quorum size, removing the current leader requires a
caught-up serving replacement leader hint, optional leader hints during follower removal must target caught-up serving
voters, and leader transfer keeps the peer set unchanged while moving the hint only to a caught-up serving voter.
The planner rejects membership changes while a partition is already in split/merge state.

Ratis 3.2.2 exposes a `LISTENER` peer role and listener configuration APIs, and `RaftConfiguration` describes listeners as non-voting members. It does not expose an `OBSERVER` peer role in `RaftPeerRole`. This is API evidence only; it is not a proof that listener catch-up, promotion, and failure behavior satisfy Rockserver replacement requirements. `RatisNonVotingCatchUpAssessment` therefore reports non-voting online replacement as unproven until an integration proof exists. `OnlineReplacementSafetyPolicy` blocks non-voting catch-up replacement in that state and allows only conservative voter changes while an existing quorum is healthy.

Hot-partition rebalancing is driven by explicit reports and admin admission. `PartitionLoadReport` captures per-partition
load, and `HotPartitionDetector` flags fresh reports that cross operation-rate, write-rate, byte-rate, latency, or
queue-depth thresholds. `AdminRebalanceAdmissionController` does not auto-execute rebalances; it admits explicit
operator requests only when metadata epoch, partition epoch, active topology state, request shape, reason text, and
concurrency limits pass. This keeps early production behavior manual and auditable.

## CDC And Streams

Replicated CDC should derive from the committed apply path, not local RocksDB WAL alone.

Requirements:

- monotonic partition-local event positions;
- stable resume tokens;
- schema epoch in event records;
- merge result visibility matching API semantics;
- partition split/merge handling;
- bounded retention and backpressure;
- clear difference between embedded CDC and replicated CDC.

Replicated CDC resume tokens are structured as `(metadataEpoch, partitionId, partitionEpoch, appliedIndex, eventOrdinal)`. There is no total event order across partitions in the first production cut. A cluster-level CDC stream is a merge of partition streams and must expose partition-local ordering. Partition split and merge records translate parent tokens to child partition starting points through the metadata group.

Partition-local replicated CDC is emitted from the committed apply path, not from the local RocksDB WAL. A CDC-enabled partition applier adds `ReplicatedCdcEvent` records to the same partition write batch that materializes user mutations, dedupe metadata, and `lastApplied`. Events are stored under the `CDC_EVENT` keyspace by `(appliedIndex, eventOrdinal)` and encode schema epoch, column ID, calculated key, value where applicable, operation, and the structured resume token. Duplicate request replay can advance `lastApplied` but does not emit a second event for a mutation that was already acknowledged.

Cluster CDC pages are built by `ClusterCdcStreamMerger` from already ordered partition-local event lists. The merger validates each event belongs to its partition stream and that positions are strictly increasing within that partition. It emits a bounded page by cycling through partition streams in partition-ID order, preserving local order while making no claim that applied indexes are comparable across partitions. Resume state is therefore a map of partition ID to the last emitted partition token, not a single cluster-wide cursor.

Partition movement translates CDC tokens through replicated metadata records. `CdcTokenTranslation.Split` maps one parent partition end token to the starting tokens for two child partitions. `CdcTokenTranslation.Merge` maps two parent end tokens to the starting token for the merged partition. `RecordCdcTokenTranslation` fences these records by metadata epoch so consumers can move resume state across split and merge epochs without guessing local partition history. Same-epoch translation records must reference the current post-topology child or merged partition descriptors at matching partition epochs; older retained translation records remain historical and are not revalidated against the live partition map.

## Operational Model

Minimum production controls:

- per-partition leader, commit index, applied index, and lag metrics;
- node health status, report age, available disk, memory utilization, CPU load, and replica count;
- Raft log size and snapshot age;
- RocksDB memory, compaction, stalls, and block-cache metrics;
- request queue depth and deadline drops;
- dedupe-table size and expiry;
- partition split/merge progress;
- backup/restore progress;
- membership change audit log;
- corruption and replay diagnostics.

Failure handling must cover:

- leader crash before commit;
- leader crash after commit before response;
- follower lag;
- disk loss;
- corrupted local RocksDB;
- corrupted Raft log;
- network partition;
- duplicate client retry;
- node replacement;
- partial snapshot transfer;
- rolling upgrade with mixed versions.

## Security

Replicated mode must secure both client and internal traffic:

- separate public API and Ratis/internal endpoints;
- TLS/mTLS for internal Raft traffic where deployment requires it;
- node identity validation;
- administrative authorization for membership, backup, restore, and unsafe local actions;
- artifact integrity for merge operators;
- audit logs for cluster metadata changes.

Replicated mode does not allow ad hoc local `mergeOperatorClass` loading as a schema shortcut. Merge operators must be activated through cluster metadata by content hash, name, version, declared API level, and deterministic-operator policy. Every voting replica must prove it has the same artifact before the schema epoch can activate.

## Native Image

Ratis integration must be treated as JVM-first until proven otherwise.

The Phase 8 Java 25/native-image report is recorded in `REPLICATION_JAVA25_NATIVE_COMPATIBILITY.md`. The JVM path is acceptable for continued replicated-mode development behind the classpath/process adapter boundary: the native profile and inactive Ratis spike compile on GraalVM Java 25 with Enforcer dependency convergence enabled. Full native-image support remains a no-go. The current native image proceeds into baseline native-image analysis, then fails with `UnsupportedFeatureException: Type is not available in this platform: com.oracle.svm.hosted.NativeImageClassLoader$1` before any Ratis-specific native runtime behavior is validated.

Native-image support requires a separate spike for:

- Ratis dependencies and transport reflection;
- gRPC/native transport behavior;
- protobuf generated classes;
- RocksDB JNI loading;
- logging initialization;
- resource inclusion;
- snapshot and file IO paths.

If native-image support is not verified, replicated mode must be explicitly excluded from native builds.

## Defaults And Resolved Verification Choices

- The first transport choice is `ratis-grpc`; `ratis-netty` is only a fallback if Phase 0 proves gRPC dependency or runtime conflicts.
- The first production cut uses one metadata group. Schema and placement may split later only after metadata throughput or blast-radius measurements justify it.
- Default linearizable reads use the verified Ratis 3.2.2 linearizable read boundary: Rockserver routes through `RatisPartitionClient.queryLinearizable(...)` only when the target leader is currently serving reads, and the Phase 0 Ratis probe verifies ordinary `sendReadOnly` uses ReadIndex for `RaftServerConfigKeys.Read.Option.LINEARIZABLE`.
- Ratis listener APIs are present, but listener catch-up and promotion behavior is not proven; `OBSERVER` is not present in `RaftPeerRole`. Non-voting online replacement is blocked until an integration proof exists, so replacement workflows use conservative voter changes while an existing quorum is healthy.
- Response-producing write support is proven at the persistent partition state-machine layer with dedupe response persistence in the same RocksDB batch. Supported first-cut modes are previous-value/previous-presence for put/delete, delta/changed for put, and merged-value for merge only when a deterministic merge resolver is configured; incompatible operation/mode pairs are rejected before acknowledgement. Public replicated sync and async single-request writes now route through the production partition-write bridge and decode persisted committed response payloads before returning.
