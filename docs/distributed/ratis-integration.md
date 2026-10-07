# Ratis integration boundaries

These are verified released-source/API facts and initial implementation choices,
not final distribution or performance acceptance.

## Completed apply and reads

Ratis invokes data apply in order but permits asynchronous completion. Its
configuration/metadata callback is `notifyTermIndexUpdated(term,index)` and has
no returned future. The read completion queue advances by maximum observed
completion, so simply returning queued data futures would allow a later
configuration callback to overtake earlier native work. The current adapter
completes native apply synchronously and persists non-data positions through the
same storage lane. Future asynchronous apply must order both queries and these
callbacks behind contiguous completion, without serializing proposal RTTs.

Source: [entry dispatch](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/RaftServerImpl.java),
[updater](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/StateMachineUpdater.java),
[read completion](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/ReadRequests.java).

## Local seed and snapshot identities

Ratis persists initial membership, invokes state-machine initialization, then
initializes its log from the advertised snapshot index. A fresh child group
therefore starts its own consensus position at `TermIndex.INITIAL_VALUE (0,-1)`.
Its parent's freeze position belongs in separate seed/recovery provenance.
Already-local seed data is initial state, not an advertised child Raft snapshot.
The child must refuse voter startup until its exact durable seed is verified,
and retain parent recovery references until a real child image covers it.

Source: [ServerState initialization](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/ServerState.java),
[initial position](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server-api/src/main/java/org/apache/ratis/server/protocol/TermIndex.java).
This shape is source-supported; the real local-seed integration test is pending.

## Snapshot installation and configuration

The released leader transports its **current** Raft configuration in an install
request, which may be newer than the snapshot cut. The receiver invokes
`notifyConfigurationChanged` for this transport configuration, outside ordinary
committed entry apply and potentially with synthetic term zero. Keep it separate
from the native cut configuration. Do not advance native A from that callback,
and do not overwrite Ratis consensus membership with the older image header.
Normal committed configuration entries persist their cut metadata atomically
with the real completed position; later tail replay closes the native gap.

Source: [LeaderProtoUtils](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/LeaderProtoUtils.java),
[SnapshotInstallationHandler](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/SnapshotInstallationHandler.java).

Remote installation pauses the state machine, finalizes the downloaded snapshot,
then schedules reload; RPC SUCCESS precedes native `reinitialize()` completion.
The replica must stay fenced until its actual native installation is complete.
`SimpleStateMachineStorage` caches its latest image, so reload must explicitly
call `loadLatestSnapshot()`. Discovery reads the MD5 sidecar without verifying
image bytes, and can select checksumless files. The adapter must validate bytes,
image identity, schema and its internal checksum before advertising or installing
an image. A recovered native A alone is never permission to truncate Raft logs.

Source: [SimpleStateMachineStorage](https://github.com/apache/ratis/blob/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/statemachine/impl/SimpleStateMachineStorage.java).
These installation requirements are pending implementation and fault tests.

## Dependencies and runtime

Ratis's overlapping automatic-module packages require the optional classpath
adapter. `ratis-metrics-default` must be included explicitly; it is only a test
dependency of upstream server. Released Ratis thirdparty-misc 1.1.0 relocates
protobuf, gRPC and Netty into Ratis namespaces, including native filenames.
The nominal upstream bytecode target is Java 8; actual JDK 25 execution passed
the local process gate. The source/API investigation correlated Maven's released
ServerState source with the release tag and checked actual cached jar signatures.

Source: [published parent](https://repo.maven.apache.org/maven2/org/apache/ratis/ratis/3.3.1/ratis-3.3.1.pom),
[shading](https://repo.maven.apache.org/maven2/org/apache/ratis/ratis-thirdparty-misc/1.1.0/ratis-thirdparty-misc-1.1.0.pom).
Upstream licenses are Apache-2.0. Local jar SHA-256 identities are recorded in
`ratis-artifacts.sha256`; every file matched its Maven-cached published SHA-1.
Maven Central did not provide `.jar.sha256` or `.jar.sha512` for the server jar.
This check establishes artifact identity/integrity, not detached-signature
verification. Release signature verification remains a packaging gate.


## Fresh read rounds in the optional runtime

The optional adapter now uses `rockserver-ratis-server:3.3.1-rockserver.1`, a
single server implementation artifact assembled from the checksum-pinned 3.3.1
release with replacement `LeaderStateImpl` and `ReadIndexHeartbeats` classes.
The stock server is a provided compile input, not a second consumer runtime
implementation. Unchanged classes and upstream LICENSE/NOTICE are retained.
Standalone's reactor and JPMS dependencies are unchanged. Replacement classes
require the selected JDK 25; the historical process gate used stock 3.3.1 and
does not establish acceptance of this replacement.

Reads enter bounded sealed batches. Each batch samples current-term/configuration
identity and all next heartbeat call IDs before any trigger, then requires fresh
successful replies with sufficient follower commit and the exact appender
identity. Actual Ratis old/new joint-majority predicates govern completion.
A later arrival or warmed committed index never reuses an old barrier. The
existing completed-apply wait remains between the round and native query.
The 32-appender bound covers the union of two bounded 16-replica configurations;
oversize context failure is recoverable after topology shrinks.

The original deadline covers queued, active and decided-but-unpublished waiters,
including invalidation and later capacity failure. Successful publication also
checks that deadline before resolving its promise. Failures leave timer threads
through individual virtual-thread publications, with a shared 4,096-publication
budget held until synchronous observers return. Cancellation and rejected
publication preserve/release ownership exactly once. This bounds publication
work; it does not account for every request payload or replace the architecture's
remaining node/pool/tenant admission and measured-density requirements.

Ordinary deterministic round tests and runtime classpath checks pass. They cover
freshness, old replies, synchronous triggers, real joint quorum, completed apply,
startup/minimum indexes, decided deadlines, shutdown/rejection/cancellation,
blocked observers and node publication-budget conservation. Multi-process fresh
read acceptance, independent-host failure domains, and representative resource
performance remain unverified. No excluded network/failure exercise was run.
