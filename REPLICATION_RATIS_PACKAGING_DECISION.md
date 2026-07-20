# Ratis Packaging Decision

Decision date: 2026-05-20

Last evidence refresh: 2026-06-10

## Decision

Direct Ratis integration is still not accepted for Rockserver production JPMS, the root fatjar, or native-image packaging.

The selected packaging path is now a dedicated shaded adapter artifact built by the opt-in `ratis-shaded-adapter` Maven profile. This profile compiles the adapter implementation on a classpath, creates `target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar`, relocates `org.apache.ratis` to `it.cavallium.rockserver.shaded.ratis.org.apache.ratis`, merges service resources, preserves license/notice/dependency resources, excludes dependency module descriptors/signatures, and writes a single-jar adapter distribution manifest without an external jar `Class-Path`.

The existing inactive `ratis-spike` profile remains the runtime compatibility and rehearsal baseline. It keeps proving real Apache Ratis behavior through the classpath/process adapter before the shaded artifact is treated as production packaging.

The shaded adapter is not a production release artifact yet. The package build, generated-jar leak check, health smoke, classpath/process rehearsal, and shaded external-process runtime rehearsal now pass; the remaining gates are copied/shaded notice review, supervisor acceptance, and deployed network/storage rehearsal.

## Current Evidence

- 2026-06-10 full Java suite refresh passes: `mvn test` reports 1558 tests with 0 failures/errors/skips after the packaging/module-descriptor refresh and Rust workspace cleanup.
- 2026-06-10 adapter notice metadata hardening passes: `mvn -P library,ratis-spike -DskipTests compile`, `mvn -P library,ratis-shaded-adapter -DskipTests package`, `mvn -Dtest=RatisPackagingBoundaryTest test`, and `mvn -P library,ratis-shaded-adapter -DskipTests package antrun:run@run-ratis-shaded-adapter-process-rehearsal` pass after copying the RainbowGum parent POM into both adapter notice trees, asserting the inherited BSD 3-Clause license declaration is present, and rerunning the shaded external-process adapter rehearsal with the hardened notice tree.
- 2026-06-10 classpath adapter refresh passes: `mvn -P library,ratis-spike -DskipTests package` regenerates the classpath adapter distribution and `mvn -P library,ratis-spike -DskipTests compile antrun:run@run-ratis-classpath-probe` passes the adapter resource-scope, reload cleanup, timeout, config, snapshot rejection, read path, durability, three-peer, single-partition runtime, metadata group, external-process rehearsal, membership, RocksDB snapshot, and snapshot-transfer probes.
- 2026-06-10 Rust workspace refresh passes: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` pass from `rust-client/`. The refresh moves ignored member Cargo profile tables to the workspace root, removes examples-runner warnings, keeps generated code lint-clean for Java/schema-compatible names and generated enum sizes, and keeps the raw-scan decoder lint-clean while preserving the public Rust client error surface.
- 2026-06-10 packaging refresh passes: `mvn -Dtest=RatisPackagingBoundaryTest test` reports 9 tests with 0 failures/errors, `mvn -P library,ratis-shaded-adapter -DskipTests package` regenerates the shaded adapter distribution, `java -jar target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar --print-health` reports `READY`, `mvn -P library,ratis-shaded-adapter -DskipTests package antrun:run@run-ratis-shaded-adapter-process-rehearsal` verifies the shaded external-process adapter rehearsal, `mvn -P fatjar -DskipTests package` passes after aligning the fatjar/native module descriptors with the library descriptor's test-only `core.cluster.internal` export, and `JAVA_HOME=/usr/lib/jvm/java-25-graalvm PATH=/usr/lib/jvm/java-25-graalvm/bin:/usr/bin:/bin mvn -P native -DskipTests compile` passes. An initial native-profile compile attempt with the default SDKMAN `JAVA_HOME` failed before project compilation because that JDK did not provide GraalVM `native-image`.
- 2026-06-05 local evidence refresh passes: full `mvn test` reports 1128 tests with 0 failures/errors, `cargo test` passes from `rust-client/`, `mvn -P fatjar -DskipTests package` passes for the non-Ratis production fatjar, `mvn -P library,ratis-spike -DskipTests package` regenerates the classpath adapter distribution, `mvn -P library,ratis-spike -DskipTests compile antrun:run@run-ratis-classpath-probe` passes the classpath/process Ratis probes, `mvn -P library,ratis-shaded-adapter -DskipTests package` regenerates the shaded adapter distribution, the shaded adapter `--print-health` smoke reports `READY`, the shaded external-process rehearsal passes, `mvn -Dtest=RatisPackagingBoundaryTest test` passes with 9 tests, direct shaded-jar inspection confirms no manifest `Class-Path` and no unrelocated `org/apache/ratis/*` entries, and `mvn dependency:tree -Dincludes=io.grpc:*,io.netty:*,com.google.protobuf:* -Dverbose` still selects the expected gRPC/Netty/protobuf mediation.
- 2026-06-05 post-refresh metadata-log hardening rerun passes: full `mvn test` reports 1131 tests with 0 failures/errors, `mvn -P fatjar -DskipTests package` passes for the non-Ratis production fatjar, `mvn -P library,ratis-shaded-adapter -DskipTests package` regenerates the shaded adapter distribution, and the shaded adapter `--print-health` smoke reports `READY`.
- 2026-06-05 post-refresh node endpoint-invariant rerun passes: full `mvn test` reports 1143 tests with 0 failures/errors, `mvn -P fatjar -DskipTests package` passes for the non-Ratis production fatjar, `mvn -P library,ratis-shaded-adapter -DskipTests package` regenerates the shaded adapter distribution, and the shaded adapter `--print-health` smoke reports `READY` after registered node metadata became authoritative for partition and route-group member Ratis endpoints.
- `mvn -P library,ratis-spike -DskipTests compile antrun:run@run-ratis-classpath-probe` passes. It compiles Rockserver on the library module path, compiles the isolated Ratis probe on the classpath, forks the probe JVM, and verifies gRPC transport, quorum durability watches, single-partition Rockserver put/read/delete, adapter runtime metadata reload, a local three-process external adapter cluster with leader transfer and restarted-peer catch-up, membership changes, RocksDB checkpoint snapshots, and snapshot transfer into a lagging follower.
- `mvn -P library,ratis-shaded-adapter -DskipTests package` passes. It builds the selected shaded adapter artifact, uses a Java 25-capable ASM override for relocation, writes `target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar` with `Main-Class` and no manifest `Class-Path`, writes `classpath.txt` and `ratis-adapter.properties`, and extracts copied Ratis/RainbowGum notice metadata beside the distribution.
- `java -jar target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar --print-health` passes and reports adapter health `READY`.
- `mvn -P library,ratis-shaded-adapter -DskipTests package antrun:run@run-ratis-shaded-adapter-process-rehearsal` passes. It uses the shaded distribution as the child adapter process runtime and verifies a local three-process Ratis-backed Rockserver put/read, leader transfer, restarted-peer recovery, post-restart write/read through another peer, and restarted-peer catch-up read.
- `mvn -P fatjar -DskipTests package` passes when Ratis is excluded from the fatjar profile. That proves the non-Ratis production fatjar can still be packaged, but it does not accept Ratis for production packaging.
- The visible gRPC/Netty/protobuf mediation is not introduced by Ratis. The refreshed baseline selects `netty-transport-native-unix-common:4.2.12.Final` directly, with grpc-netty's older unix-common transitive edge excluded.
- `mvn -Dtest=RatisPackagingBoundaryTest test` guards against accidental direct Ratis module-path dependencies, Ratis project dependencies, retired custom Raft/MicroRaft packages or protobuf RPCs, visible Netty mediation regressions, and raw `org.apache.ratis.*` imports in production core sources.
- `RatisPackagingBoundaryTest` now also locks the `ratis-shaded-adapter` profile shape: explicit Ratis/RainbowGum extra artifacts, central version pins, project-jar classpath suppression, relocation from `org.apache.ratis`, service/resource transformers, module descriptor/signature exclusions, profile-isolated notice unpack markers, copied notice metadata extraction, generated notice metadata completeness, and generated shaded-jar leak/manifest inspection when the jar exists.
- `mvn -P library,ratis-spike -DskipTests package` passes after moving Ratis out of the root project dependency graph and into an explicit adapter-only artifact copy list. The inactive profile generates `target/ratis-adapter-distribution/` with a classpath adapter jar in `adapter/`, copied runtime dependencies in `lib/`, a generated `classpath.txt`, and a generated `ratis-adapter.properties` manifest consumed by the external-process command model. This remains development evidence only.
- The inactive adapter distribution extracts copied Ratis and RainbowGum metadata into `target/ratis-adapter-distribution/notices/`. Ratis artifacts include `LICENSE`, `NOTICE`, `DEPENDENCIES`, and `ratis-thirdparty-misc` bundled license files. RainbowGum artifacts expose Maven POM/properties metadata in the generated notice directories, but the resolved jars do not include standalone `LICENSE` or `NOTICE` files.
- Directly adding Ratis artifacts as Maven project dependencies still fails during JPMS `testCompile`: the Java module graph reads split packages such as `org.apache.ratis.server`, `org.apache.ratis.server.storage`, `org.apache.ratis.server.raftlog`, `org.apache.ratis.server.metrics`, and `org.apache.ratis.server.leader` from both `ratis.server` and `ratis.server.api`.
- `ratis-metrics-default-3.2.2.jar` cannot produce a valid automatic module name because `ratis.metrics.default` contains the Java keyword `default`.
- Native-image packaging is also not accepted. The current native report documents a no-go in baseline native-image analysis before Ratis-specific native behavior can be treated as proven.

## Rejected For The First Production Cut

- Directly placing Ratis server artifacts on Rockserver's production JPMS module path.
- Shipping the current `fatjar,ratis-spike` root shaded jar as a Ratis production artifact.
- Treating the classpath probe as a production runtime package.
- Disabling the Maven compiler module path for the root project as a packaging workaround.
- Shipping Ratis inside the native image until a separate native-image spike proves reflection, initialization, threading, gRPC, Netty, Ratis third-party, and RocksDB behavior.
- Shipping the shaded adapter before relocation, service/resource preservation, notice preservation, classpath probe/runtime rehearsal, and deployed supervisor rehearsal are all proven.

## Selected Implementation Track

The first production-candidate track is the shaded adapter artifact:

- Build with `mvn -P library,ratis-shaded-adapter -DskipTests package`.
- Keep raw `org.apache.ratis.*` imports out of `src/main`, `src/library`, `src/fatjar`, and `src/native`; raw imports are allowed only in the adapter implementation source before shading.
- Keep Ratis out of Rockserver core's JPMS module path and root Maven dependency graph.
- Relocate every Ratis class and service entry in the shaded adapter jar.
- Keep the shaded adapter jar self-contained by rejecting generated manifests with external `Class-Path` entries.
- Preserve generated `LICENSE`, `NOTICE`, and `DEPENDENCIES` evidence beside the shaded adapter distribution.
- Reuse the existing adapter manifest, launch-plan, preflight, RPC, reload, metadata, and runtime rehearsal coverage against the shaded artifact before production acceptance.

Fallback paths remain available only if the shaded artifact fails qualification: a separate non-JPMS Maven module or distribution artifact, the current classpath process adapter, or an upstream Ratis packaging change that removes the split-package and automatic-module blockers.

## Gate Closure Criteria

The release gate can be checked only after all of these are true:

- The shaded adapter has a reproducible Maven build target.
- JPMS compile and test compile pass without Ratis split-package errors.
- The production fatjar or runtime distribution starts without placing raw Ratis artifacts on Rockserver core's module path.
- The external adapter process command is produced from distribution metadata rather than hand-assembled caller input.
- Ratis adapter lifecycle tests prove startup, shutdown, failed startup cleanup, and restart with retained state.
- A deployed multi-node environment proves the same adapter process lifecycle under the chosen supervisor and network/storage layout after the configured deployment preflight has accepted the launch plans and archived the structured preflight report plus the atomically published launch-file and planned config/metadata fingerprints.
- Public metadata commands can create the metadata state and data partition state consumed by the adapter distribution without pre-published probe fixtures.
- The configured real Ratis metadata-group state machine and client enforce the generated-live snapshot requirement while feeding the public `ReplicatedConnection` metadata command hook.
- The Ratis dependency graph, visible gRPC/Netty/protobuf mediation, relocation rules, and service/resource files are locked by automated tests or build checks.
- Third-party `LICENSE`, `NOTICE`, and `DEPENDENCIES` metadata are generated or preserved in the distribution output.
- The native-image decision is explicit for the chosen packaging path: either excluded by design through a process boundary, or proven by a dedicated native-image build.

Until those criteria are met, production replicated mode remains no-go for user-facing release.
