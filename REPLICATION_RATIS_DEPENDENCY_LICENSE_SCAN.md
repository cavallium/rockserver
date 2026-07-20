# Ratis Dependency And License Scan

Scan date: 2026-06-05

Scope:

- Maven profile: `ratis-spike`.
- Maven profile: `ratis-shaded-adapter`.
- Ratis version: `3.2.2`.
- Adapter-only copied non-Ratis artifacts: `io.jstach.rainbowgum:rainbowgum-core`, `rainbowgum-annotation`, `rainbowgum-pattern`, and `rainbowgum-slf4j` at `0.8.2`.
- Evidence sources: Maven dependency trees for the baseline visible graph, inactive adapter artifact copy lists, shaded adapter relocation/resource configuration, generated adapter notice directories, direct shaded-jar inspection, local artifact POM license declarations, and `META-INF/LICENSE`, `META-INF/NOTICE`, and `META-INF/DEPENDENCIES` entries in resolved Ratis JARs.

Commands run:

- `mvn dependency:tree -Dincludes=io.grpc:*,io.netty:*,com.google.protobuf:* -Dverbose`
- `mvn -Dtest=RatisPackagingBoundaryTest test`
- `mvn -P library,ratis-spike -DskipTests package`
- `mvn -P library,ratis-shaded-adapter -DskipTests package`
- `mvn -P library,ratis-shaded-adapter -DskipTests package antrun:run@run-ratis-shaded-adapter-process-rehearsal`
- `mvn -P library,ratis-spike -DskipTests compile antrun:run@run-ratis-classpath-probe`
- `mvn -P fatjar -DskipTests package`
- `java -jar target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar --print-health`
- `unzip -p target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar META-INF/MANIFEST.MF`
- `zipinfo -1 target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar org/apache/ratis/*`
- `zipinfo -1 target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar it/cavallium/rockserver/shaded/ratis/org/apache/ratis/server/RaftServer.class`

## Dependency Graph

The inactive classpath profile does not add Apache Ratis artifacts as Maven project dependencies. Instead, the adapter distribution copies these explicit artifacts into `target/ratis-adapter-distribution/lib`.

The selected shaded profile also keeps Apache Ratis out of the project dependency graph. It compiles the adapter against a staged classpath, then feeds the same explicit Ratis and RainbowGum artifact set to `maven-shade-plugin` as non-transitive `extraArtifacts` for relocation into `target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar`:

- `org.apache.ratis:ratis-server:3.2.2`
- `org.apache.ratis:ratis-thirdparty-misc:1.0.11`
- `org.apache.ratis:ratis-proto:3.2.2`
- `org.apache.ratis:ratis-common:3.2.2`
- `org.apache.ratis:ratis-client:3.2.2`
- `org.apache.ratis:ratis-server-api:3.2.2`
- `org.apache.ratis:ratis-metrics-api:3.2.2`
- `org.apache.ratis:ratis-grpc:3.2.2`
- `org.apache.ratis:ratis-metrics-default:3.2.2`
- `io.jstach.rainbowgum:rainbowgum-core:0.8.2`
- `io.jstach.rainbowgum:rainbowgum-annotation:0.8.2`
- `io.jstach.rainbowgum:rainbowgum-pattern:0.8.2`
- `io.jstach.rainbowgum:rainbowgum-slf4j:0.8.2`

Ratis does not add visible `ratis-netty`, `io.grpc`, `io.netty`, or `com.google.protobuf` artifacts directly. The Ratis gRPC transport uses `ratis-thirdparty-misc`, which shades/bundles its transport dependencies under the Ratis third-party namespace.

The shaded adapter relocates `org.apache.ratis` to `it.cavallium.rockserver.shaded.ratis.org.apache.ratis`, merges Java service resources, appends `META-INF/DEPENDENCIES`, preserves Apache license and notice resources through shade transformers, excludes dependency module descriptors/signatures, and extracts copied source notice metadata beside the distribution for review.

## Mediation Findings

- `mvn -P library,ratis-spike -DskipTests package` succeeds and keeps Ratis artifacts out of JPMS test compilation.
- `mvn -P library,ratis-shaded-adapter -DskipTests package` succeeds with a Java 25-capable ASM override for Shade relocation. The `ratis-shaded-adapter` profile is the selected release-packaging track, and `RatisPackagingBoundaryTest` locks the shade extra-artifact list, relocation, resource transformers, notice extraction, and raw-import ban for production core sources.
- `mvn -P library,ratis-shaded-adapter -DskipTests package antrun:run@run-ratis-shaded-adapter-process-rehearsal` succeeds and uses the shaded distribution as the child adapter process runtime for the local three-process Rockserver/Ratis put/read/restart/catch-up rehearsal.
- The inactive adapter package extracts copied Ratis and RainbowGum metadata under `target/ratis-adapter-distribution/notices/`. Ratis artifacts include `LICENSE`, `NOTICE`, `DEPENDENCIES`, and `ratis-thirdparty-misc` bundled license files. RainbowGum artifacts expose Maven POM/properties metadata in the generated notice directories, but resolved jars do not include standalone `LICENSE` or `NOTICE` files.
- The shaded adapter package extracts equivalent copied Ratis and RainbowGum metadata under `target/ratis-shaded-adapter-distribution/notices/`, while the shaded jar itself uses shade resource transformers for service, license, notice, and dependency resources. The classpath and shaded notice unpack executions now use separate dependency-plugin marker directories, and `RatisPackagingBoundaryTest` checks generated metadata presence when the distributions exist, preventing one profile run from causing the other profile to skip extraction.
- Direct shaded-jar inspection confirms the manifest has `Main-Class` and no external `Class-Path`, no unrelocated `org/apache/ratis/*` entries are present, and relocated Ratis classes exist under `it/cavallium/rockserver/shaded/ratis/org/apache/ratis/`.
- Local artifact inspection still shows `ratis-metrics-default-3.2.2.jar` cannot produce a valid automatic module name because `ratis.metrics.default` contains the Java keyword `default`.
- Existing JPMS/fatjar risk remains: direct in-module production integration is still blocked by Ratis split packages and automatic-module behavior. Keep the classpath/process adapter boundary.
- The visible gRPC/Netty/protobuf tree is not introduced by Ratis. Running the same tree without `ratis-spike` shows the baseline mediation is now explicit: Rockserver excludes grpc-netty's `netty-transport-native-unix-common` transitive edge and adds a direct `netty-transport-native-unix-common:${netty.version}` dependency.
- The refreshed baseline resolves visible Netty transport artifacts to the intended `4.2.12.Final` family, including `netty-transport-native-unix-common:4.2.12.Final`, without a remaining visible unix-common 4.1 edge.
- `RatisPackagingBoundaryTest` guards the current boundary by checking production module descriptors, absence of Apache Ratis project dependencies, inactive-profile confinement for copied Apache Ratis artifacts, central `${ratis.version}` and `${ratis.thirdparty.version}` pins, absence of retired custom Raft/MicroRaft source packages and public protobuf RPCs, and the explicit grpc-netty unix-common exclusion.

## License Findings

No new Ratis-profile license blocker was found in the local artifact metadata, subject to preserving third-party notices during packaging.

| Component set | License evidence | Release note |
| --- | --- | --- |
| Apache Ratis `3.2.2` artifacts | Ratis parent POM declares Apache License 2.0. Resolved Ratis JARs include `META-INF/LICENSE`, `META-INF/NOTICE`, and `META-INF/DEPENDENCIES`. | Acceptable for the spike and future adapter packaging if notices are preserved. |
| `ratis-thirdparty-misc:1.0.11` | Ratis third-party parent POM declares Apache License 2.0. The JAR includes dependency and notice files, plus specific license files for bundled BoringSSL/Tomcat Native material. | Acceptable only if shaded/bundled dependency notices are preserved in distribution output. |
| Ratis third-party dependency list | `META-INF/DEPENDENCIES` lists Apache-2.0, BSD-3-Clause, and MIT-licensed dependencies, including gRPC, Netty, Protobuf, Gson, Guava, Dropwizard Metrics, PerfMark, Checker Qual, Animal Sniffer annotations, and SLF4J. | No copyleft blocker found in the bundled Ratis third-party dependency list. |
| RainbowGum `0.8.2` profile dependencies | RainbowGum Maven parent POM declares BSD 3-Clause. Resolved RainbowGum jars expose Maven POM/properties metadata in generated notice directories, but no standalone `LICENSE` or `NOTICE` files were present in the extracted jar metadata. | Acceptable for the classpath probe/logging binding; release review should decide whether to add a flattened BSD 3-Clause license/notice bundle for distribution. |
| Visible gRPC/Netty/Protobuf baseline dependencies | gRPC POMs declare Apache-2.0. Netty parent POMs declare Apache-2.0. Protobuf parent POM declares BSD-3-Clause. | No Ratis-specific blocker; visible Netty native transport mediation is explicitly pinned to the `4.2.12.Final` family. |
| Jakarta Annotation API | Ratis metadata references `jakarta.annotation-api:1.3.5`; Rockserver already pins `jakarta.annotation-api:3.0.0`. The resolved project artifact declares EPL 2.0 and GPL2 with Classpath Exception. | Not introduced by Ratis profile, but release legal review must already account for the existing direct dependency. |

## Release Decision

Dependency/license scan result: conditional pass for the inactive `ratis-spike` profile and conditional packaging-start approval for the opt-in `ratis-shaded-adapter` profile.

Packaging result: the shaded adapter is now the selected production-candidate packaging path, but still no-go for production release distribution until copied/shaded notice review, supervisor acceptance, and deployed-environment rehearsal conditions in `REPLICATION_RATIS_PACKAGING_DECISION.md` are met. The classpath probe remains accepted as development/runtime evidence, not as a release package by itself.

Conditions before production distribution:

- Keep Ratis behind the shaded or classpath/process adapter boundary; do not place Ratis server artifacts directly on the production JPMS module path.
- Preserve `LICENSE`, `NOTICE`, and `DEPENDENCIES` metadata from Ratis and `ratis-thirdparty-misc` in the shaded adapter jar and beside any copied adapter distribution.
- Preserve or add release-reviewed BSD 3-Clause license/notice material for bundled RainbowGum artifacts, because the resolved jars expose Maven metadata but no standalone license/notice files.
- Keep the explicit visible Netty mediation pin covered by dependency-tree or enforcer checks before producing a fatjar or production runtime image.
- Complete release notice review, including any required flattened notice bundle beyond the generated copied-artifact metadata, before a production release candidate. Automated checks now verify metadata presence, but they do not replace legal/release review of the combined notices.
