# Java 25 And Native Image Compatibility

Checked on May 20, 2026.

## Runtime Matrix

| Path | Result | Notes |
| --- | --- | --- |
| Ambient JVM | Pass for ordinary Java 25 execution | `java -version` is Temurin OpenJDK 25+36 LTS. |
| GraalVM JVM | Pass for native-profile compilation | `/usr/lib/jvm/default/bin/java -version` is Oracle GraalVM 25.0.2+10.1. |
| Native profile compile | Pass with current known build caveats | `JAVA_HOME=/usr/lib/jvm/default PATH=/usr/lib/jvm/default/bin:$PATH mvn -P native -DskipTests compile` succeeds with Maven Enforcer dependency convergence enabled. |
| Native profile plus Ratis spike compile | Pass with adapter caveats | `JAVA_HOME=/usr/lib/jvm/default PATH=/usr/lib/jvm/default/bin:$PATH mvn -P native,ratis-spike -DskipTests compile` succeeds with Maven Enforcer dependency convergence enabled and runs the classpath probe compile step. |
| Full native image | Fail | `JAVA_HOME=/usr/lib/jvm/default PATH=/usr/lib/jvm/default/bin:$PATH mvn -P native -Dmaven.test.skip=true -DskipNativeTests=true package -e` fails during native-image analysis. |

The native-image tool is Oracle GraalVM Native Image 25.0.2+10.1.

## Build-Gate Findings

Running the native profile under the ambient Temurin `JAVA_HOME` is not sufficient because the native Maven plugin needs GraalVM. Running with `JAVA_HOME=/usr/lib/jvm/default` reaches the native build.

Maven Enforcer dependency convergence now passes for both `-P native` and `-P native,ratis-spike` compile paths. The resolved baseline pins `slf4j-api`, `error_prone_annotations`, `reactive-streams`, visible Netty artifacts, `fastutil`, `jakarta.annotation-api`, and JetBrains annotations through dependency management.

Native source compilation required these Netty 4.2 compatibility cleanups:

- `src/native/java/module-info.java` now reads `io.netty.codec.compression`.
- stale Netty reference-count field-offset substitutions were removed because Netty 4.2 uses `RefCnt` fields instead of the old static offset fields.
- the stale `NioEventLoop.newTaskQueue0(int)` substitution was removed because Netty 4.2 no longer exposes that method.
- SSL/OpenSSL substitutions were updated for Netty 4.2 constructor and field shapes, including `ResumptionController`, SNI server names, OpenSSL credential lists, and the newer OpenSSL default protocol fields.

After those source fixes and dependency convergence pins, the full native image proceeds past Maven Enforcer and source compilation, then fails during native-image analysis:

```text
Error: com.oracle.graal.pointsto.constraints.UnsupportedFeatureException:
Type is not available in this platform:
com.oracle.svm.hosted.NativeImageClassLoader$1
```

The same native-image run also logs a build-time network-interface warning from Netty initialization under the sandboxed environment:

```text
java.net.SocketException: Operation not permitted (Socket creation failed)
```

That warning is not the final terminating exception; the terminating exception is the unsupported hosted native-image classloader type during analysis.

## Ratis Decision

Java 25 runtime compatibility is acceptable for the current Ratis adapter plan when Ratis remains behind the shaded or classpath/process boundary. The selected shaded adapter path keeps raw Ratis artifacts off Rockserver core's JPMS module path, while the `native,ratis-spike` compile path works on GraalVM Java 25 with the already documented `ratis-metrics-default` automatic-module-name warning.

The shaded adapter build requires a Java 25-capable ASM override on `maven-shade-plugin:3.6.0`; without it, relocation fails on class file major version 69. The accepted profile pins `org.ow2.asm` `asm`, `asm-commons`, and `asm-tree` to `${shade.asm.version}` for the shaded adapter execution.

Native-image compatibility is not proven. The full image is currently blocked by baseline native-profile issues before any Ratis-specific native runtime behavior is validated. Replicated mode must stay excluded from native-image builds unless a later spike resolves:

- hosted native-image analysis failure under GraalVM 25;
- Netty 4.2 native transport reachability and build-time initialization behavior under GraalVM 25;
- updated reachability metadata for gRPC/Netty 4.2 instead of fallback metadata from older Netty 4.1 versions;
- Ratis transport, snapshot, file IO, threading, logging, and shutdown behavior inside a native image.

## Go/No-Go

- Java 25 JVM path: go for continued replicated-mode development behind the adapter boundary.
- Direct Ratis on the production JPMS module path: no-go, unchanged from Phase 0.
- Native-image replicated mode: no-go until a dedicated native-image spike resolves the baseline native profile and then proves Ratis-specific behavior.
