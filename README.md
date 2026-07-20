# Rockserver

## Fast unary GET

Embedded databases with `database.global.enable-fast-get=true` use the owned
native GET API from `it.cavallium:rocksdbjni:11.1.2.5`. The public synchronous
API always returns an independent heap `Buf`. Unary gRPC current-value reads may
instead retain a RocksDB pin only for synchronous response framing; transactions,
bucketed columns, and proxy backends keep the ordinary implementation.

The default gRPC strategy is `automatic`: it inspects the pinned result size and
either streams it directly or uses the JNI `copyAndReset` path for independently
owned heap output.
The measured default uses pinned streaming through 128 bytes, from 512 bytes
through 4 KiB, and at or above 32 KiB. The remaining bands use independently
owned heap output. Operators can replace that table with one cutoff by setting
`-Drockserver.grpc.fast-get.pinned-min-bytes=<bytes>`. The
`rockserver.grpc.fast-get.strategy` property accepts `legacy`, `exact-heap`,
`pinned`, and `automatic`; the non-automatic values exist for the
performance matrix and operational comparison.

Run the five-round real-RocksDB/local-gRPC release gate with:

```shell
mvn -DskipTests test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=it.cavallium.rockserver.core.impl.benchmark.GrpcFastGetBenchmark
```

## Package fat jar
```shell
mvn -Pfatjar -Dagent -DskipTests clean package
```

## Package native
```shell
GRAALVM_HOME=/usr/lib/jvm/xx;JAVA_HOME=/usr/lib/jvm/xx mvn -Pnative -Dagent -DskipTests clean package
```

## Deploy the library
```shell
mvn -Plibrary -Dagent -DperformRelease=true -DskipTests -Dgpg.skip=true -Drevision=1.0.0-SNAPSHOT deploy
```
