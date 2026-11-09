#!/usr/bin/env bash
# Runs LoadBench (distributed/server/src/test/java/.../bench/LoadBench.java) with the classes of this checkout.
# Build first: mvn -o -q -f distributed/pom.xml -pl server -am test-compile -DskipTests
# Usage: distributed/bench/load-bench.sh --target replicated --root /path/on/a/real/disk [LoadBench options]
# Hot keys, on and off: add --hot-keys 4:0.5, and --server-arg --pipelined-updates for on.
set -euo pipefail
here="$(cd "$(dirname "$0")/../.." && pwd)"
server="$here/distributed/server"
cpfile="$server/target/load-bench-reactor.classpath"
if [[ ! -f "$cpfile" || "$server/pom.xml" -nt "$cpfile" || "$here/pom.xml" -nt "$cpfile" ]]; then
	# Resolved through the reactor (-am), so rockserver-core is this checkout's target/library/classes and the
	# third-party jars are those of this checkout's poms, never a rockserver-core snapshot or its pom from ~/.m2.
	# Each module of the reactor writes its own target/load-bench-reactor.classpath; the server's is the one read here.
	mvn -o -q -f "$here/distributed/pom.xml" -pl server -am test-compile dependency:build-classpath \
		-Dmdep.outputFile=target/load-bench-reactor.classpath -DincludeScope=test >/dev/null
fi
deps="$(<"$cpfile")"
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xms2g -Xmx2g \
	-cp "$server/target/test-classes:$server/target/classes:$deps" \
	it.cavallium.rockserver.distributed.bench.LoadBench "$@"
