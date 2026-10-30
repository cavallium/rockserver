#!/usr/bin/env bash
# Runs LoadBench (distributed/server/src/test/java/.../bench/LoadBench.java) with the classes of this checkout.
# Build first: mvn -o -q -f distributed/pom.xml -pl server -am test-compile -DskipTests
# Usage: distributed/bench/load-bench.sh --target replicated --root /path/on/a/real/disk [LoadBench options]
# Hot keys, on and off: add --hot-keys 4:0.5, and --server-arg --pipelined-updates for on.
set -euo pipefail
here="$(cd "$(dirname "$0")/../.." && pwd)"
server="$here/distributed/server"
cpfile="$server/target/load-bench.classpath"
if [[ ! -f "$cpfile" || "$server/pom.xml" -nt "$cpfile" ]]; then
	mvn -o -q -f "$server/pom.xml" dependency:build-classpath -Dmdep.outputFile="$cpfile" -Dmdep.includeScope=test >/dev/null
fi
# Use this checkout's core build, never whatever rockserver-core snapshot is in ~/.m2.
deps="$(tr ':' '\n' < "$cpfile" | grep -v '/rockserver-core/' | paste -sd:)"
core="$here/target/library/classes"
[[ -d "$core/it" ]] || core="$(ls "$here"/target/rockserver-core-*.jar | head -1)"
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xms2g -Xmx2g \
	-cp "$server/target/test-classes:$server/target/classes:$core:$deps" \
	it.cavallium.rockserver.distributed.bench.LoadBench "$@"
