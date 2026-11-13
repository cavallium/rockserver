# Qualified legacy lifecycle overlay

This branch preserves the Java 25 / `1.0.0-SNAPSHOT` service line. **Do not merge it into modern master or deploy a fully shaded build from this branch.** The qualified artifact is an overlay on the exact deployed original JAR, not a dependency upgrade.

`manifest.json` pins original `ce384b…`, deployed overlay `59b885…`, five replaced implementation classes and three added telemetry classes. Every other original entry, including ClientBuilder, API/protocol/configuration classes and JNI, remains byte-identical. The recipe performs no deployment or database operation and refuses unqualified inputs/output.

## Reproduce

Obtain the original JAR matching the full manifest SHA. Use two separately retained class directories:

- `instrumented`: Java 25 classes from `15b504c8d56df62846154fb0d2478191aceafe8b`.
- `cutoff`: classes from `6ee13f98a579dbe256288795ece3646f86d05a73` (cutoff fix plus deterministic regression test).

Both pinned source commits already set Maven compiler plugin **3.15.0** in `pom.xml`; the unmodified `a771fcd` base used 3.14.0 and is not a class-input revision for this recipe. The qualified builds used Temurin 25.0.5 and the exact JNI `.12` snapshot in the manifest. Select that JDK explicitly in each isolated checkout (the path below is the qualification machine’s installation):

```sh
JAVA_HOME=/home/cavallium/.sdkman/candidates/java/25.0.5-tem \
PATH="/home/cavallium/.sdkman/candidates/java/25.0.5-tem/bin:$PATH" \
mvn -Pfatjar -DskipTests compile
```

This produces `target/fatjar/classes`; no compiler override is needed at either pinned revision. Compilation is not qualification: every consumed class must match its manifest SHA. No other compiled classes enter the overlay.

Only EmbeddedDB and RWScheduler are taken from the cutoff build. The other six classes retain the previously qualified bytes, including nested-class debug line metadata. Do not substitute newly compiled nested classes or normalize unexplained bytecode differences.

```sh
python3 ops/legacy-lifecycle/reproduce_overlay.py \
  --original /private/original-ce384.jar \
  --instrumented-classes /private/instrumented/target/fatjar/classes \
  --cutoff-classes /private/cutoff/target/fatjar/classes \
  --output /private/legacy-lifecycle-overlay.jar
```

Original ZIP entry metadata/order and the three added entries' metadata are pinned. The final archive must match the full `59b885…` SHA; a compression/toolchain mismatch fails closed and removes only the newly created output. An existing output is never overwritten.

Qualification: 56 focused tests, 83 packaged original-client/native checks, 3429 full tests (zero failures/errors; one opt-in benchmark skipped); four original/overlay restored-data open/read/reopen phases; production graceful-exit143 rollout followed by unchanged old-ABI metadata/record reads and 14 fresh lifecycle series. Metrics describe registered overdue resources and failed cleanup attempts, not every possible native leak.
