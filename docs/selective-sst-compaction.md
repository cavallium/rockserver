# Selective live SST compaction

Rockserver exposes live SST metadata and `CompactFiles` through the embedded Java
API and gRPC. This permits a script to pack adjacent small L5 or L6 files while
normal reads and writes continue. The new operation is separate from the existing
whole-database `compact()` method; it never flushes, changes column options,
pauses background work, or compacts unrelated columns intentionally.

## API

```java
var api = connection.getSyncApi(RequestContext.batch());
long columnId = api.getColumnId("messages");
var metadata = api.getSstMetadata(columnId, 6); // -1 returns all levels

// Choose a contiguous run from metadata.files(), already ordered by user key.
var request = new SstMaintenance.Request(
    columnId,
    metadata.session(),
    selectedFiles,             // List<String> of exact SST basenames
    6,                         // both input and output level; L1+ only
    0,                         // explicit index into metadata.paths()
    1L << 30,                  // target output file size: 1 GiB
    1L << 30,                  // observed-input preflight budget: 1 GiB
    1,                         // explicit max subcompactions
    false);                    // validation-only dry run
var plan = api.compactFiles(request);
```

To run that selection, send an otherwise identical request with `execute=true`.
`getSstMetadataAsync` and `compactFilesAsync` are also available. Metadata includes:

- A per-open DB session token and the logical column ID/name.
- Configured level count, the live RocksDB base level, and ordered storage paths.
- Each SST's basename, level, path ID, byte size, raw user-key bounds as lowercase
  hex, and `beingCompacted` flag. These are physical keys, not application-decoded
  keys. Lexicographical hex ordering matches RocksDB's bytewise key ordering.

The result includes `executed`, input filenames, output filenames, observed input
bytes, actual native output bytes and elapsed native-call time. An executed
result reports the **actual native input filenames**, which can differ from the
preflight selection if RocksDB expands it. A dry run reports the observed
selection and empty outputs.

A metadata call observes current installed SSTs without reading their data blocks
or pinning them. It is not a lease on file membership. Query once for planning,
then refresh after every compaction or conflict. The native metadata capture and
base-level property are separate reads, so base level can change between them.

Metadata requests require BATCH context. Compaction requests run on Rockserver's
PHYSICAL_MAINTENANCE lane with COMPACTION accounting, independently of the caller's
ordinary workload profile. No event-loop thread performs native compaction.

## gRPC for scripts

Methods on `it.cavallium.rockserver.core.common.api.proto.RocksDBService`:

- `getSstMetadata(GetSstMetadataRequest) -> GetSstMetadataResponse`
- `compactFiles(CompactFilesRequest) -> CompactFilesResponse`

Use the checked-in `src/main/proto/rocksdb.proto`; reflection is not required.
Example metadata request JSON:

```json
{
  "columnId": "123",
  "level": 6,
  "context": {
    "profile": "BATCH",
    "workloadContractVersion": 3,
    "timeoutNanos": "30000000000"
  }
}
```

Example **dry-run** request, after replacing the session, column and filenames
with current metadata:

```json
{
  "workloadContractVersion": 3,
  "columnId": "123",
  "session": "SESSION_FROM_METADATA",
  "files": ["000123.sst", "000124.sst"],
  "level": 6,
  "outputPathId": 0,
  "outputFileSizeLimit": "1073741824",
  "maxInputBytes": "1073741824",
  "maxSubcompactions": 1,
  "execute": false
}
```

`execute` defaults to false. Sizes are bytes; protobuf JSON encodes int64 values
as strings. Metadata can exceed gRPC's usual 4 MiB response limit on large stores.
The Java client raises its inbound limit to 64 MiB for this method only. External
clients should do the same (grpcurl: `-max-msg-sz 67108864`). The server rejects
metadata responses above 64 MiB; use per-level requests where necessary. This is
not a streaming/paginated inventory API.

This additive API requires an updated server. Older servers report UNIMPLEMENTED;
the workload contract remains version 3. Thrift explicitly reports NOT_IMPLEMENTED
for these operations. The Rust client has not gained convenience methods; generic
gRPC clients can use the protobuf directly.

## Selecting files properly

1. Pick a level and inspect its key-ordered file list. Select **consecutive files**,
   not simply the smallest filenames or sizes. A large or busy intervening file
   ends a small-only group. Filenames do not encode key order.
2. Bound both input bytes and file count. The API permits at most 256 input files
   per request; the example defaults to 64. Native sanitization examines metadata
   while holding the DB mutex, so a huge list is undesirable even if its SSTs are
   tiny. A reasonable initial experiment is one subcompaction and a 1 GiB observed
   input/output target, not an established production-safe limit.
3. Do not split a shared user-key boundary. Include the entire boundary group or
   skip it if it exceeds the selected budget. The server rejects non-contiguous
   selections and boundaries touching unselected SSTs at validation time.
4. Select output storage explicitly. A source with one path uses 0. For the
   current tiered messages-v2 configuration, the capacity tier is path 1; verify
   the live mapping before use. Never prepend/reorder paths or assume output
   path 0 is always the correct destination.
5. Dry-run, examine the result, then execute one job and await its response.
   Refresh metadata before selecting the next group. Monitor foreground latency,
   flush/compaction debt and free space outside this example before admitting
   additional jobs. The example is a selection tool, not an autonomous production
   performance controller.

Compression inherits the live column-family policy. The explicit output-file
limit avoids the native API's effectively unlimited default. Output sizing is a
compaction target, not a hard physical byte cap or guaranteed number of files.

### Limits and races

The server rechecks session, file membership, level, adjacency, shared boundaries,
busy state, output path and observed input bytes immediately before the native
call. A changed session requires a new plan. Missing/moved/busy inputs report
`COMPACTION_CONFLICT` (gRPC ABORTED); malformed or oversized preflight selections
report INVALID_ARGUMENT. Native execution failures report `COMPACTION_FAILED`.

**`maxInputBytes` is not a hard native job limit.** A concurrent compaction/flush
can change the Version after the Java check. RocksDB can then add required files
while sanitizing the request under its own mutex. That preserves database
correctness but may increase work. A strict post-expansion byte/file cap needs a
native extension checked under that same mutex before reservation/execution. This
implementation does not claim that guarantee and does not disable automatic
compaction to manufacture it. A successful dry run reserves nothing.

The wrapper holds DB and column leases until the native operation returns,
including after caller cancellation; deleting the column or closing the DB must
wait for that ownership to end. It holds no global column-edit lock during the
rewrite. Native RocksDB retains its normal snapshot, sequence, compaction conflict,
MANIFEST/SuperVersion installation and obsolete-file handling.

There is no per-job cancellation/status protocol in this first API. The deployed
JNI `CompactionOptions` does not expose C++'s per-call canceled flag. Interrupting
a Java future, timing out or disconnecting **does not mean compaction stopped**.
The Java gRPC client does not automatically retry `compactFiles`. A script must
stop on an uncertain RPC outcome and inspect fresh metadata before deciding what
to do; do not loop blindly over the same request. This is a synchronous completion
RPC, not a durable job receipt surviving process restart.

## Example script

`scripts/compact-adjacent-ssts.py` uses Python's standard library and `grpcurl`.
It chooses a bounded small-only run using potential file reduction per observed
input byte, asks the server to validate it, and prints the plan. It never skips
intervening files, silently chooses a storage path, or retries failed RPCs.

```bash
# Read-only metadata and server validation; no compaction.
python3 scripts/compact-adjacent-ssts.py 127.0.0.1:5333 COLUMN_ID \
  --plaintext --level 6 --output-path-id 0

# After reviewing the selection: execute at most one job.
python3 scripts/compact-adjacent-ssts.py 127.0.0.1:5333 COLUMN_ID \
  --plaintext --level 6 --output-path-id 0 --execute --max-jobs 1
```

Omit `--plaintext` for TLS endpoints. Increase `--max-jobs` only with an external
admission policy; each successful job causes a fresh inventory query. The script
only targets L5/L6; the underlying API allows any L1+ level.

During a physical SST migration, consolidating the source creates replacement
SSTs that must be staged again. Migration pins and snapshots can retain old bytes
long after RocksDB retires them. Budget that duplication, or consolidate after
installation/cutover in a separately authorized operation.

## Validation

See [the validation receipt](selective-sst-compaction-validation.md) for commands,
coverage, source hashes and the wider-suite limitations.
