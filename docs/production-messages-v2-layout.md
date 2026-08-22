# Production `messages-v2` layout

`messages-v2` is a new column family for a later, explicitly controlled migration. The existing
`messages` column remains authoritative until an independently validated cutover.

## Storage and cache contract

- Path ID 0 is the bounded NVMe upper-level tier:
  `/media/yotsuba4/hot-storage/rockserver-messages-v2-upper`, target `1TiB`.
- Path ID 1 is the unbounded HDD capacity tier:
  `/media/yotsuba4/sst-compressed-heavy/rockserver-volume-v2`.
- Path order is immutable after the first SST is created because RocksDB persists path IDs in the
  MANIFEST. Never reorder or prepend paths.
- Multiple paths explicitly disable dynamic level bytes. With a `1GiB` level base and RocksDB's
  level-size multiplier, the 1 TiB path is intended to retain L0-L3 while deeper capacity moves to
  HDD. Actual placement must be verified from live SST metadata after population begins.
- Explicit bottommost manual compaction targets the final path rather than RocksDB's path-0 default.
- The column uses an isolated 8 GiB migration cache while it is being populated so it cannot evict
  the live `messages` working set. At cutover, move it to the default HDD cache and retire the old
  column in a separate, validated change.
- New SSTs use two-level partitioned indexes/filters, Bloom in L0-L1, Ribbon from L2 downward at ten
  Bloom-equivalent bits per key, 128 KiB data blocks, and the existing PLAIN/LZ4/ZSTD per-level
  compression policy. Keeping Bloom in the write-heavy upper levels bounds Ribbon construction CPU.

## Migration safety

The generic raw-SST writer is intentionally rejected for multi-path columns. RocksDB's Java external
ingestion API does not expose a destination path ID and can otherwise fill path 0. Until a dedicated
path-aware migration workflow exists, populate `messages-v2` through ordinary Rockserver writes at a
rate bounded by NVMe usage, HDD compaction debt, foreground p95 latency, and free-space guards.

Before the first write:

1. Start a compatible Rockserver build and verify both directories, path IDs, named-cache gauges,
   fixed-level sizing, filter policy, and the empty physical column.
2. Register the logical schema through `createColumn`: fixed keys `[8, 4]`, `hasValue=true`, no merge
   operator. The config-created physical column is intentionally unconfigured until this step.
3. Record a finite migration target and progress checkpoint outside the destination data being built.
4. Keep reads and writes on the original `messages`; do not dual-read or cut over implicitly.

During migration, verify key/value equality on sampled and boundary ranges, count reconciliation,
restart recovery, cache isolation, NVMe occupancy below the 1 TiB target, HDD free space, compaction
debt, throughput, and latency. Stop population before any capacity or 5% performance gate is crossed.

## Backup, cutover, and rollback

RocksDB BackupEngine/Checkpoint does not cover multi-path column-family layouts as one self-contained
artifact. A coordinated backup must include the DB metadata/MANIFEST, WAL directory, NVMe path, and
HDD path from the same quiesced boundary.

Cutover requires a separately reviewed application change and a final delta/cursor fence. Retain the
old `messages` column and its configuration through the rollback window. Deleting either column,
changing path order, expanding the NVMe target, or enabling raw-SST ingestion is not part of this
layout change.
