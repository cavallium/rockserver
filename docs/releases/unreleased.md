# Rockserver: next release (unreleased)

## CDC removed

Change data capture is removed, not deprecated: it was not used in production.

**Removed API.** The removal is incompatible for code that references CDC. Release it as a new major version.

- **gRPC:** the `cdc*` RPCs and their messages. `WorkloadProfile` value 5 (`CDC`) is reserved.
- **Thrift:** the CDC service methods and structs. Profile value 5 and error codes 39–42 are reserved.
- **Java:** the CDC methods of `RocksDBSyncAPI`/`RocksDBAsyncAPI`, the `it.cavallium.rockserver.core.common.cdc`
  package, `WorkloadProfile.CDC`, and the `RocksDBErrorType.CDC_*` constants.
- **Rust:** the client's `cdc_*` methods and its `Cdc*` types.

Code that only passes CDC calls through must drop those overrides to compile. For example, Yotsuba's
`RockserverAsyncApiViews.cdcPollBatchAsync`.

**Wire compatibility.** It is unchanged for everything else. No remaining field, enum value or error code was
renumbered, and error types still travel by name.

**Existing databases.** They open unchanged. A database created by an older version keeps its `_cdc_meta_`
column family: it is opened, but never read or written. `_cdc_meta_` can no longer be created as a user column.

**Config files.** The removed workload keys are ignored, so old config files still load:
`cdc-queue-capacity`, `read-cdc-reservation`, `write-cdc-reservation`, `cdc-drr-weight`,
`cdc-quantum-max-mutations`, `cdc-quantum-max-bytes` and `cdc-quantum-max-duration`.

**Metrics removed.**

- `rockserver.workload.cdc.lag`;
- `rockserver.cdc.events` and `rockserver.cdc.bytes`;
- every scheduler series tagged `profile=CDC`.

Update dashboards and alerts that use them.

## WAL archive off by default

The WAL archive only kept CDC history.

- **New defaults:** `wal-ttl-seconds` and `wal-size-limit-mb` now default to 0, which means no archive. Before,
  they were 86400 s and 102400 MiB.
- **0 is now valid** for `wal-ttl-seconds`.
- **Leftover archives are deleted.** When both settings are 0, startup deletes `<wal-path>/archive`, the archive an
  earlier version left behind. That frees up to about 100 GiB on upgraded databases. RocksDB never touches that
  directory once archiving is off.
- **Configs that copied the old values.** A config file that copied the old defaults (86400 / 102400) keeps
  archiving: remove both keys, or set them to 0.
- **A TTL without a cap.** A config that sets only `wal-ttl-seconds` now has no size cap. The cap used to default
  to 100 GiB.
