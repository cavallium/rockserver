# Merge operators

Rockserver columns can use RocksDB's native merge operators. A client sends a
`merge` with a delta instead of a client-side read-modify-write, and the server
applies the operator's rules (for example "append string" or "add integer").

## Why not read-modify-write

Updating a value from its current state on the client takes three steps: read the
value, compute the new value locally, and write it back, usually with conditional
updates or CAS loops to stay atomic. This has drawbacks:

- **Race conditions**: concurrent updates need locking or optimistic retry loops.
- **Network overhead**: every update needs at least a read and a write round trip.
- **Client complexity**: every client (Java, Rust, and others) must implement the
  same merge logic exactly.

A merge avoids all three:

- **Atomic**: RocksDB applies the merge internally; no client-side locks.
- **Fast**: a single network call per update.
- **Logic on the server**: the rules live in a `MergeOperator` class deployed on the
  server.

Writes use `merge`, `mergeMulti`, or `mergeBatch` through a context-bound API view.
Reads return the fully merged value.

## Updating schemas and operators

### Key schema (immutable)

The key structure defined by `ColumnSchema` is immutable. You cannot change the
number or length of fixed keys, or the hashing strategy of variable keys, of an
existing column. `createColumn` rejects any change to the physical key layout. To
change the key structure, create a new column and migrate the data.

### Value schema (supported)

Rockserver treats values as opaque bytes (or buckets of opaque bytes). Client-side
serialization (for example Protobuf or JSON) can change as long as it still reads
old data.

### Merge operator logic (supported)

The `MergeOperator` code can change to handle new data formats or business rules.

With a server restart:

1. Deploy the new `MergeOperator` class (for example in a new jar).
2. Point the database configuration at the new class if its name changed.
3. Restart Rockserver. If the key schema matches but the merge operator changed,
   Rockserver updates the persisted schema metadata.

Without downtime (hot swap):

1. Upload the new jar with `uploadMergeOperator` (or `ensureMergeOperator`, which
   reuses an identical jar already uploaded) and receive a new version ID.
2. Call `createColumn` with the existing column name, the *same* key schema, and the
   new merge operator name and version.
3. Rockserver detects the schema change and swaps the operator in place. All later
   merges and reads use the new logic. A `DelegatingMergeOperator` wrapper forwards
   native calls to the active Java implementation.
