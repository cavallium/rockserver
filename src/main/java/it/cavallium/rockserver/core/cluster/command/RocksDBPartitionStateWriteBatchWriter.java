package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public final class RocksDBPartitionStateWriteBatchWriter {

    private final RocksDB db;
    private final ReplicationFailureInjector failureInjector;

    public RocksDBPartitionStateWriteBatchWriter(RocksDB db) {
        this(db, ReplicationFailureInjector.none());
    }

    public RocksDBPartitionStateWriteBatchWriter(RocksDB db, ReplicationFailureInjector failureInjector) {
        this.db = requireArgument(db, "db");
        this.failureInjector = requireArgument(failureInjector, "failureInjector");
    }

    public void write(PartitionStateWriteBatch batch) {
        var requiredBatch = requireArgument(batch, "batch");
        try (var writeBatch = new WriteBatch(); var writeOptions = new WriteOptions()) {
            for (var userWrite : requiredBatch.userWrites()) {
                var key = userWrite.key().toByteArray();
                switch (userWrite.operation()) {
                    case PUT -> writeBatch.put(key, userWrite.requiredValue().toByteArray());
                    case DELETE -> writeBatch.delete(key);
                    case MERGE -> writeBatch.merge(key, userWrite.requiredValue().toByteArray());
                }
            }
            writeBatch.put(requiredBatch.dedupeKey().toByteArray(), requiredBatch.dedupeValue());
            writeBatch.put(requiredBatch.clientDedupeStateKey().toByteArray(), requiredBatch.clientDedupeStateValue());
            writeBatch.put(requiredBatch.lastAppliedKey().toByteArray(), requiredBatch.lastAppliedValue());
            for (var event : requiredBatch.cdcEvents()) {
                var token = event.token();
                writeBatch.put(
                        RockserverStorageKeyspace.cdcEvent(token.appliedIndex(), token.eventOrdinal()).toByteArray(),
                        ReplicatedCdcEventCodec.encode(event)
                );
            }
            failureInjector.inject(ReplicationFailurePhase.ROCKSDB_WRITE);
            db.write(writeOptions, writeBatch);
        } catch (org.rocksdb.RocksDBException e) {
            throw new IllegalStateException("Failed to write partition state batch", e);
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
