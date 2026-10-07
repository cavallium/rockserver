package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collection;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Initial synchronous apply adapter; the pool owner drains Ratis before closing native handles. */
public final class RatisRangeStateMachine extends BaseStateMachine {
    private final ReplicatedRangeStore store;
    private volatile Throwable storageFailure;
    private final HashMap<OperationId, Admission> admissions = new HashMap<>();
    private long admittedBytes;

    private static final class Admission {
        final OperationId id;
        final byte[] digest;
        final int bytes;
        int owners = 1;
        Admission(OperationId id, byte[] digest, int bytes) {
            this.id = id;
            this.digest = digest;
            this.bytes = bytes;
        }
    }

    public RatisRangeStateMachine(ReplicatedRangeStore store) { this.store = store; }

    @Override public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        var transaction = super.startTransaction(request);
        try {
            requireHealthy();
            byte[] encoded = request.getMessage().getContent().toByteArray();
            var command = DistributedCommandCodec.decode(encoded);
            store.validate(command);
            byte[] digest = command.payloadDigest();
            // Native receipt reads stay outside the short admission lock. If apply
            // advanced while reading, retry validation before acquiring ownership.
            for (int attempt = 0; attempt < 3; attempt++) {
                var observed = store.applied();
                store.operationResult(command.operationId(), digest);
                synchronized (admissions) {
                    if (!java.util.Objects.equals(observed, store.applied())) continue;
                    var admission = admissions.get(command.operationId());
                    if (admission != null) {
                        if (!Arrays.equals(admission.digest, digest)) {
                            throw new IllegalArgumentException("Operation identity reused with different payload");
                        }
                        admission.owners++;
                    } else {
                        if (admissions.size() >= 1024 || admittedBytes + encoded.length > 16L * 1024 * 1024) {
                            throw new IllegalStateException("Range proposal budget exhausted");
                        }
                        admission = new Admission(command.operationId(), digest, encoded.length);
                        admissions.put(admission.id, admission);
                        admittedBytes += admission.bytes;
                    }
                    return transaction.setStateMachineContext(admission);
                }
            }
            throw new IllegalStateException("Range changed during admission; retry with same identity");
        } catch (Exception rejected) {
            return transaction.setException(rejected);
        }
    }

    @Override public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
        try {
            requireHealthy();
            var entry = transaction.getLogEntry();
            var command = DistributedCommandCodec.decode(entry.getStateMachineLogEntry().getLogData().toByteArray());
            byte[] result = store.apply(new ApplyPosition(entry.getTerm(), entry.getIndex()), command);
            // This future is complete only after the WAL-enabled atomic batch returned.
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(result)));
        } catch (Throwable failure) {
            storageFailure = failure;
            return CompletableFuture.failedFuture(failure);
        } finally {
            releaseAdmission(transaction);
        }
    }

    @Override public TransactionContext cancelTransaction(TransactionContext transaction) throws IOException {
        releaseAdmission(transaction);
        return super.cancelTransaction(transaction);
    }

    @Override public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
        for (var transaction : pendingEntries) releaseAdmission(transaction);
    }

    private void releaseAdmission(TransactionContext transaction) {
        synchronized (admissions) {
            if (transaction.getStateMachineContext() instanceof Admission admission) {
                transaction.setStateMachineContext(null);
                if (--admission.owners == 0 && admissions.remove(admission.id, admission)) admittedBytes -= admission.bytes;
            }
        }
    }

    @Override public void notifyTermIndexUpdated(long term, long index) {
        try {
            requireHealthy();
            // Configuration and metadata entries share the updater's ordered lane.
            // Do not use the base callback's immediate marker advance with async apply.
            store.applyNoop(new ApplyPosition(term, index));
            updateLastAppliedTermIndex(term, index);
        } catch (Throwable failure) {
            storageFailure = failure;
            throw new IllegalStateException("Range failed completed apply", failure);
        }
    }

    @Override public CompletableFuture<Message> query(Message request) {
        try {
            requireHealthy();
            byte[] encoded = request.getContent().toByteArray();
            if (encoded.length < 16 || encoded.length > DistributedCommandCodec.MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("Invalid point read size");
            }
            var input = ByteBuffer.wrap(encoded);
            var column = new UUID(input.getLong(), input.getLong());
            var key = new byte[input.remaining()];
            input.get(key);
            try (var pin = store.capture()) {
                byte[] value = pin.get(column, key);
                var result = ByteBuffer.allocate(4 + (value == null ? 0 : value.length));
                result.putInt(value == null ? -1 : value.length);
                if (value != null) result.put(value);
                return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(result.array())));
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void requireHealthy() {
        if (storageFailure != null) throw new IllegalStateException("Range requires repair", storageFailure);
    }
}
