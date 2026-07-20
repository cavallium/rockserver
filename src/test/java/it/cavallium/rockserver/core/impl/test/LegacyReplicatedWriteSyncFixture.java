package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.cluster.ReplicatedCommandClassifier;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCommandEncoder;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * Test-only fixture for the initial raw-mutation replication boundary.
 *
 * <p>Production writes must use the sealed partition-command submission path.
 */
final class LegacyReplicatedWriteSyncFixture implements RocksDBSyncAPI {

    private final ReplicatedLogClient logClient;

    LegacyReplicatedWriteSyncFixture(ReplicatedLogClient logClient) {
        this.logClient = requireArgument(logClient, "logClient");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
        ReplicatedCommandClassifier.requireSafeForReplicatedLog(req);
        var mutations = ReplicatedMutationCommandEncoder.encodeAll(req);
        if (!mutations.isEmpty()) {
            var payload = mutations.size() == 1
                    ? ReplicatedMutationCodec.encode(mutations.getFirst())
                    : ReplicatedMutationCodec.encodeBatch(mutations);
            var commit = replicate(payload);
            if (!commit.committed()) {
                throw RocksDBException.of(
                        RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN,
                        "replicated write did not reach quorum"
                );
            }
        }
        return (RS) successfulResult(req);
    }

    private static Object successfulResult(RocksDBAPICommand<?, ?, ?> req) {
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?>) {
            return List.of();
        }
        return null;
    }

    private ReplicatedLogCommit replicate(byte[] payload) {
        try {
            return logClient.propose(payload).join();
        } catch (CompletionException e) {
            var cause = e.getCause();
            if (cause instanceof RocksDBException rocksDBException) {
                throw rocksDBException;
            }
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.COMMIT_FAILED,
                    "failed to replicate write",
                    cause == null ? e : cause
            );
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
