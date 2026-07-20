package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandValidator;
import it.cavallium.rockserver.core.cluster.metadata.MetadataGroupStartupPolicy;
import it.cavallium.rockserver.core.cluster.metadata.MetadataStateMachine;
import it.cavallium.rockserver.core.replication.ratis.RatisMetadataCommandSubmitter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;

public final class RockserverMetadataClasspathStateMachine extends BaseStateMachine {

    private final ClusterMetadata initialMetadata;
    private final SimpleStateMachineStorage snapshotStorage = new SimpleStateMachineStorage();
    private MetadataStateMachine stateMachine;
    private ClusterMetadata appendedMetadata;

    public RockserverMetadataClasspathStateMachine(ClusterMetadata initialMetadata) {
        this.initialMetadata = Objects.requireNonNull(initialMetadata, "initialMetadata");
        this.stateMachine = new MetadataStateMachine(initialMetadata);
        this.appendedMetadata = initialMetadata;
    }

    @Override
    public synchronized TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        try {
            var command = MetadataCommandValidator.decodeForProposal(request.getMessage().getContent().toByteArray());
            return super.startTransaction(request).setStateMachineContext(command);
        } catch (RuntimeException e) {
            throw new IOException("Invalid Rockserver metadata proposal", e);
        }
    }

    @Override
    public synchronized TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
        Objects.requireNonNull(trx, "trx");
        try {
            var command = requireProposalCommand(trx);
            appendedMetadata = MetadataCommandValidator.projectForProposal(appendedMetadata, command);
            return trx;
        } catch (RuntimeException e) {
            throw new IOException("Invalid Rockserver metadata proposal", e);
        }
    }

    @Override
    public synchronized void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
        resetAppendedMetadata();
    }

    @Override
    public synchronized void notifyLeaderReady() {
        resetAppendedMetadata();
    }

    @Override
    public synchronized void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage)
            throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
        snapshotStorage.init(storage);
        openFromLatestSnapshot();
    }

    @Override
    public synchronized CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        var entry = trx.getLogEntry();
        var payload = entry.getStateMachineLogEntry().getLogData().toByteArray();
        try {
            stateMachine.applyCommitted(entry.getIndex(), payload);
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return CompletableFuture.completedFuture(Message.valueOf("rockserver-metadata-applied:"
                    + entry.getIndex()));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public synchronized CompletableFuture<Message> query(Message request) {
        Objects.requireNonNull(request, "request");
        try {
            var payload = request.getContent().toByteArray();
            if (!Arrays.equals(payload, RatisMetadataCommandSubmitter.snapshotQueryPayload())) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "unsupported metadata state-machine query"
                ));
            }
            return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(
                    RatisMetadataCommandSubmitter.encodeSnapshot(stateMachine.metadata())
            )));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public synchronized long takeSnapshot() throws IOException {
        var termIndex = getLastAppliedTermIndex();
        if (termIndex.getIndex() <= 0) {
            return termIndex.getIndex();
        }

        var snapshotFile = snapshotStorage.getSnapshotFile(termIndex.getTerm(), termIndex.getIndex()).toPath();
        RatisMetadataCommandSubmitter.writeSnapshot(snapshotFile, stateMachine.metadata());

        MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile.toFile());
        snapshotStorage.updateLatestSnapshot(new SingleFileSnapshotInfo(new FileInfo(snapshotFile, md5), termIndex));
        return termIndex.getIndex();
    }

    @Override
    public synchronized void reinitialize() throws IOException {
        openFromLatestSnapshot();
    }

    @Override
    public SimpleStateMachineStorage getStateMachineStorage() {
        return snapshotStorage;
    }

    @Override
    public SingleFileSnapshotInfo getLatestSnapshot() {
        return snapshotStorage.getLatestSnapshot();
    }

    private void openFromLatestSnapshot() throws IOException {
        var snapshot = snapshotStorage.getLatestSnapshot();
        if (snapshot == null) {
            validateNoSnapshotStartup(initialMetadata);
            stateMachine = new MetadataStateMachine(initialMetadata);
            resetAppendedMetadata();
            return;
        }
        var metadata = RatisMetadataCommandSubmitter.readSnapshot(snapshot.getFile().getPath());
        stateMachine = new MetadataStateMachine(metadata, snapshot.getTermIndex().getIndex());
        resetAppendedMetadata();
        setLastAppliedTermIndex(snapshot.getTermIndex());
    }

    private static MetadataCommand requireProposalCommand(TransactionContext trx) {
        if (trx.getStateMachineContext() instanceof MetadataCommand command) {
            return command;
        }
        throw new IllegalArgumentException("metadata proposal transaction is missing decoded command context");
    }

    private void resetAppendedMetadata() {
        appendedMetadata = stateMachine.metadata();
    }

    private static void validateNoSnapshotStartup(ClusterMetadata metadata) throws IOException {
        if (MetadataGroupStartupPolicy.requiresSnapshot(metadata)) {
            throw new IOException(MetadataGroupStartupPolicy.GENERATED_LIVE_METADATA_REQUIRES_SNAPSHOT);
        }
    }

}
