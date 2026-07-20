package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.command.PartitionStateWrite.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public record PartitionStateWriteBatch(List<PartitionStateWrite> userWrites,
                                       Buf dedupeKey,
                                       byte[] dedupeValue,
                                       Buf clientDedupeStateKey,
                                       byte[] clientDedupeStateValue,
                                       Buf lastAppliedKey,
                                       byte[] lastAppliedValue,
                                       long lastAppliedIndex,
                                       List<ReplicatedCdcEvent> cdcEvents) {

    public PartitionStateWriteBatch(List<PartitionStateWrite> userWrites,
                                    Buf dedupeKey,
                                    byte[] dedupeValue,
                                    Buf clientDedupeStateKey,
                                    byte[] clientDedupeStateValue,
                                    Buf lastAppliedKey,
                                    byte[] lastAppliedValue,
                                    long lastAppliedIndex) {
        this(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey, clientDedupeStateValue, lastAppliedKey,
                lastAppliedValue, lastAppliedIndex, List.of());
    }

    public PartitionStateWriteBatch {
        userWrites = copyUserWrites(userWrites);
        dedupeKey = copyRequiredBuf(dedupeKey, "dedupeKey");
        dedupeValue = copyRequiredBytes(dedupeValue, "dedupeValue");
        clientDedupeStateKey = copyRequiredBuf(clientDedupeStateKey, "clientDedupeStateKey");
        clientDedupeStateValue = copyRequiredBytes(clientDedupeStateValue, "clientDedupeStateValue");
        lastAppliedKey = copyRequiredBuf(lastAppliedKey, "lastAppliedKey");
        lastAppliedValue = copyRequiredBytes(lastAppliedValue, "lastAppliedValue");
        if (lastAppliedIndex < 0) {
            throw new IllegalArgumentException("lastAppliedIndex must be >= 0");
        }
        if (PartitionStateMetadataCodec.decodeLastAppliedIndex(lastAppliedValue) != lastAppliedIndex) {
            throw new IllegalArgumentException("lastAppliedValue does not match lastAppliedIndex");
        }
        ClientDedupeStateCodec.decode(clientDedupeStateValue);
        cdcEvents = copyCdcEvents(cdcEvents);
    }

    public static PartitionStateWriteBatch fromAppliedCommand(ClusterMetadata metadata,
                                                              PartitionWriteCommand command,
                                                              List<?> responses,
                                                              long lastAppliedIndex) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredCommand = requireArgument(command, "command");
        var requiredResponses = requireArgument(responses, "responses");
        if (requiredCommand.responseMode().producesResponse()) {
            if (requiredResponses.size() != requiredCommand.mutations().size()) {
                throw new IllegalArgumentException("response count must match mutation count");
            }
        } else if (!requiredResponses.isEmpty()) {
            throw new IllegalArgumentException("no-response commands must not persist responses");
        }

        var userWrites = new ArrayList<PartitionStateWrite>(requiredCommand.mutations().size());
        for (var mutation : requiredCommand.mutations()) {
            var calculatedKey = PartitionWriteCommandReplayContext.calculatedKey(requiredMetadata, requiredCommand, mutation);
            var storageKey = RockserverStorageKeyspace.userData(mutation.columnId(), calculatedKey);
            userWrites.add(switch (mutation.operation()) {
                case PUT -> PartitionStateWrite.put(storageKey, mutation.requiredValue());
                case DELETE -> PartitionStateWrite.delete(storageKey);
                case MERGE -> PartitionStateWrite.merge(storageKey, mutation.requiredValue());
            });
        }

        return new PartitionStateWriteBatch(
                userWrites,
                RockserverStorageKeyspace.dedupeResponse(requiredCommand.requestId()),
                PartitionWriteResponseCodec.encodeResponses(requiredResponses),
                RockserverStorageKeyspace.dedupeClientState(requiredCommand.requestId().clientId()),
                ClientDedupeStateCodec.encode(ClientDedupeState.acknowledged(requiredCommand.requestId())),
                RockserverStorageKeyspace.lastAppliedIndex(),
                PartitionStateMetadataCodec.encodeLastAppliedIndex(lastAppliedIndex),
                lastAppliedIndex,
                List.of()
        );
    }

    public byte[] dedupeValue() {
        return Arrays.copyOf(dedupeValue, dedupeValue.length);
    }

    public byte[] clientDedupeStateValue() {
        return Arrays.copyOf(clientDedupeStateValue, clientDedupeStateValue.length);
    }

    public byte[] lastAppliedValue() {
        return Arrays.copyOf(lastAppliedValue, lastAppliedValue.length);
    }

    public int userMutationCount() {
        return userWrites.size();
    }

    public boolean containsUserWriteOperation(Operation operation) {
        for (var userWrite : userWrites) {
            if (userWrite.operation() == operation) {
                return true;
            }
        }
        return false;
    }

    private static List<PartitionStateWrite> copyUserWrites(List<PartitionStateWrite> userWrites) {
        var requiredUserWrites = requireArgument(userWrites, "userWrites");
        var copiedWrites = new ArrayList<PartitionStateWrite>(requiredUserWrites.size());
        for (int i = 0; i < requiredUserWrites.size(); i++) {
            copiedWrites.add(requireArgument(requiredUserWrites.get(i), "userWrites[" + i + "]"));
        }
        return List.copyOf(copiedWrites);
    }

    private static List<ReplicatedCdcEvent> copyCdcEvents(List<ReplicatedCdcEvent> cdcEvents) {
        var requiredCdcEvents = requireArgument(cdcEvents, "cdcEvents");
        var copiedEvents = new ArrayList<ReplicatedCdcEvent>(requiredCdcEvents.size());
        for (int i = 0; i < requiredCdcEvents.size(); i++) {
            copiedEvents.add(requireArgument(requiredCdcEvents.get(i), "cdcEvents[" + i + "]"));
        }
        return List.copyOf(copiedEvents);
    }

    private static Buf copyRequiredBuf(Buf value, String name) {
        var requiredValue = requireArgument(value, name);
        return Buf.wrap(requiredValue.toByteArray()).freeze();
    }

    private static byte[] copyRequiredBytes(byte[] value, String name) {
        var requiredValue = requireArgument(value, name);
        return Arrays.copyOf(requiredValue, requiredValue.length);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
