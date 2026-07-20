package it.cavallium.rockserver.core.cluster;

import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;

public final class ReplicatedCommandClassifier {

    private ReplicatedCommandClassifier() {
    }

    public static ReplicatedCommandKind classify(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);

        if (usesLocalHandle(requiredCommand)) {
            return ReplicatedCommandKind.LOCAL_SESSION;
        }

        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Put<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch) {
            return ReplicatedCommandKind.REPLICATED_WRITE;
        }

        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn) {
            return ReplicatedCommandKind.REPLICATED_METADATA;
        }

        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Get<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator
                || requiredCommand instanceof RocksDBAPICommand.GetAllColumnDefinitions
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?>
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?>) {
            return ReplicatedCommandKind.ROUTED_READ;
        }

        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?>) {
            return ReplicatedCommandKind.LOCAL_SESSION;
        }

        if (requiredCommand instanceof RocksDBAPICommand.Flush
                || requiredCommand instanceof RocksDBAPICommand.Compact
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw) {
            return ReplicatedCommandKind.LOCAL_ADMIN;
        }

        if (requiredCommand instanceof RocksDBAPICommand.CdcCreate
                || requiredCommand instanceof RocksDBAPICommand.CdcDelete
                || requiredCommand instanceof RocksDBAPICommand.CdcCommit
                || requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll) {
            return ReplicatedCommandKind.CDC;
        }

        return ReplicatedCommandKind.UNSUPPORTED;
    }

    public static boolean usesLocalHandle(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);
        return switch (requiredCommand) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put -> put.transactionOrUpdateId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete -> delete.transactionOrUpdateId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge -> merge.transactionOrUpdateId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti ->
                    putMulti.transactionOrUpdateId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti ->
                    deleteMulti.transactionOrUpdateId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> mergeMulti ->
                    mergeMulti.transactionOrUpdateId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get -> get.transactionOrUpdateId() != 0
                    || (get.requestType() != null
                    && get.requestType().getRequestTypeId() == RequestTypeId.FOR_UPDATE);
            case RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduceRange ->
                    reduceRange.transactionId() != 0;
            case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange -> getRange.transactionId() != 0;
            default -> false;
        };
    }

    public static boolean usesUnsafeLocalWritePath(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);
        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch) {
            return putBatch.mode() != PutBatchMode.WRITE_BATCH;
        }
        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch) {
            return mergeBatch.mode() != MergeBatchMode.MERGE_WRITE_BATCH;
        }
        return false;
    }

    public static boolean requiresResponseDedupe(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);
        return switch (requiredCommand) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put -> producesResponse(put.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti ->
                    producesResponse(putMulti.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete -> producesResponse(delete.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti ->
                    producesResponse(deleteMulti.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge -> producesResponse(merge.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> mergeMulti ->
                    producesResponse(mergeMulti.requestType());
            default -> false;
        };
    }

    public static ReplicatedWriteRetryPolicy retryPolicy(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);
        if (!classify(requiredCommand).entersReplicatedLog()
                || usesUnsafeLocalWritePath(requiredCommand)
                || nullWriteRequestTypeDescription(requiredCommand) != null) {
            return ReplicatedWriteRetryPolicy.UNSUPPORTED;
        }
        if (requiresResponseDedupe(requiredCommand)) {
            return ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE;
        }
        return ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE;
    }

    public static boolean requiresStateMachineRead(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);
        return switch (requiredCommand) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch ignored -> true;
            case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put ->
                    RequestType.requiresGettingPreviousValue(put.requestType())
                            || RequestType.requiresGettingPreviousPresence(put.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti ->
                    RequestType.requiresGettingPreviousValue(putMulti.requestType())
                            || RequestType.requiresGettingPreviousPresence(putMulti.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete ->
                    RequestType.requiresGettingPreviousValue(delete.requestType())
                            || RequestType.requiresGettingPreviousPresence(delete.requestType());
            case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti ->
                    RequestType.requiresGettingPreviousValue(deleteMulti.requestType())
                            || RequestType.requiresGettingPreviousPresence(deleteMulti.requestType());
            default -> false;
        };
    }

    @SuppressWarnings("ConstantValue")
    public static void requireSafeForReplicatedLog(RocksDBAPICommand<?, ?, ?> command) {
        var requiredCommand = requireCommand(command);
        var kind = classify(requiredCommand);
        if (!kind.entersReplicatedLog()) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "command is not a replicated-log command: " + requiredCommand.getClass().getName());
        }
        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch
                && putBatch.mode() == null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    "putBatch mode must not be null");
        }
        if (requiredCommand instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch
                && mergeBatch.mode() == null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    "mergeBatch mode must not be null");
        }
        var nullWriteRequestType = nullWriteRequestTypeDescription(requiredCommand);
        if (nullWriteRequestType != null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    nullWriteRequestType + " must not be null");
        }
        if (usesUnsafeLocalWritePath(requiredCommand)) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "unsafe local batch/SST mode cannot bypass consensus in replicated mode: " + requiredCommand);
        }
    }

    private static boolean producesResponse(RequestType<?, ?> requestType) {
        return requestType != null && requestType.getRequestTypeId() != RequestTypeId.NOTHING;
    }

    private static String nullWriteRequestTypeDescription(RocksDBAPICommand<?, ?, ?> command) {
        return switch (command) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put when put.requestType() == null ->
                    "put requestType";
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti when putMulti.requestType() == null ->
                    "putMulti requestType";
            case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete when delete.requestType() == null ->
                    "delete requestType";
            case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti
                    when deleteMulti.requestType() == null -> "deleteMulti requestType";
            case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge when merge.requestType() == null ->
                    "merge requestType";
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> mergeMulti
                    when mergeMulti.requestType() == null -> "mergeMulti requestType";
            default -> null;
        };
    }

    private static RocksDBAPICommand<?, ?, ?> requireCommand(RocksDBAPICommand<?, ?, ?> command) {
        if (command == null) {
            throw new IllegalArgumentException("command must not be null");
        }
        return command;
    }
}
