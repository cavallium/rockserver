package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native state-machine wiring, without a running division or server. */
class RatisReferencedFileStateMachineTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void fileOwnerIsBoundToTheExactGroupBeforeInitialization() throws Exception {
        try(var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("native-fixture"),RatisRangeStateMachine.RetentionLimits.defaults());
            var files=new RatisReferencedFiles(root.resolve("files"),f.groupId,RatisReferencedFiles.Limits.defaults(),Runnable::run);
            var foreign=new RatisReferencedFiles(root.resolve("foreign"),java.util.UUID.randomUUID(),RatisReferencedFiles.Limits.defaults(),Runnable::run)) {
            var machine=new RatisRangeStateMachine(f.store,f.group,new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1));
            assertThrows(IllegalArgumentException.class,() -> machine.referencedFiles(foreign));
            machine.referencedFiles(files); assertSame(files,machine.data());
            assertThrows(IllegalStateException.class,() -> machine.referencedFiles(files));
            assertThrows(IllegalStateException.class,() -> f.machine.referencedFiles(files));
        }
    }
    @Test void absentFileOwnerRejectsInlineStrippedAndStreamedDataWithoutChangingOrdinaryWrites() throws Exception {
        try(var f=new RatisRetentionAdmissionTest.Fixture(root,RatisRangeStateMachine.RetentionLimits.defaults())) {
            var ordinary=LogEntryProto.newBuilder().setTerm(1).setIndex(1)
                    .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFromUtf8("ordinary"))).build();
            assertNull(f.machine.data().write(ordinary).get());
            var inline=ordinary.toBuilder().setStateMachineLogEntry(ordinary.getStateMachineLogEntry().toBuilder()
                    .setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFromUtf8("unsupported-file")))).build();
            assertThrows(ExecutionException.class,() -> f.machine.data().write(inline).get());
            var stripped=ordinary.toBuilder().setStateMachineLogEntry(ordinary.getStateMachineLogEntry().toBuilder()
                    .setStateMachineEntry(StateMachineEntryProto.newBuilder().setLogEntryProtoSerializedSize(1024))).build();
            assertThrows(ExecutionException.class,() -> f.machine.data().write(stripped).get());
            assertThrows(ExecutionException.class,() -> f.machine.data().read(stripped).get());
            var streamed=ordinary.toBuilder().setStateMachineLogEntry(ordinary.getStateMachineLogEntry().toBuilder()
                    .setType(StateMachineLogEntryProto.Type.DATASTREAM)).build();
            assertThrows(UnsupportedOperationException.class,() -> f.machine.data().link(null,streamed));
            assertEquals(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,f.apply(f.propose(f.put(0)),1));
        }
    }
}
