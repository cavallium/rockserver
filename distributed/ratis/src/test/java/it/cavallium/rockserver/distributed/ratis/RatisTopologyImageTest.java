package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native images with controlled Ratis initialization/reinitialization; no transport/quorum claim. */
class RatisTopologyImageTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void receivedActivatedChildCheckpointRestoresBirthAndBusinessWithoutAuthorityRpc()throws Exception {
        try(var donor=new RatisTopologyActivationTest.Fixture(root.resolve("donor"));var receiver=new RatisTopologyActivationTest.Fixture(root.resolve("receiver"))) {
            assertEquals(-1,receiver.machine.takeSnapshot());assertTrue(receiver.child.topologyBirthPending());
            assertTrue(donor.prepare().ready());var original=donor.activate(1);
            var command=new DistributedCommand(donor.target.groupId(),1,donor.parent.schema.digest(),new OperationId(RatisTopologyFreezeTest.id(400),1,0),
                    List.of(new Mutation(Mutation.Kind.PUT,donor.parent.column,new byte[]{1},new byte[]{7})));
            var context=donor.propose(DistributedCommandCodec.encode(command));assertNull(context.getException());donor.machine.preAppendTransaction(context);context.initLogEntry(1,2);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(donor.machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertEquals(2,donor.machine.takeSnapshot());
            var source=donor.machine.getStateMachineStorage().getSnapshotFile(1,2).toPath();
            var manifest=NativeRangeImage.inspect(source,new NativeRangeImage.Limits(4<<20,16<<20,1));
            assertNotNull(manifest.topologyOrigin());assertArrayEquals(original.encode(),manifest.topologyOrigin().activation().encode());
            var destination=receiver.machine.getStateMachineStorage().getSnapshotFile(1,2).toPath();Files.copy(source,destination);
            receiver.unavailable=true;receiver.machine.pause();receiver.machine.reinitialize();
            assertEquals(0,receiver.reads.get());assertFalse(receiver.child.topologyBirthPending());assertEquals(new ApplyPosition(1,2),receiver.child.applied());
            assertArrayEquals(new byte[]{7},receiver.child.readCurrentPoint(receiver.parent.column,new byte[]{1}));
            receiver.child.topologyActivation().requireSeed(receiver.seed);
            assertArrayEquals(receiver.seed.birth(receiver.target.groupId()).proof(receiver.seed).digest(),receiver.child.topologyActivation().proof().predecessorDigest());
            assertArrayEquals(original.encode(),receiver.activate(3).encode());assertEquals(0,receiver.reads.get());
            assertEquals(3,receiver.machine.takeSnapshot());
        }
    }
}
