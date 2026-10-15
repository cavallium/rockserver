package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRecoveryImageRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void exactCurrentReceiverReceiptRejectsPartialAndCrossedAuthority()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)) {
            var scope=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9300),null,new UUID(0,9301),fixture.donor,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,fixture.group,fixture.definition.sourceDirectory());
            var request=new RatisRecoveryImageRpc.Request(scope,fixture.store.applied(),new byte[32]);
            byte[] wire=RatisRecoveryImageRpc.request(request);ScopedTransactionRpc.kind(java.nio.ByteBuffer.wrap(wire),RatisRecoveryImageRpc.MAX_REQUEST_BYTES);assertArrayEquals(wire,RatisRecoveryImageRpc.request(RatisRecoveryImageRpc.decode(wire)));
            var absent=new RatisRecoveryImageRpc.Evidence(request.digest(),TopologyChange.Owner.of(fixture.donor),null,null,null,false);
            assertFalse(RatisRecoveryImageRpc.reply(RatisRecoveryImageRpc.reply(absent),request).imageInstalled());
            assertThrows(IllegalArgumentException.class,()->new RatisRecoveryImageRpc.Evidence(request.digest(),TopologyChange.Owner.of(fixture.donor),null,null,new byte[32],true));
            var installed=new RatisRecoveryImageRpc.Evidence(request.digest(),TopologyChange.Owner.of(fixture.donor),fixture.store.applied(),fixture.store.configuration(),new byte[32],true);
            var reply=RatisRecoveryImageRpc.reply(installed);assertTrue(RatisRecoveryImageRpc.reply(reply,request).imageInstalled());
            var changed=new RatisRecoveryImageRpc.Request(scope,new ApplyPosition(1,fixture.store.applied().index()+1),new byte[32]);assertThrows(IllegalArgumentException.class,()->RatisRecoveryImageRpc.reply(reply,changed));
            byte[] changedHash=new byte[32];changedHash[0]=1;assertThrows(IllegalArgumentException.class,()->RatisRecoveryImageRpc.reply(reply,new RatisRecoveryImageRpc.Request(scope,request.imageCut(),changedHash)));
            assertThrows(IllegalArgumentException.class,()->RatisRecoveryImageRpc.decode(Arrays.copyOf(wire,wire.length-1)));
            assertThrows(IllegalArgumentException.class,()->RatisRecoveryImageRpc.reply(Arrays.copyOf(reply,reply.length-1),request));
            assertThrows(IllegalArgumentException.class,()->new RatisRecoveryImageRpc.Request(fixture.request(50),request.imageCut(),request.imageHash()));
        }
    }
}
