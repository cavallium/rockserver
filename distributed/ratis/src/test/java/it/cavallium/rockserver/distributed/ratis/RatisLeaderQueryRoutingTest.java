package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual adapter roles classify routing before any body decoding, native capture or proof allocation. */
class RatisLeaderQueryRoutingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static Message frame(int kind){return Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,new byte[0])));}

    @Test void normalLeaderProofsAndCapturedOwnerControlsKeepTheirDistinctRoutes()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)) {
            var range=fixture.firstDirectory.ranges().getFirst();
            var data=fixture.server.machines.get(RaftGroupId.valueOf(range.groupId()));
            var cat=fixture.plan.catalogue();
            var view=new RatisCatalogueViewRpc.Request(fixture.first.clusterId(),fixture.first.databaseId(),cat.groupId(),cat.directoryColumn(),fixture.first.domainId());
            assertTrue(fixture.catalogueMachine.requiresLeaderForLinearizableQuery(Message.valueOf(ByteString.copyFrom(RatisCatalogueViewRpc.request(view)))));
            var request=new RatisTopologySourceSnapshotRpc.Request(UUID.randomUUID(),null,cat.groupId(),fixture.local,
                    RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),fixture.firstDirectory);
            for(int kind:new int[]{RatisTopologySourceSnapshotRpc.PREPARE,RatisTopologySourceSnapshotRpc.TRANSFER})
                assertTrue(data.requiresLeaderForLinearizableQuery(Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.request(kind,request)))));
            assertFalse(data.requiresLeaderForLinearizableQuery(Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.RELEASE,request)))));
            for(int kind:new int[]{ScopedTransactionRpc.STATE,ScopedTransactionRpc.OPEN,ScopedTransactionRpc.GET,
                    ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE,ScopedTransactionRpc.PARTICIPANT_STATE,
                    RatisSourceHeadRpc.KIND,RatisCdcPollRpc.KIND,RatisTopologyOperationOutcomeRpc.KIND,RatisSnapshotCohortRpc.OWNER})
                assertTrue(data.requiresLeaderForLinearizableQuery(frame(kind)),"Leader read kind "+kind);
            for(int kind:new int[]{ScopedTransactionRpc.POINT,ScopedTransactionRpc.CLOSE,ScopedTransactionRpc.SNAPSHOT_CAPTURE,
                    ScopedTransactionRpc.SNAPSHOT_GET,ScopedTransactionRpc.SNAPSHOT_RELEASE,ScopedTransactionRpc.EXPORT_BUILD,
                    RatisSnapshotCohortRpc.CAPTURE,RatisSnapshotCohortRpc.STATE,RatisSnapshotCohortRpc.CANCEL,
                    RatisRecoveryObsoleteRpc.KIND,RatisRecoverySupersessionRpc.KIND,ScopedTransactionRpc.TOPOLOGY_OWNER})
                assertFalse(data.requiresLeaderForLinearizableQuery(frame(kind)),"Local owner kind "+kind);
            for(var home:fixture.plan.homes()) {
                var machine=fixture.remoteHomes.get(home.directory().ranges().getFirst().groupId());
                int kind=home.role()==RatisNodePlan.Role.TRANSACTION_HOME?ScopedTransactionRpc.HOME_RECOVERY_PREPARE:ScopedTransactionRpc.SNAPSHOT_HOME_FENCE;
                assertTrue(machine.requiresLeaderForLinearizableQuery(frame(kind)));
                assertFalse(machine.requiresLeaderForLinearizableQuery(frame(ScopedTransactionRpc.CLOSE)));
            }
            // Mixed relocation evidence carries its routing intent in a fixed tail, independent of the definition body.
            byte[] body=new byte[72];
            assertTrue(data.requiresLeaderForLinearizableQuery(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(RatisRelocationOwnerRpc.KIND,body)))));
            body[4]=1;
            assertFalse(data.requiresLeaderForLinearizableQuery(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(RatisRelocationOwnerRpc.KIND,body)))));
        }
    }

    @Test void segmentedLargeControlFramesNeedOnlyTheirFixedHeaderForRouting()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)) {
            var data=fixture.server.machines.get(RaftGroupId.valueOf(fixture.firstDirectory.ranges().getFirst().groupId()));
            var home=fixture.plan.homes().stream().filter(value->value.role()==RatisNodePlan.Role.TRANSACTION_HOME).findFirst().orElseThrow();
            var transactionHome=fixture.remoteHomes.get(home.directory().ranges().getFirst().groupId());
            int size=ScopedTransactionRpc.MAX_REQUEST_BYTES+1;
            assertTrue(size<RatisSourceOutcomeRpc.MAX_REQUEST_BYTES-ScopedTransactionRpc.HEADER_BYTES);
            assertTrue(size<RatisOperationRerouteAdmissionRpc.MAX_BYTES-ScopedTransactionRpc.HEADER_BYTES);
            ByteString body=ByteString.EMPTY,segment=ByteString.copyFrom(new byte[4096]);
            while(body.size()<size)body=body.concat(segment.substring(0,Math.min(segment.size(),size-body.size())));
            var source=segmented(RatisSourceOutcomeRpc.PREPARE,body);
            assertTrue(source.getClass().getSimpleName().contains("RopeByteString"));
            assertTrue(data.requiresLeaderForLinearizableQuery(Message.valueOf(source)));
            assertTrue(transactionHome.requiresLeaderForLinearizableQuery(Message.valueOf(segmented(RatisOperationRerouteAdmissionRpc.PREPARE,body))));
            assertFalse(data.requiresLeaderForLinearizableQuery(Message.EMPTY));
            var malformed=ByteString.copyFrom(ByteBuffer.allocate(12).putInt(ScopedTransactionRpc.MAGIC).putInt(ScopedTransactionRpc.STATE).putInt(1).array());
            assertFalse(data.requiresLeaderForLinearizableQuery(Message.valueOf(malformed)));
        }
    }
    private static ByteString segmented(int kind,ByteString body) {
        return ByteString.copyFrom(ByteBuffer.allocate(12).putInt(ScopedTransactionRpc.MAGIC).putInt(kind).putInt(body.size()).array()).concat(body);
    }
}
