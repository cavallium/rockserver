package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Original whole-batch results across native split activation; controlled transport, no sockets. */
class RatisRoutedTopologyOutcomeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisTopologyControllerTest helper=new RatisTopologyControllerTest();
        final RatisTopologyControllerTest.Fixture topology;
        final RatisRoutedClient routed;
        int writes,queries;
        boolean lose,unavailable,crossed;
        Fixture() throws Exception {
            helper.root=root;topology=helper.new Fixture(root.resolve("native"),false,false);
            routed=new RatisRoutedClient(topology.definition.sourceDirectory(),new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor range,DistributedCommand command) throws IOException {
                    writes++;
                    try {
                        var store=topology.store(range.groupId());
                        var result=CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command));
                        topology.server.machines.get(RaftGroupId.valueOf(range.groupId())).notifyTermIndexUpdated(1,store.applied().index());
                        if(lose) { lose=false;throw new IOException("Lost applied original result"); }
                        return result;
                    } catch(IOException failure) { throw failure; }
                    catch(Exception failure) { throw new IOException("Original proposal failed",failure); }
                }
                public byte[] transaction(RangeDescriptor range,byte[] wire,boolean fresh) throws IOException {
                    queries++;assertTrue(fresh);
                    if(unavailable)throw new IOException("Original parent unavailable");
                    try {
                        var request=RatisTopologyOperationOutcomeRpc.decodeRequest(wire);
                        if(crossed) {
                            var changed=new RatisTopologyOperationOutcomeRpc.Request(request.clusterId(),request.databaseId(),request.domainId(),request.lineage(),
                                    new OperationId(request.operation().sessionId(),request.operation().epoch(),request.operation().sequence()+1),request.receiptDigest());
                            return RatisTopologyOperationOutcomeRpc.reply(changed,topology.store(range.groupId()).readTopologyOperationOutcome(
                                    request.clusterId(),request.databaseId(),request.domainId(),request.lineage(),request.operation(),request.receiptDigest()));
                        }
                        return topology.server.machines.get(RaftGroupId.valueOf(range.groupId())).queryLinearizable(RatisTopologyControllerTest.message(wire))
                                .get(5,TimeUnit.SECONDS).getContent().toByteArray();
                    } catch(Exception failure) { throw new IOException("Original outcome unavailable",failure); }
                }
            });
        }
        RatisRoutedClient.PreparedCommand prepare(int value) {
            return routed.prepare(new OperationId(new UUID(178,1),0,0),List.of(
                    new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{(byte)200},new byte[]{(byte)value},Mutation.ReturnMode.PREVIOUS),
                    new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{1},new byte[]{(byte)value},Mutation.ReturnMode.PREVIOUS)));
        }
        void activate() throws Exception {
            Path journal=Files.createDirectory(root.resolve("topology"));
            try(var controller=topology.controller(journal)) {
                var handle=controller.prepare(topology.definition);helper.until(controller,handle,journal,RatisTopologyController.Phase.ACTIVE);
            }
        }
        public void close() throws Exception { routed.close();topology.close(); }
    }
    @Test void lostWholeBatchRetainsResultOrderAndHashAfterSplitAndExpiryCannotBecomeAbsence() throws Exception {
        try(var f=new Fixture()) {
            var original=f.prepare(7);var changed=f.prepare(8);f.lose=true;
            assertThrows(IOException.class,()->f.routed.execute(original));assertEquals(1,f.writes);
            var parent=f.topology.store(original.descriptor().groupId());byte[] saved=parent.operationResult(original.command().operationId(),original.command().receiptDigest());
            assertNotNull(saved);f.activate();f.routed.refresh(f.topology.definition.targetDirectory());
            var found=f.routed.operationOutcome(original);assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.FOUND,found.outcome());assertArrayEquals(saved,found.result());
            var result=f.routed.execute(original);assertArrayEquals(saved,result.encode());assertEquals(2,result.valueCount());assertNull(result.value(0));assertArrayEquals(new byte[]{42},result.value(1));assertEquals(1,f.writes);
            assertThrows(IOException.class,()->f.routed.operationOutcome(changed));
            for(var child:f.topology.definition.targets()) {
                var store=f.topology.store(child.groupId());byte[] key=child.lowerInclusive()==null ? new byte[]{1} : new byte[]{(byte)200};assertArrayEquals(new byte[]{7},store.readCurrentPoint(RatisTopologyControllerTest.COLUMN,key));
            }
            var directory=f.topology.definition.sourceDirectory();var ledger=SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),0);
            var clock=DistributedCommand.sessionControl(original.descriptor().groupId(),original.descriptor().rangeEpoch(),parent.schemaDigest(),new OperationId(new UUID(178,2),1,0),RangeSessionControl.advance(ledger),-1);
            parent.apply(new ApplyPosition(1,parent.applied().index()+1),clock);f.topology.server.machines.get(RaftGroupId.valueOf(original.descriptor().groupId())).notifyTermIndexUpdated(1,parent.applied().index());
            assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.EXPIRED,f.routed.operationOutcome(original).outcome());assertEquals(CommandResult.Status.SESSION_EXPIRED,f.routed.execute(original).status());assertEquals(1,f.writes);assertEquals(0,parent.activeSnapshotPins());
        }
    }
    @Test void lostAcknowledgementAfterSplitDrainsOnlyOriginalParentAndReusesItsReceipt()throws Exception {
        try(var f=new Fixture()) {
            var original=f.prepare(7);f.routed.execute(original);
            var ack=f.routed.prepareAcknowledgement(new OperationId(original.command().operationId().sessionId(),0,1),RatisTopologyControllerTest.COLUMN,new byte[]{1},0);
            f.activate();f.routed.refresh(f.topology.definition.targetDirectory());f.lose=true;
            assertThrows(IOException.class,()->f.routed.execute(ack));assertEquals(2,f.writes);
            var parent=f.topology.store(original.descriptor().groupId());assertEquals(RangeOwnership.State.RETIRED,parent.ownership().state());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.routed.execute(original).status());
            assertEquals(CommandResult.Status.SUCCESS,f.routed.execute(ack).status());assertEquals(2,f.writes);
            for(var child:f.topology.definition.targets()) {
                var store=f.topology.store(child.groupId());assertNull(store.operationResult(ack.command().operationId(),ack.command().receiptDigest()));
                assertArrayEquals(new byte[]{7},store.readCurrentPoint(RatisTopologyControllerTest.COLUMN,child.lowerInclusive()==null?new byte[]{1}:new byte[]{(byte)200}));
            }
            assertEquals(0,parent.activeSnapshotPins());
        }
    }
    @Test void authoritativeAbsenceKeepsWholeOriginalRequestAndUnavailableOrCrossedRepliesNeverAuthorizeWrites() throws Exception {
        try(var f=new Fixture()) {
            var original=f.prepare(7);f.activate();f.routed.refresh(f.topology.definition.targetDirectory());
            var absent=assertThrows(RatisRoutedClient.OriginalOperationAbsent.class,()->f.routed.execute(original));
            assertSame(original,absent.original());assertArrayEquals(original.command().receiptDigest(),absent.evidence().request().receiptDigest());assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.ABSENT,absent.evidence().outcome());assertEquals(0,f.writes);
            f.unavailable=true;assertThrows(IOException.class,()->f.routed.execute(original));f.unavailable=false;
            f.crossed=true;var malformed=assertThrows(IOException.class,()->f.routed.execute(original));assertFalse(malformed instanceof RatisRoutedClient.OriginalOperationAbsent);assertEquals(0,f.writes);
            var left=f.topology.definition.targets().getFirst();var right=f.topology.definition.targets().getLast();
            assertArrayEquals(new byte[]{42},f.topology.store(left.groupId()).readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));assertNull(f.topology.store(right.groupId()).readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{(byte)200}));
        }
    }
    @Test void cachedParentRouteResolvesSavedResultAfterRejectedWriteWithoutRepartitioning() throws Exception {
        try(var f=new Fixture()) {
            var original=f.prepare(7);var saved=f.routed.execute(original);f.activate();
            assertArrayEquals(saved.encode(),f.routed.execute(original).encode());assertEquals(2,f.writes);assertEquals(1,f.queries);
        }
    }
}
