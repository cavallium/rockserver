package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Local metadata-only birth fixtures; dynamic child Raft activation is deliberately not fabricated. */
class NativeTopologySeedTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyFreezeTest topology=new NativeTopologyFreezeTest();
    final NativeBucketApplyTest outer=topology.outer;
    final NativePreparedParticipantTest helper=topology.helper;
    DistributedStoragePool pool(Path path) { return DistributedStoragePool.open(path,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass())); }
    ReplicatedRangeStore open(DistributedStoragePool pool,RangeDescriptor descriptor) throws Exception {
        var schema=outer.schema(); var store=pool.openRange(new RangeOwnership.Owner(descriptor.groupId(),descriptor.rangeEpoch()),new RangeOwnership.Interval(outer.domain,schema.columnIds(),descriptor.lowerInclusive(),descriptor.upperExclusive()),schema.digest(),outer.names,Set.of()); store.enableLogicalSchema(schema); return store;
    }
    TopologyChange.State freeze(TopologyChange.Definition definition,List<ReplicatedRangeStore> parents) throws Exception {
        var state=TopologyChange.State.begin(definition.header()).beginFencing();
        for(var parent:parents) { if(parent.applied()==null) parent.applyNoop(new ApplyPosition(1,0)); long next=parent.applied().index()+1; var freeze=parent.applyTopologyFreeze(new ApplyPosition(1,next),definition,topology.admission); state=state.frozen(freeze.proof(topology.replica.replicaId())); }
        return state;
    }
    RocksDB db(DistributedStoragePool pool) throws Exception { var field=DistributedStoragePool.class.getDeclaredField("loaded"); field.setAccessible(true); return ((RocksDBLoader.LoadedDb)field.get(pool)).db().get(); }
    Map<String,ColumnFamilyHandle> handles(DistributedStoragePool pool) throws Exception { var field=DistributedStoragePool.class.getDeclaredField("handles"); field.setAccessible(true); return (Map<String,ColumnFamilyHandle>)field.get(pool); }
    RangeOwnership ownership(DistributedStoragePool pool) throws Exception { var field=DistributedStoragePool.class.getDeclaredField("ownership"); field.setAccessible(true); return (RangeOwnership)field.get(pool); }
    DistributedCommand put(UUID group,long sequence,int key,int value) { return new DistributedCommand(group,1,outer.schema().digest(),new OperationId(outer.session,1,sequence),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,helper.key(key),helper.key(value)))); }
    @Test void splitBirthIsOnlyForcedMetadataAndPreservesReadonlyPinsAndOriginalParentOutcomes() throws Exception {
        Path path=root.resolve("split"); var definition=topology.split(outer.schema(),100); NativeTopologySeed seed; TopologyChange.State authority; byte[] original;
        try(var pool=pool(path)) {
            var parent=open(pool,definition.parents().getFirst()); parent.applyNoop(new ApplyPosition(1,0)); var command=put(outer.group,0,1,9); original=parent.apply(new ApplyPosition(1,1),command); authority=freeze(definition,List.of(parent));
            try(var old=parent.capture()) {
                long sequence=db(pool).getLatestSequenceNumber(); var cfBefore=new HashMap<>(handles(pool));
                seed=pool.seedTopology(definition,authority,List.of(parent),topology.replica);
                assertEquals(RangeOwnership.State.RETIRED,parent.ownership().state()); assertTrue(parent.topologyRetired()); assertArrayEquals(helper.key(9),old.get(outer.fixed,helper.key(1))); assertArrayEquals(original,parent.operationResult(command.operationId(),command.receiptDigest())); assertEquals(-1,parent.retiredAcknowledgedThrough(command.operationId())); assertEquals(parent.applied(),parent.retiredOutcomeState().applied());
                assertThrows(IllegalStateException.class,parent::activate); assertThrows(IllegalStateException.class,parent::captureForRecovery);
                try(var updates=db(pool).getUpdatesSince(sequence+1)) {
                    assertTrue(updates.isValid()); try(var batch=updates.getBatch().writeBatch();var audit=new MetadataOnlyBatch(handles(pool).get(DistributedStoragePool.STATE_COLUMN).getID())) { batch.iterate(audit); }
                }
                assertEquals(cfBefore,handles(pool));
                for(var target:definition.targets()) {
                    var birth=seed.birth(target.groupId()); var child=pool.reopenTopologyRange(birth,outer.schema()); assertNull(child.applied()); assertNull(child.configuration()); assertTrue(child.topologyBirthPending()); assertEquals(RangeOwnership.State.FENCED,child.ownership().state()); assertEquals(new ApplyPosition(0,0),seed.proof(target.groupId()).position()); assertThrows(IllegalStateException.class,child::activate);
                    assertSame(handles(pool).get("fixed"),handles(pool).get(outer.names.get(outer.fixed))); assertArrayEquals(helper.key(9),db(pool).get(handles(pool).get("fixed"),helper.key(1)));
                }
                var later=authority; for(var target:definition.targets()) later=later.seeded(seed.proof(target.groupId()));
                assertEquals(seed,pool.seedTopology(definition,later,List.of(parent),topology.replica)); parent.applyNoop(new ApplyPosition(2,parent.applied().index()+1)); assertEquals(2,parent.retiredOutcomeState().applied().term());
            }
        }
        for(boolean parentFirst:List.of(false,true)) try(var pool=pool(path)) {
            ReplicatedRangeStore parent=null; if(parentFirst) parent=open(pool,definition.parents().getFirst());
            for(var target:definition.targets()) { var child=pool.reopenTopologyRange(seed.birth(target.groupId()),outer.schema()); assertNull(child.applied()); assertTrue(child.topologyBirthPending()); }
            if(parent==null) parent=open(pool,definition.parents().getFirst()); assertEquals(RangeOwnership.State.RETIRED,parent.ownership().state()); assertArrayEquals(original,parent.operationResult(new OperationId(outer.session,1,0),put(outer.group,0,1,9).receiptDigest()));
        }
    }
    private static final class MetadataOnlyBatch extends WriteBatch.Handler {
        private final int state; MetadataOnlyBatch(int state) { this.state=state; }
        private void unexpected() { fail("Topology birth issued a non-metadata PUT operation"); }
        @Override public void put(int cf,byte[] key,byte[] value) { assertEquals(state,cf,"Birth cannot write USER/receipt CFs"); }
        @Override public void put(byte[] key,byte[] value) { unexpected(); }
        @Override public void merge(int cf,byte[] key,byte[] value) { unexpected(); }
        @Override public void merge(byte[] key,byte[] value) { unexpected(); }
        @Override public void delete(int cf,byte[] key) { unexpected(); }
        @Override public void delete(byte[] key) { unexpected(); }
        @Override public void singleDelete(int cf,byte[] key) { unexpected(); }
        @Override public void singleDelete(byte[] key) { unexpected(); }
        @Override public void deleteRange(int cf,byte[] lower,byte[] upper) { unexpected(); }
        @Override public void deleteRange(byte[] lower,byte[] upper) { unexpected(); }
        @Override public void logData(byte[] value) { unexpected(); }
        @Override public void putBlobIndex(int cf,byte[] key,byte[] value) { unexpected(); }
        @Override public void markBeginPrepare() { unexpected(); }
        @Override public void markEndPrepare(byte[] xid) { unexpected(); }
        @Override public void markNoop(boolean empty) { unexpected(); }
        @Override public void markRollback(byte[] xid) { unexpected(); }
        @Override public void markCommit(byte[] xid) { unexpected(); }
        @Override public void markCommitWithTimestamp(byte[] xid,byte[] timestamp) { unexpected(); }
    }
    @Test void mergeBirthInheritsBothAdjacentParentsWithoutMovingEitherUserRow() throws Exception {
        var schema=outer.schema(); UUID second=new UUID(0,40); var sources=List.of(topology.range(20,outer.group,null,new byte[]{(byte)128}),topology.range(21,second,new byte[]{(byte)128},null)); var definition=new TopologyChange.Definition(new UUID(0,100),topology.directory(schema,1,sources),topology.directory(schema,2,List.of(topology.range(22,new UUID(0,41),null,null))));
        try(var pool=pool(root.resolve("merge"))) {
            var left=open(pool,sources.get(0)); var right=open(pool,sources.get(1)); left.applyNoop(new ApplyPosition(1,0)); right.applyNoop(new ApplyPosition(1,0)); left.apply(new ApplyPosition(1,1),put(outer.group,0,1,9)); right.apply(new ApplyPosition(1,1),put(second,0,200,8)); var authority=freeze(definition,List.of(left,right));
            try(var oldLeft=left.capture();var oldRight=right.capture()) {
                var seed=pool.seedTopology(definition,authority,List.of(right,left),topology.replica); var child=pool.reopenTopologyRange(seed.birth(definition.targets().getFirst().groupId()),schema);
                assertNull(child.applied()); assertTrue(child.topologyBirthPending()); assertArrayEquals(helper.key(9),oldLeft.get(outer.fixed,helper.key(1))); assertArrayEquals(helper.key(8),oldRight.get(outer.fixed,helper.key(200)));
                assertArrayEquals(helper.key(9),db(pool).get(handles(pool).get("fixed"),helper.key(1))); assertArrayEquals(helper.key(8),db(pool).get(handles(pool).get("fixed"),helper.key(200)));
            }
        }
    }
    @Test void unknownForceRetainsReservationAndExactRetryOrColdRecoveryForcesOriginalBirth() throws Exception {
        for(boolean after:List.of(false,true)) {
            Path path=root.resolve("unknown-"+after); var definition=topology.split(outer.schema(),100); TopologyChange.State authority; NativeTopologySeed seed;
            try(var pool=pool(path)) {
                var parent=open(pool,definition.parents().getFirst()); var state=freeze(definition,List.of(parent)); authority=state;
                pool.topologySeedForceObserver(boundary->{ if(boundary==after) throw new RocksDBException("Controlled topology force uncertainty"); });
                assertThrows(RocksDBException.class,()->pool.seedTopology(definition,state,List.of(parent),topology.replica)); assertEquals(RangeOwnership.State.ACTIVE,parent.ownership().state()); assertFalse(parent.topologyRetired());
                assertThrows(IllegalStateException.class,()->open(pool,definition.targets().getFirst()));
                var wrong=new ReplicaDescriptor(topology.replica.nodeId(),new UUID(0,99),topology.replica.replicaId(),topology.replica.peerId(),topology.replica.address(),topology.replica.role()); assertThrows(IllegalArgumentException.class,()->pool.seedTopology(definition,state,List.of(parent),wrong));
                pool.topologySeedForceObserver(boundary->{}); seed=pool.seedTopology(definition,state,List.of(parent),topology.replica); assertTrue(parent.topologyRetired()); assertEquals(seed,pool.seedTopology(definition,state,List.of(parent),topology.replica));
            }
            try(var pool=pool(path)) { var parent=open(pool,definition.parents().getFirst()); assertTrue(parent.topologyRetired()); assertEquals(seed,pool.seedTopology(definition,authority,List.of(parent),topology.replica)); }
        }
        Path path=root.resolve("cold-unknown"); var definition=topology.split(outer.schema(),100);
        try(var pool=pool(path)) { var parent=open(pool,definition.parents().getFirst()); var authority=freeze(definition,List.of(parent)); pool.topologySeedForceObserver(after->{if(after)throw new RocksDBException("Lost force reply");}); assertThrows(RocksDBException.class,()->pool.seedTopology(definition,authority,List.of(parent),topology.replica)); }
        try(var pool=pool(path)) { var parent=open(pool,definition.parents().getFirst()); assertTrue(parent.topologyRetired()); assertThrows(IllegalStateException.class,parent::activate); }
    }
    @Test void geometryLeasesPreventSeedAndRetiredShutdownWaitsForActualReadonlyOwnership() throws Exception {
        var definition=topology.split(outer.schema(),100); var pool=pool(root.resolve("lease"));
        try {
            var parent=open(pool,definition.parents().getFirst()); var authority=freeze(definition,List.of(parent)); parent.fence();
            try(var lease=ownership(pool).claimInstall(parent.ownership().owner())) { assertThrows(IllegalArgumentException.class,()->pool.seedTopology(definition,authority,List.of(parent),topology.replica)); }
            parent.activate(); var readonly=parent.capture(); pool.seedTopology(definition,authority,List.of(parent),topology.replica);
            try { assertFalse(pool.close(Duration.ZERO)); assertNull(readonly.get(outer.fixed,helper.key(1))); } finally { readonly.close(); }
            assertTrue(pool.close(Duration.ofSeconds(5)));
        } finally { pool.close(); }
    }
}
