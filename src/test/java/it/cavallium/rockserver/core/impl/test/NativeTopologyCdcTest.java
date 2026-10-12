package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real native terminal lineage and metadata-only retired retention; no Raft/server or route acceptance claim. */
class NativeTopologyCdcTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologySeedTest helper=new NativeTopologySeedTest();
    final UUID catalogue=new UUID(0,900),issuer=new UUID(0,901);
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,32<<20,4);
    DistributedCdcCheckpoint.Lineage lineage(RangeDescriptor range) { return new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()); }
    byte[] admission(TopologyChange.Definition definition)throws Exception { byte[] fencing=TopologyChange.State.begin(definition.header()).beginFencing().encode();return java.security.MessageDigest.getInstance("SHA-256").digest(ByteBuffer.allocate(16+fencing.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(fencing).array()); }
    final class Fixture implements AutoCloseable {
        final DistributedStoragePool pool;final TopologyChange.Definition definition=helper.topology.split(helper.outer.schema(),100);final ReplicatedRangeStore parent;
        NativeTopologyFreeze freeze;NativeTopologySeed seed;ReplicatedRangeStore child;
        Fixture(Path path)throws Exception { pool=helper.pool(path);parent=helper.open(pool,definition.parents().getFirst());parent.initializeConfiguration(new byte[]{1});parent.enableCdc(definition.parents().getFirst().rangeId());parent.enableSessionControls();parent.enableScopedHistory(RangeMutationHistory.Limits.defaults());parent.applyNoop(new ApplyPosition(1,0)); }
        DistributedCommand write(ReplicatedRangeStore store,long sequence,int value) { return new DistributedCommand(store.ownership().owner().groupId(),1,store.schemaDigest(),new OperationId(helper.outer.session,1,sequence),List.of(new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(1),helper.helper.key(value)))); }
        void freeze()throws Exception { freeze=parent.applyTopologyFreeze(new ApplyPosition(1,parent.applied().index()+1),definition,admission(definition)); }
        void seed()throws Exception { var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(helper.topology.replica.replicaId()));seed=pool.seedTopology(definition,authority,List.of(parent),helper.topology.replica);var target=definition.targets().getFirst();child=pool.reopenTopologyRange(seed.birth(target.groupId()),helper.outer.schema());child.enableCdc(target.rangeId());child.initializeConfiguration(new byte[]{1});child.applyNoop(new ApplyPosition(3,0));child.applyTopologyActivation(new ApplyPosition(3,1),definition,authority.freezeDigest(),catalogue,new ApplyPosition(1,10),NativeTopologyActivation.publicationCommitment(definition,authority.freezeDigest(),catalogue)); }
        DistributedCommand clock(SessionEpochLedger ledger,long sequence) { return DistributedCommand.sessionControl(parent.ownership().owner().groupId(),1,parent.schemaDigest(),new OperationId(issuer,1,sequence),RangeSessionControl.advance(ledger),sequence-1); }
        ReplicatedRangeStore.CdcPage retired(DistributedCdcCheckpoint.Cursor from)throws Exception { return parent.pollRetiredCdc(lineage(definition.parents().getFirst()),from,256,8<<20); }
        @Override public void close() throws java.io.IOException { pool.close(); }
    }
    @Test void parentTerminalBarrierIsAtomicAndChildStartsAtOwnActivationFrontier()throws Exception {
        try(var f=new Fixture(root)) {
            var command=f.write(f.parent,0,9);byte[] result=f.parent.apply(new ApplyPosition(1,1),command);f.freeze();var page=f.retired(DistributedCdcCheckpoint.Cursor.beginning());assertEquals(2,page.records().size());var barrier=page.records().getLast();assertEquals(DistributedCdcRecord.Kind.TERMINAL_BARRIER,barrier.kind());assertEquals(f.freeze,barrier.barrier());assertNull(barrier.command());assertEquals(0,barrier.mutationCount());assertTrue(barrier.events().isEmpty());assertEquals(DistributedCdcCheckpoint.Cursor.after(f.freeze.position()),barrier.end());assertTrue(f.retired(barrier.end()).fromVerified());
            assertThrows(IllegalArgumentException.class,()->f.retired(new DistributedCdcCheckpoint.Cursor(f.freeze.position(),0)));
            f.seed();assertEquals(RangeOwnership.State.RETIRED,f.parent.ownership().state());assertArrayEquals(result,f.parent.operationResult(command.operationId(),command.receiptDigest()));assertEquals(2,f.retired(DistributedCdcCheckpoint.Cursor.beginning()).records().size());
            var baseline=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(3,1));var childLineage=lineage(f.definition.targets().getFirst());assertTrue(f.child.pollCdc(childLineage,baseline,10,8<<20).fromVerified());assertThrows(ReplicatedRangeStore.CdcGap.class,()->f.child.pollCdc(childLineage,DistributedCdcCheckpoint.Cursor.beginning(),10,8<<20));f.child.apply(new ApplyPosition(3,2),f.write(f.child,0,8));var childPage=f.child.pollCdc(childLineage,baseline,10,8<<20);assertTrue(childPage.fromVerified());assertEquals(1,childPage.records().size());assertEquals(new ApplyPosition(3,2),childPage.records().getFirst().position());assertEquals(DistributedCdcRecord.Kind.MUTATIONS,childPage.records().getFirst().kind());assertEquals(2,f.retired(DistributedCdcCheckpoint.Cursor.beginning()).records().size());
        }
    }
    @Test void duplicateFreezeAndActivationKeepOriginalEventsAndFrontierAfterPrefixAdvanceAndImage()throws Exception {
        Path image=root.resolve("frozen-image");TopologyChange.Definition definition;NativeTopologyFreeze freeze;
        try(var f=new Fixture(root.resolve("source"))) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();freeze=f.freeze;definition=f.definition;assertEquals(freeze,f.parent.applyTopologyFreeze(new ApplyPosition(2,3),definition,admission(definition)));assertEquals(2,f.retired(DistributedCdcCheckpoint.Cursor.beginning()).records().size());NativeRangeImage.write(image,f.parent,limits);f.seed();var original=f.child.topologyActivation();f.child.applyTopologyActivation(new ApplyPosition(4,2),definition,original.commonFreezeDigest(),catalogue,original.catalogueCut(),original.publicationCommitment());var baseline=DistributedCdcCheckpoint.Cursor.after(original.position());var page=f.child.pollCdc(lineage(definition.targets().getFirst()),baseline,10,8<<20);assertTrue(page.fromVerified());assertEquals(baseline,page.floor());assertTrue(page.records().isEmpty());
        }
        try(var pool=helper.pool(root.resolve("image-target"))) { var parent=helper.open(pool,definition.parents().getFirst());parent.initializeConfiguration(new byte[]{1});parent.enableCdc(definition.parents().getFirst().rangeId());parent.fence();parent.installSnapshot(image,limits);parent.activate();var page=parent.pollRetiredCdc(lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.beginning(),10,8<<20);assertEquals(2,page.records().size());assertEquals(freeze,page.records().getLast().barrier());assertTrue(parent.pollRetiredCdc(lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.after(freeze.position()),10,8<<20).fromVerified()); }
    }
    @Test void retiredCommittedClockExpiresTerminalAndAgeCellsAtExactBoundaryWithoutChildEffects()throws Exception {
        Path path=root.resolve("retired");TopologyChange.Definition definition;NativeTopologySeed seed;DistributedCommand original;byte[] result;
        try(var f=new Fixture(path)) {
            original=f.write(f.parent,0,9);result=f.parent.apply(new ApplyPosition(1,1),original);f.freeze();f.seed();definition=f.definition;seed=f.seed;f.child.apply(new ApplyPosition(3,2),f.write(f.child,0,8));
            var ledger=SessionEpochLedger.initial(helper.outer.schema().clusterId(),helper.outer.schema().databaseId(),0);var first=f.clock(ledger,0);assertTrue(f.parent.metadataTopologyControl(first));f.parent.validate(first);assertTrue(f.parent.estimateReceiptRetention(first).newReceipt());f.parent.apply(new ApplyPosition(1,3),first);assertThrows(IllegalArgumentException.class,()->f.retired(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,3))));f.parent.apply(new ApplyPosition(1,4),f.clock(ledger.advanceTime(86_399_999),1));assertEquals(2,f.retired(DistributedCdcCheckpoint.Cursor.beginning()).records().size());f.parent.apply(new ApplyPosition(1,5),f.clock(ledger.advanceTime(86_399_999).advanceTime(86_400_000),2));var gap=assertThrows(ReplicatedRangeStore.CdcGap.class,()->f.retired(DistributedCdcCheckpoint.Cursor.beginning()));assertEquals(DistributedCdcCheckpoint.Cursor.after(f.freeze.position()),gap.page().floor());assertTrue(f.retired(gap.page().floor()).records().isEmpty());assertTrue(f.retired(gap.page().floor()).fromVerified());assertArrayEquals(helper.helper.key(8),f.child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertArrayEquals(result,f.parent.operationResult(original.operationId(),original.receiptDigest()));
            byte[] summary=helper.db(f.pool).get(helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN),ByteBuffer.allocate(17).putLong(original.groupId().getMostSignificantBits()).putLong(original.groupId().getLeastSignificantBits()).put((byte)0x51).array());assertEquals(0,ByteBuffer.wrap(summary,60,4).getInt());assertEquals(0,ByteBuffer.wrap(summary,64,8).getLong());
            for(long index:List.of(1L,2L)) { byte[] key=ByteBuffer.allocate(25).putLong(original.groupId().getMostSignificantBits()).putLong(original.groupId().getLeastSignificantBits()).put((byte)0x52).putLong(index).array();assertNull(helper.db(f.pool).get(helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN),key)); }
        }
        try(var pool=helper.pool(path)) { var parent=helper.open(pool,definition.parents().getFirst());parent.enableCdc(definition.parents().getFirst().rangeId());parent.enableSessionControls();var floor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2));assertTrue(parent.pollRetiredCdc(lineage(definition.parents().getFirst()),floor,10,8<<20).records().isEmpty());var next=DistributedCommand.sessionControl(original.groupId(),1,parent.schemaDigest(),new OperationId(issuer,1,3),RangeSessionControl.advance(SessionEpochLedger.initial(helper.outer.schema().clusterId(),helper.outer.schema().databaseId(),0).advanceTime(86_399_999).advanceTime(86_400_000).advanceTime(86_400_001)),2);parent.apply(new ApplyPosition(2,6),next);assertArrayEquals(result,parent.operationResult(original.operationId(),original.receiptDigest()));var child=pool.reopenTopologyRange(seed.birth(definition.targets().getFirst().groupId()),helper.outer.schema());assertArrayEquals(helper.helper.key(8),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertEquals(new ApplyPosition(3,2),child.applied()); }
    }
    @Test void failedFreezeAndFreshRetiredUserOrBindingCannotAppendOrMutate()throws Exception {
        try(var f=new Fixture(root)) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));var before=f.parent.applied();assertThrows(IllegalStateException.class,()->f.parent.applyTopologyFreeze(new ApplyPosition(1,3),f.definition,admission(f.definition)));assertEquals(before,f.parent.applied());assertEquals(1,f.parent.pollCdc(lineage(f.definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.beginning(),10,8<<20).records().size());f.freeze();f.seed();assertThrows(IllegalStateException.class,()->f.parent.apply(new ApplyPosition(1,3),f.write(f.parent,1,8)));assertEquals(new ApplyPosition(1,2),f.parent.applied());
            var ledger=SessionEpochLedger.initial(helper.outer.schema().clusterId(),helper.outer.schema().databaseId(),0).issue(new UUID(0,904),1000);var bind=DistributedCommand.sessionControl(f.parent.ownership().owner().groupId(),1,f.parent.schemaDigest(),new OperationId(issuer,1,0),RangeSessionControl.bind(ledger,ledger.grants().getFirst(),2),-1);assertFalse(f.parent.metadataTopologyControl(bind));assertThrows(IllegalStateException.class,()->f.parent.validate(bind));assertThrows(IllegalStateException.class,()->f.parent.apply(new ApplyPosition(1,3),bind));assertEquals(new ApplyPosition(1,2),f.parent.applied());
        }
    }
    @Test void lateCdcEnableDoesNotInventPreexistingCachedFreezeBarrier()throws Exception {
        var definition=helper.topology.split(helper.outer.schema(),101);try(var pool=helper.pool(root)) { var parent=helper.open(pool,definition.parents().getFirst());parent.initializeConfiguration(new byte[]{1});parent.applyNoop(new ApplyPosition(1,0));var freeze=parent.applyTopologyFreeze(new ApplyPosition(1,1),definition,admission(definition));parent.enableCdc(definition.parents().getFirst().rangeId());assertTrue(parent.pollRetiredCdc(lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.after(freeze.position()),10,8<<20).records().isEmpty());assertThrows(ReplicatedRangeStore.CdcGap.class,()->parent.pollRetiredCdc(lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.beginning(),10,8<<20));assertEquals(freeze,parent.applyTopologyFreeze(new ApplyPosition(1,2),definition,admission(definition)));assertTrue(parent.pollRetiredCdc(lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.after(freeze.position()),10,8<<20).records().isEmpty()); }
    }
    @Test void barrierUsesOrdinaryRecordCapacityAndCountEvictionDeletesMatchingAgeCell()throws Exception {
        try(var f=new Fixture(root)) {
            for(int i=0;i<4096;i++)f.parent.apply(new ApplyPosition(1,i+1),f.write(f.parent,i,9));f.freeze();
            var gap=assertThrows(ReplicatedRangeStore.CdcGap.class,()->f.retired(DistributedCdcCheckpoint.Cursor.beginning()));assertEquals(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)),gap.page().floor());
            var terminal=f.retired(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4096)));assertEquals(1,terminal.records().size());assertEquals(f.freeze,terminal.records().getFirst().barrier());
            byte[] key=ByteBuffer.allocate(25).putLong(f.parent.ownership().owner().groupId().getMostSignificantBits()).putLong(f.parent.ownership().owner().groupId().getLeastSignificantBits()).put((byte)0x52).putLong(1).array();assertNull(helper.db(f.pool).get(helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN),key));key[16]=0x50;assertNull(helper.db(f.pool).get(helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN),key));
        }
    }

    @Test void catalogueTopologyPrefixIsBoundedAtOneCutAndRejectsDataRole()throws Exception {
        try(var f=new Fixture(root.resolve("data"))) { assertThrows(IllegalStateException.class,()->f.parent.readCurrentMetadataPrefix(helper.outer.fixed,new byte[]{(byte)0x82},17,128,1024)); }
        try(var pool=helper.pool(root.resolve("catalogue"))) {
            UUID group=new UUID(0,905),column=new UUID(0,906);var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(group,Set.of(column),null,null),new byte[32],Map.of(column,"catalogue"),Set.of());store.enableCatalogueScopeCompaction();store.initializeConfiguration(new byte[]{1});store.applyNoop(new ApplyPosition(1,0));
            byte[] first=ByteBuffer.allocate(17).put((byte)0x82).putLong(0).putLong(1).array(),second=ByteBuffer.allocate(17).put((byte)0x82).putLong(0).putLong(2).array(),other=ByteBuffer.allocate(17).put((byte)0x83).putLong(0).putLong(3).array();
            byte[] directory=ByteBuffer.allocate(16).putLong(0x8200000000000000L).putLong(9).array();var base=helper.outer.schema();var directorySchema=new DistributedSchema(base.clusterId(),base.databaseId(),new UUID(0x8200000000000000L,9),1,base.routingPrefixBytes(),base.columns());var declared=helper.topology.directory(directorySchema,1,List.of(helper.topology.range(20,helper.outer.group,null,null)));
            store.apply(new ApplyPosition(1,1),new DistributedCommand(group,1,new byte[32],new OperationId(issuer,1,0),List.of(new Mutation(Mutation.Kind.PUT,column,directory,RangeDirectoryCodec.encode(declared)),new Mutation(Mutation.Kind.PUT,column,first,new byte[8]),new Mutation(Mutation.Kind.PUT,column,second,new byte[8]),new Mutation(Mutation.Kind.PUT,column,other,new byte[8]))));
            var inventory=store.readCurrentMetadataPrefix(column,new byte[]{(byte)0x82},17,128,64);assertEquals(new ApplyPosition(1,1),inventory.position());assertEquals(2,inventory.entries().size());assertArrayEquals(first,inventory.entries().getFirst().key());byte[] returned=inventory.entries().getFirst().value();returned[0]=99;assertEquals(0,inventory.entries().getFirst().value()[0]);assertThrows(UnsupportedOperationException.class,()->inventory.entries().clear());
            assertThrows(IllegalStateException.class,()->store.readCurrentMetadataPrefix(column,new byte[]{(byte)0x82},17,1,64));assertThrows(IllegalStateException.class,()->store.readCurrentMetadataPrefix(column,new byte[]{(byte)0x82},17,128,49));assertThrows(IllegalArgumentException.class,()->store.readCurrentMetadataPrefix(column,new byte[]{(byte)0x81},17,128,64));assertEquals(new ApplyPosition(1,1),store.applied());
        }
    }

    @Test void missingTerminalWithAdjustedSummaryCannotReopenOrPublishAnImage()throws Exception {
        Path path=root.resolve("corrupt-terminal");TopologyChange.Definition definition;
        try(var f=new Fixture(path)) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();definition=f.definition;var state=helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN);var db=helper.db(f.pool);UUID group=f.parent.ownership().owner().groupId();
            byte[] terminal=ByteBuffer.allocate(25).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x50).putLong(2).array(),age=terminal.clone();age[16]=0x52;byte[] first=terminal.clone();ByteBuffer.wrap(first,17,8).putLong(1);byte[] summaryKey=Arrays.copyOf(terminal,17);summaryKey[16]=0x51;byte[] summary=db.get(state,summaryKey);ByteBuffer.wrap(summary,60,4).putInt(1);ByteBuffer.wrap(summary,64,8).putLong(58L+db.get(state,first).length);
            try(var batch=new org.rocksdb.WriteBatch();var options=new org.rocksdb.WriteOptions().setDisableWAL(false).setSync(true)) { batch.delete(state,terminal);batch.delete(state,age);batch.put(state,summaryKey,summary);db.write(options,batch); }
            assertThrows(java.io.IOException.class,()->NativeRangeImage.write(root.resolve("invalid-image"),f.parent,limits));
        }
        try(var pool=helper.pool(path)) { var parent=helper.open(pool,definition.parents().getFirst());assertThrows(IllegalArgumentException.class,()->parent.enableCdc(definition.parents().getFirst().rangeId())); }
    }

}
