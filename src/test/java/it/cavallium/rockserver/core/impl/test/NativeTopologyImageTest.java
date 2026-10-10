package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Existing original member native image transfer; no new-member bootstrap, Raft or transport claim. */
class NativeTopologyImageTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologySeedTest helper=new NativeTopologySeedTest();
    final ReplicaDescriptor first=helper.topology.replica;
    final ReplicaDescriptor second=new ReplicaDescriptor(new UUID(0,50),new UUID(0,51),new UUID(0,52),"second","127.0.0.1:9998",ReplicaDescriptor.Role.VOTER);
    final UUID catalogue=new UUID(0,900),extra=new UUID(0,901);
    final ApplyPosition catalogueCut=new ApplyPosition(1,10);
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,32<<20,5);
    TopologyChange.Definition definition() {
        var template=helper.topology.split(helper.outer.schema(),100);
        java.util.function.Function<RangeDescriptor,RangeDescriptor> place=r->new RangeDescriptor(r.rangeId(),r.groupId(),r.rangeEpoch(),r.configurationVersion(),r.lowerInclusive(),r.upperExclusive(),r.topologyState(),List.of(first,second));
        return new TopologyChange.Definition(template.intentId(),helper.topology.directory(helper.outer.schema(),1,template.parents().stream().map(place).toList()),helper.topology.directory(helper.outer.schema(),2,template.targets().stream().map(place).toList()));
    }
    byte[] admission(TopologyChange.Definition definition)throws Exception { byte[] fencing=TopologyChange.State.begin(definition.header()).beginFencing().encode();return java.security.MessageDigest.getInstance("SHA-256").digest(java.nio.ByteBuffer.allocate(16+fencing.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(fencing).array()); }
    Map<UUID,String> names(String prefix) { var names=new HashMap<UUID,String>();helper.outer.names.forEach((column,name)->names.put(column,prefix+name));return Map.copyOf(names); }
    final class Fixture implements AutoCloseable {
        final DistributedStoragePool pool;final NativeTopologySeed seed;final ReplicatedRangeStore child;final List<ReplicatedRangeStore> children;
        final TopologyChange.Definition definition;final String prefix;
        Fixture(Path path,ReplicaDescriptor local,String prefix)throws Exception {
            this.prefix=prefix;definition=definition();pool=helper.pool(path);var schema=helper.outer.schema();var parentRange=definition.parents().getFirst();var parent=pool.openRange(new RangeOwnership.Owner(parentRange.groupId(),parentRange.rangeEpoch()),new RangeOwnership.Interval(schema.domainId(),schema.columnIds(),null,null),schema.digest(),names(prefix),Set.of());parent.enableLogicalSchema(schema);parent.applyNoop(new ApplyPosition(1,0));var freeze=parent.applyTopologyFreeze(new ApplyPosition(1,1),definition,admission(definition));
            var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(first.replicaId())).frozen(freeze.proof(second.replicaId()));seed=pool.seedTopology(definition,authority,List.of(parent),local);var stores=new ArrayList<ReplicatedRangeStore>();
            for(var target:definition.targets()) { var store=pool.reopenTopologyRange(seed.birth(target.groupId()),schema);store.initializeConfiguration(new byte[]{1});store.applyNoop(new ApplyPosition(1,0));store.applyTopologyActivation(new ApplyPosition(1,1),definition,authority.freezeDigest(),catalogue,catalogueCut,NativeTopologyActivation.publicationCommitment(definition,authority.freezeDigest(),catalogue));stores.add(store); }children=List.copyOf(stores);child=children.getFirst();
        }
        DistributedCommand write(int value) { return new DistributedCommand(child.ownership().owner().groupId(),1,child.schemaDigest(),new OperationId(helper.outer.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(1),helper.helper.key(value)))); }
        void cdc()throws Exception { child.enableCdc(definition.targets().getFirst().rangeId()); }
        void evolve()throws Exception {
            var source=helper.outer.schema();var columns=new ArrayList<>(source.columns());columns.add(new DistributedSchema.Column(901,extra,"added",columns.getFirst().schema(),null));var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch()+1,source.routingPrefixBytes(),columns);
            var change=new SchemaChange.Definition(new UUID(0,902),new OperationId(new UUID(0,903),1,0),definition.targetDirectory(),source,target);var targetNames=new HashMap<>(names(prefix));targetNames.put(extra,prefix+"added");
            for(var store:children) { store.applySchemaFence(new ApplyPosition(1,2),change.header());var installation=pool.installSchema(store,new ApplyPosition(1,3),change,targetNames,Set.of());store.applySchemaActivation(new ApplyPosition(1,4),installation.proof(),catalogue,catalogueCut); }
        }
        @Override public void close() throws java.io.IOException { pool.close(); }
    }
    void assertLocal(ReplicatedRangeStore child,NativeTopologySeed seed,NativeTopologyActivation receipt)throws Exception {
        assertEquals(seed.birth(child.ownership().owner().groupId()),child.topologyBirth());assertArrayEquals(receipt.encode(),child.topologyActivation().encode());assertEquals(seed.localReplica().replicaId(),child.topologyActivation().proof().owner().replicaId());assertArrayEquals(seed.birth(child.ownership().owner().groupId()).proof(seed).digest(),child.topologyActivation().proof().predecessorDigest());
    }
    @Test void activatedChildRoundTripRestoresUserReceiptCdcAndColdLocalProof()throws Exception {
        Path path=root.resolve("same"),image=root.resolve("same-image");NativeTopologySeed seed;NativeTopologyActivation receipt;DistributedCommand command;byte[] result;
        try(var f=new Fixture(path,first,"")) {
            f.cdc();command=f.write(9);result=f.child.apply(new ApplyPosition(1,2),command);seed=f.seed;receipt=f.child.topologyActivation();var manifest=NativeRangeImage.write(image,f.child,limits);assertNotNull(manifest.topologyOrigin());assertArrayEquals(seed.encode(),manifest.topologyOrigin().seed().encode());
            var later=new DistributedCommand(command.groupId(),1,f.child.schemaDigest(),new OperationId(new UUID(0,904),1,0),List.of(new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(1),helper.helper.key(8))));f.child.apply(new ApplyPosition(1,3),later);f.child.fence();f.child.installSnapshot(image,limits);f.child.activate();assertLocal(f.child,seed,receipt);assertArrayEquals(helper.helper.key(9),f.child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertArrayEquals(result,f.child.operationResult(command.operationId(),command.receiptDigest()));
            var lineage=new DistributedCdcCheckpoint.Lineage(f.definition.targets().getFirst().rangeId(),command.groupId(),1);assertEquals(1,f.child.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)),10,8<<20).records().size());
        }
        try(var pool=helper.pool(path)) { var child=pool.reopenTopologyRange(seed.birth(command.groupId()),helper.outer.schema());assertLocal(child,seed,receipt);assertArrayEquals(result,child.operationResult(command.operationId(),command.receiptDigest()));assertEquals(new ApplyPosition(1,2),child.applied()); }
    }
    @Test void foreignOriginalMemberAndAliasesNormalizeToRetainedReceiverRecipeAcrossReopen()throws Exception {
        Path targetPath=root.resolve("target"),image=root.resolve("foreign-image");NativeTopologySeed localSeed;NativeTopologyActivation localReceipt;DistributedCommand command;byte[] result;
        try(var source=new Fixture(root.resolve("source"),first,"source-");var target=new Fixture(targetPath,second,"target-")) {
            source.cdc();target.cdc();command=source.write(9);result=source.child.apply(new ApplyPosition(1,2),command);localSeed=target.seed;localReceipt=target.child.topologyActivation();assertFalse(Arrays.equals(source.seed.digest(),localSeed.digest()));NativeRangeImage.write(image,source.child,limits);target.child.fence();target.child.installSnapshot(image,limits);target.child.activate();assertLocal(target.child,localSeed,localReceipt);assertArrayEquals(helper.helper.key(9),target.child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertArrayEquals(result,target.child.operationResult(command.operationId(),command.receiptDigest()));assertEquals(names("target-"),target.pool.schemaPhysicalNames(target.child));
        }
        try(var pool=helper.pool(targetPath)) { var child=pool.reopenTopologyRange(localSeed.birth(command.groupId()),helper.outer.schema());assertLocal(child,localSeed,localReceipt);assertEquals(names("target-"),pool.schemaPhysicalNames(child));var next=new DistributedCommand(command.groupId(),1,child.schemaDigest(),new OperationId(new UUID(0,905),1,0),List.of(new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(1),helper.helper.key(8))));child.apply(new ApplyPosition(1,3),next);assertArrayEquals(helper.helper.key(8),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1))); }
    }
    @Test void equivalentVerifiedSchemaEvolutionRetainsLocalAliasHistoryAndBehindReceiverRejects()throws Exception {
        Path image=root.resolve("evolved-image"),targetPath=root.resolve("evolved-target");NativeTopologySeed localSeed;NativeTopologyActivation localReceipt;
        try(var source=new Fixture(root.resolve("evolved-source"),first,"source-");var target=new Fixture(targetPath,second,"target-");var behind=new Fixture(root.resolve("behind"),second,"behind-")) {
            source.evolve();target.evolve();localSeed=target.seed;localReceipt=target.child.topologyActivation();var localRecord=target.child.schemaInstallation();var command=new DistributedCommand(source.child.ownership().owner().groupId(),1,source.child.schemaDigest(),new OperationId(helper.outer.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,extra,helper.helper.key(1),helper.helper.key(9))));source.child.apply(new ApplyPosition(1,5),command);NativeRangeImage.write(image,source.child,limits);
            var before=behind.child.applied();behind.child.fence();assertThrows(IOException.class,()->behind.child.installSnapshot(image,limits));assertEquals(before,behind.child.applied());assertFalse(behind.child.pendingInstallation());
            target.child.fence();target.child.installSnapshot(image,limits);target.child.activate();assertLocal(target.child,localSeed,localReceipt);assertArrayEquals(localRecord.encode(),target.child.schemaInstallation().encode());assertEquals("target-added",target.pool.schemaPhysicalNames(target.child).get(extra));assertArrayEquals(helper.helper.key(9),target.child.readCurrentPoint(extra,helper.helper.key(1)));
        }
        try(var pool=helper.pool(targetPath)) { var child=pool.reopenTopologyRange(localSeed.birth(definition().targets().getFirst().groupId()),helper.outer.schema());assertLocal(child,localSeed,localReceipt);assertEquals("target-added",pool.schemaPhysicalNames(child).get(extra));assertArrayEquals(helper.helper.key(9),child.readCurrentPoint(extra,helper.helper.key(1))); }
    }
    @Test void interruptedForeignInstallKeepsLocalEvidenceAndColdExactRetryCompletes()throws Exception {
        Path path=root.resolve("interrupted"),image=root.resolve("interrupted-image");NativeTopologySeed seed;NativeTopologyActivation receipt;
        try(var source=new Fixture(root.resolve("interrupted-source"),first,"source-");var target=new Fixture(path,second,"target-")) {
            source.child.apply(new ApplyPosition(1,2),source.write(9));NativeRangeImage.write(image,source.child,limits);seed=target.seed;receipt=target.child.topologyActivation();target.child.fence();assertThrows(IOException.class,()->target.child.installSnapshot(image,limits,batch->{throw new IOException("Controlled interruption");}));assertTrue(target.child.pendingInstallation());assertLocal(target.child,seed,receipt);assertNull(target.child.applied());
        }
        try(var pool=helper.pool(path)) { var child=pool.reopenTopologyRange(seed.birth(definition().targets().getFirst().groupId()),helper.outer.schema());assertTrue(child.pendingInstallation());assertEquals(RangeOwnership.State.FENCED,child.ownership().state());assertThrows(IllegalStateException.class,child::activate);assertLocal(child,seed,receipt);child.installSnapshot(image,limits);child.activate();assertLocal(child,seed,receipt);assertArrayEquals(helper.helper.key(9),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1))); }
    }
    @Test void malformedOrForeignOriginCannotChangeTargetMarkerOrUserRows()throws Exception {
        try(var f=new Fixture(root.resolve("invalid"),first,"")) {
            f.child.apply(new ApplyPosition(1,2),f.write(9));Path image=root.resolve("good-image"),truncated=root.resolve("truncated-image");NativeRangeImage.write(image,f.child,limits);var bytes=Files.readAllBytes(image);Files.write(truncated,Arrays.copyOf(bytes,bytes.length-1));var before=f.child.applied();var origin=NativeRangeImage.inspect(image,limits).topologyOrigin();f.child.fence();assertThrows(IOException.class,()->f.child.installSnapshot(truncated,limits));assertEquals(before,f.child.applied());assertThrows(IOException.class,()->f.child.validateTopologyImageOrigin(null));
            try(var ordinaryPool=helper.pool(root.resolve("unqualified-source"))) { var range=f.definition.targets().getFirst();var ordinary=ordinaryPool.openRange(new RangeOwnership.Owner(range.groupId(),1),new RangeOwnership.Interval(helper.outer.schema().domainId(),helper.outer.schema().columnIds(),range.lowerInclusive(),range.upperExclusive()),helper.outer.schema().digest(),names("ordinary-"),Set.of());ordinary.enableLogicalSchema(helper.outer.schema());ordinary.initializeConfiguration(new byte[]{1});ordinary.applyNoop(new ApplyPosition(1,0));Path missing=root.resolve("missing-origin");NativeRangeImage.write(missing,ordinary,limits);assertNull(NativeRangeImage.inspect(missing,limits).topologyOrigin());assertThrows(IOException.class,()->f.child.installSnapshot(missing,limits));assertEquals(before,f.child.applied()); }
            var crossed=new NativeTopologyActivation(f.seed,f.seed.birth(f.definition.targets().getLast().groupId()),new ApplyPosition(1,1),catalogue,catalogueCut,origin.activation().publicationCommitment());assertThrows(IOException.class,()->f.child.validateTopologyImageOrigin(new NativeTopologyImageOrigin(f.seed,crossed)));
            var wrongCatalogueCut=new NativeTopologyActivation(f.seed,f.seed.birth(f.child.ownership().owner().groupId()),new ApplyPosition(1,1),catalogue,new ApplyPosition(1,11),origin.activation().publicationCommitment());assertThrows(IOException.class,()->f.child.validateTopologyImageOrigin(new NativeTopologyImageOrigin(f.seed,wrongCatalogueCut)));
            var wrongReceipt=new NativeTopologyActivation(f.seed,f.seed.birth(f.child.ownership().owner().groupId()),new ApplyPosition(1,2),catalogue,catalogueCut,origin.activation().publicationCommitment());assertThrows(IOException.class,()->f.child.validateTopologyImageOrigin(new NativeTopologyImageOrigin(f.seed,wrongReceipt)));assertEquals(before,f.child.applied());f.child.activate();assertArrayEquals(helper.helper.key(9),f.child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));
        }
    }
}
