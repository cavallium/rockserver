package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import static org.junit.jupiter.api.Assertions.*;

/** Independent source metadata recovery must never import, clear, or admit descendant USER data. */
class NativeMetadataRecoveryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyCdcTest nativeTest=new NativeTopologyCdcTest();
    final NativeTopologySeedTest helper=nativeTest.helper;
    NativeMetadataRecoveryRegistration registration(NativeTopologyCdcTest.Fixture source,NativeRangeImage.Manifest image) throws Exception {
        var cfg=source.parent.configuration();
        return new NativeMetadataRecoveryRegistration(new UUID(0,1100),nativeTest.catalogue,new ApplyPosition(1,20),source.definition.sourceDirectory(),source.freeze,helper.topology.replica,helper.topology.replica,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of());
    }
    ReplicatedRangeStore.TopologyOperationOutcome outcome(ReplicatedRangeStore target,TopologyChange.Definition definition,OperationId id,byte[] digest)throws Exception {
        var schema=helper.outer.schema();return target.readTopologyOperationOutcome(schema.clusterId(),schema.databaseId(),schema.domainId(),nativeTest.lineage(definition.parents().getFirst()),id,digest);
    }
    void configure(ReplicatedRangeStore target,TopologyChange.Definition definition)throws Exception {
        target.enableSessionControls();target.enableCdc(definition.parents().getFirst().rangeId());
    }
    @Test void sourceSchemaActivatedBeforeRetirementRecoversAsMetadataWithoutUserHandlesOrHistoricalSeed()throws Exception {
        Path initial=root.resolve("evolved-source-meta"),checkpoint=root.resolve("evolved-own-meta"),receiver=root.resolve("evolved-meta-receiver");
        var limits=new NativeRangeImage.Limits(4<<20,32<<20,8);var base=helper.outer.schema();UUID added=new UUID(0,9501);
        var columns=new ArrayList<>(base.columns());columns.add(new DistributedSchema.Column(9501,added,"created-before-retirement",base.column(helper.outer.fixed).schema(),null));
        var evolved=new DistributedSchema(base.clusterId(),base.databaseId(),base.domainId(),base.schemaEpoch()+1,base.routingPrefixBytes(),columns);
        var originalDirectory=helper.topology.split(base,9502).sourceDirectory();
        var create=new SchemaChange.Definition(new UUID(0,9503),new OperationId(new UUID(0,9504),1,0),originalDirectory,base,evolved);
        var children=helper.topology.split(evolved,9505).targets();
        var definition=new TopologyChange.Definition(new UUID(0,9505),create.targetDirectory(),helper.topology.directory(evolved,create.targetDirectory().generation()+1,children));
        var range=definition.parents().getFirst();var lineage=nativeTest.lineage(range);
        DistributedCommand original,created;byte[] originalResult,createdResult;NativeSchemaInstallation installation;NativeSchemaActivationProof activation;NativeMetadataRecoveryRegistration registration;NativeRangeImage.Manifest own;
        try(var pool=helper.pool(root.resolve("evolved-meta-source"))){
            var source=helper.open(pool,range);source.initializeConfiguration(new byte[]{1});source.enableSessionControls();source.enableCdc(range.rangeId());source.applyNoop(new ApplyPosition(1,0));
            original=new DistributedCommand(range.groupId(),range.rangeEpoch(),base.digest(),new OperationId(helper.outer.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(1),helper.helper.key(9))));
            originalResult=source.apply(new ApplyPosition(1,source.applied().index()+1),original);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(originalResult).status());
            source.applySchemaFence(new ApplyPosition(1,source.applied().index()+1),create.header());var aliases=new HashMap<>(helper.outer.names);aliases.put(added,"created-before-retirement");
            installation=pool.installSchema(source,new ApplyPosition(1,source.applied().index()+1),create,aliases,Set.of());
            activation=source.applySchemaActivation(new ApplyPosition(1,source.applied().index()+1),installation.proof(),nativeTest.catalogue,new ApplyPosition(1,30));
            created=new DistributedCommand(range.groupId(),range.rangeEpoch(),evolved.digest(),new OperationId(helper.outer.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,added,helper.helper.key(1),helper.helper.key(7))));
            createdResult=source.apply(new ApplyPosition(1,source.applied().index()+1),created);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(createdResult).status());
            var freeze=source.applyTopologyFreeze(new ApplyPosition(1,source.applied().index()+1),definition,nativeTest.admission(definition));
            var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(helper.topology.replica.replicaId()));
            pool.seedTopology(definition,authority,List.of(source),helper.topology.replica);assertTrue(source.topologyRetired());assertTrue(freeze.position().index()>activation.position().index());
            var image=NativeRangeImage.writeMetadata(initial,source,limits);assertArrayEquals(evolved.digest(),image.schemaDigest());assertEquals(evolved.columnIds(),image.interval().columns());
            var cfg=source.configuration();registration=new NativeMetadataRecoveryRegistration(new UUID(0,9506),nativeTest.catalogue,new ApplyPosition(1,40),definition.sourceDirectory(),freeze,helper.topology.replica,helper.topology.replica,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of());
            NativeRangeImage.visitMetadata(initial,limits,(section,column,key,value)->assertNotEquals(NativeRangeImage.Section.USER,section));
        }
        try(var pool=helper.pool(receiver)){
            var handles=Set.copyOf(helper.handles(pool).keySet());assertTrue(Collections.disjoint(handles,helper.outer.names.values()));assertFalse(handles.contains("created-before-retirement"));assertTrue(pool.topologySeeds().isEmpty());
            var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.userHandlesDetached());assertFalse(target.admissionOpen());
            assertArrayEquals(create.targetDirectory().payloadDigest(),target.metadataRecoveryRegistration().sourceDirectory().payloadDigest());assertArrayEquals(evolved.digest(),target.schemaDigest());
            target.installMetadataSnapshot(initial,limits);assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::metadataOutcomeState);target.completeMetadataRecovery(registration,target.applied(),target.configuration());
            assertArrayEquals(installation.encode(),target.schemaInstallation().encode());assertEquals(activation,target.schemaActivation());assertArrayEquals(evolved.digest(),target.installedSchema().digest());assertEquals(evolved.columnIds(),target.metadataOutcomeState().columns());
            assertArrayEquals(originalResult,outcome(target,definition,original.operationId(),original.receiptDigest()).result());assertArrayEquals(createdResult,outcome(target,definition,created.operationId(),created.receiptDigest()).result());
            var page=target.pollRetiredCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),256,8<<20);assertEquals(3,page.records().size());assertArrayEquals(base.digest(),page.records().getFirst().command().schemaDigest());assertArrayEquals(evolved.digest(),page.records().get(1).command().schemaDigest());assertEquals(added,page.records().get(1).events().getFirst().mutation().columnId());assertEquals(registration.freeze(),page.records().getLast().barrier());
            assertThrows(IllegalStateException.class,target::activate);assertThrows(IllegalStateException.class,target::capture);assertThrows(IllegalStateException.class,()->target.readCurrentPoint(added,helper.helper.key(1)));assertThrows(IllegalStateException.class,()->target.apply(new ApplyPosition(1,target.applied().index()+1),new DistributedCommand(range.groupId(),range.rangeEpoch(),evolved.digest(),new OperationId(helper.outer.session,1,2),List.of(new Mutation(Mutation.Kind.PUT,added,helper.helper.key(2),helper.helper.key(8))))));
            target.applyNoop(new ApplyPosition(1,target.applied().index()+1),new byte[]{2});own=NativeRangeImage.writeMetadata(checkpoint,target,limits);assertEquals(target.applied(),target.validateMetadataRecoveryImage(checkpoint,limits).position());
            target.installMetadataSnapshot(checkpoint,limits);assertEquals(own.position(),target.applied());assertFalse(target.metadataRecoveryPending());assertEquals(handles,helper.handles(pool).keySet());assertTrue(pool.topologySeeds().isEmpty());assertEquals(Set.of(range.groupId()),pool.persistedRangeGroups(8));
            assertThrows(IOException.class,()->target.installMetadataSnapshot(checkpoint,limits,batch->{throw new IOException("Interrupt evolved metadata restore");}));assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
        }
        try(var pool=helper.pool(receiver)){
            var handles=Set.copyOf(helper.handles(pool).keySet());var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.userHandlesDetached());assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertNull(target.applied());
            target.installMetadataSnapshot(checkpoint,limits);assertEquals(own.position(),target.applied());assertFalse(target.metadataRecoveryPending());assertArrayEquals(installation.encode(),target.schemaInstallation().encode());assertEquals(activation,target.schemaActivation());assertEquals(evolved.columnIds(),target.metadataOutcomeState().columns());
            assertArrayEquals(originalResult,outcome(target,definition,original.operationId(),original.receiptDigest()).result());assertArrayEquals(createdResult,outcome(target,definition,created.operationId(),created.receiptDigest()).result());assertEquals(registration.freeze(),target.pollRetiredCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),256,8<<20).records().getLast().barrier());
            NativeRangeImage.writeMetadata(root.resolve("evolved-cold-meta-reexport"),target,limits);assertEquals(handles,helper.handles(pool).keySet());assertTrue(Collections.disjoint(handles,helper.outer.names.values()));assertFalse(handles.contains("created-before-retirement"));assertTrue(pool.topologySeeds().isEmpty());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::capture);
        }
    }

    @Test void absentHistoricalMemberInstallsOnlyMetadataThenQualifiesActualTailAndReopens()throws Exception {
        Path image=root.resolve("source-metadata"),path=root.resolve("receiver");NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;DistributedCommand original,ack;byte[] result;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {
            original=source.write(source.parent,0,9);result=source.parent.apply(new ApplyPosition(1,1),original);source.freeze();source.seed();definition=source.definition;
            registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));assertEquals(registration,NativeMetadataRecoveryRegistration.decode(registration.encode()));
            ack=new DistributedCommand(original.groupId(),1,original.schemaDigest(),new OperationId(original.operationId().sessionId(),1,1),List.of(),List.of(),0);source.parent.apply(new ApplyPosition(1,3),ack);
        }
        try(var pool=helper.pool(path)) {
            var child=helper.open(pool,definition.targets().getFirst());child.initializeConfiguration(new byte[]{1});child.applyNoop(new ApplyPosition(3,0));child.apply(new ApplyPosition(3,1),helper.put(child.ownership().owner().groupId(),0,1,7));
            assertTrue(pool.topologySeeds().isEmpty());var target=pool.openMetadataRecovery(registration);configure(target,definition);
            assertTrue(target.userHandlesDetached());assertTrue(target.metadataRecoveryPending());assertFalse(target.metadataRecoveryImageInstalled());assertFalse(target.admissionOpen());
            assertEquals(Set.of(registration.groupId(),child.ownership().owner().groupId()),pool.persistedRangeGroups(8));
            assertTrue(pool.retainedMetadataOwners().stream().anyMatch(owner->owner.owner().groupId().equals(registration.groupId()) && owner.retired() && owner.pending()));
            assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            target.installMetadataSnapshot(image,nativeTest.limits);assertEquals(registration.imageCut(),target.applied());assertTrue(target.metadataRecoveryImageInstalled());assertTrue(target.metadataRecoveryPending());
            assertThrows(IllegalStateException.class,()->target.operationResult(original.operationId(),original.receiptDigest()));
            assertThrows(IllegalStateException.class,()->target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration()));
            // Committed source tail may replay while every public authoritative outcome remains fenced.
            target.apply(new ApplyPosition(1,3),ack);assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));
            var wrong=new ReplicatedRangeStore.Configuration(0,-1,new byte[]{2});assertThrows(IllegalArgumentException.class,()->target.completeMetadataRecovery(registration,target.applied(),wrong));
            target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration());assertFalse(target.metadataRecoveryPending());
            target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration());assertThrows(IllegalArgumentException.class,()->target.completeMetadataRecovery(registration,new ApplyPosition(1,2),target.configuration()),"Completed META retry binds its original required cut");
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(outcome(target,definition,original.operationId(),original.receiptDigest()).result()).status());
            assertNull(outcome(target,definition,new OperationId(new UUID(0,1101),2,0),new byte[32]).result());
            var page=target.pollRetiredCdc(nativeTest.lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.beginning(),256,8<<20);assertEquals(2,page.records().size());assertEquals(registration.freeze(),page.records().getLast().barrier());
            assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertEquals(new ApplyPosition(3,1),child.applied());assertEquals(0,target.activeSnapshotPins());assertTrue(pool.topologySeeds().isEmpty());
            assertThrows(IllegalStateException.class,()->helper.open(pool,definition.parents().getFirst()));
        }
        try(var pool=helper.pool(path)) {
            assertEquals(List.of(registration),pool.metadataRecoveries());assertEquals(Set.of(registration.groupId(),definition.targets().getFirst().groupId()),pool.persistedRangeGroups(8));assertTrue(pool.retainedMetadataOwners().stream().anyMatch(owner->owner.owner().groupId().equals(registration.groupId()) && owner.retired() && !owner.pending()));var target=pool.openMetadataRecovery(registration);configure(target,definition);assertFalse(target.metadataRecoveryPending());assertEquals(new ApplyPosition(1,3),target.applied());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(outcome(target,definition,original.operationId(),original.receiptDigest()).result()).status());
            var child=helper.open(pool,definition.targets().getFirst());assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertTrue(target.userHandlesDetached());
        }
    }
    @Test void interruptedMetadataInstallReopensFencedAndRetriesExactOriginalImage()throws Exception {
        Path image=root.resolve("metadata"),path=root.resolve("receiver");NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;DistributedCommand original;byte[] result;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {original=source.write(source.parent,0,9);result=source.parent.apply(new ApplyPosition(1,1),original);source.freeze();source.seed();definition=source.definition;registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));}
        try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(registration);configure(target,definition);target.installMetadataSnapshot(image,nativeTest.limits);target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());assertFalse(target.metadataRecoveryPending());assertThrows(IOException.class,()->target.installMetadataSnapshot(image,nativeTest.limits,batches->{throw new IOException("Lost install completion");}));assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::metadataOutcomeState);assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));}
        try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());target.installMetadataSnapshot(image,nativeTest.limits);assertFalse(target.pendingInstallation());target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());assertArrayEquals(result,outcome(target,definition,original.operationId(),original.receiptDigest()).result());}
    }
    @Test void completedMetadataOwnerRestoresOwnCheckpointWithExactInterruptedColdRetry()throws Exception {
        Path initial=root.resolve("initial-meta"),earlier=root.resolve("earlier-own-meta"),latest=root.resolve("latest-own-meta"),receiver=root.resolve("own-meta-receiver");
        NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;DistributedCommand original;NativeRangeImage.Manifest checkpoint;
        try(var source=nativeTest.new Fixture(root.resolve("own-meta-source"))) {
            original=source.write(source.parent,0,9);source.parent.apply(new ApplyPosition(1,1),original);source.freeze();source.seed();definition=source.definition;
            registration=registration(source,NativeRangeImage.writeMetadata(initial,source.parent,nativeTest.limits));
        }
        try(var pool=helper.pool(receiver)) {
            pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,8L<<30,java.time.Duration.ofSeconds(60),0));
            var child=helper.open(pool,definition.targets().getFirst());child.initializeConfiguration(new byte[]{1});child.applyNoop(new ApplyPosition(3,0));child.apply(new ApplyPosition(3,1),helper.put(child.ownership().owner().groupId(),0,1,7));
            var target=pool.openMetadataRecovery(registration);configure(target,definition);target.installMetadataSnapshot(initial,nativeTest.limits);target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());
            target.applyNoop(new ApplyPosition(1,3),new byte[]{2});var first=NativeRangeImage.writeMetadata(earlier,target,nativeTest.limits);
            assertEquals(first.position(),target.validateMetadataRecoveryImage(earlier,nativeTest.limits).position());target.validateMetadataRecoveryImage(initial,nativeTest.limits);
            var receipt=new ArrayList<byte[]>();NativeRangeImage.visitMetadata(earlier,nativeTest.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.RECEIPTS && receipt.isEmpty())receipt.add(value.clone());});assertFalse(receipt.isEmpty());
            byte[] replacement=receipt.getFirst().clone();replacement[replacement.length-1]^=1;Path forged=replaceMetadataBytes(earlier,root.resolve("forged-own-meta"),receipt.getFirst(),replacement);
            NativeRangeImage.inspectMetadata(forged,nativeTest.limits);assertThrows(IOException.class,()->target.validateMetadataRecoveryImage(forged,nativeTest.limits));assertThrows(IOException.class,()->target.installMetadataSnapshot(forged,nativeTest.limits));assertFalse(target.pendingInstallation());
            try(var freshPool=helper.pool(root.resolve("fresh-meta-owner"))) {
                var fresh=freshPool.openMetadataRecovery(registration);configure(fresh,definition);assertThrows(IOException.class,()->fresh.installMetadataSnapshot(earlier,nativeTest.limits));assertNull(fresh.applied());
                fresh.installMetadataSnapshot(initial,nativeTest.limits);fresh.completeMetadataRecovery(registration,registration.imageCut(),fresh.configuration());assertThrows(IOException.class,()->fresh.installMetadataSnapshot(earlier,nativeTest.limits));assertEquals(registration.imageCut(),fresh.applied());
            }
            var ack=new DistributedCommand(original.groupId(),1,original.schemaDigest(),new OperationId(original.operationId().sessionId(),1,1),List.of(),List.of(),0);target.apply(new ApplyPosition(1,4),ack);
            checkpoint=NativeRangeImage.writeMetadata(latest,target,nativeTest.limits);target.validateMetadataRecoveryImage(earlier,nativeTest.limits);assertThrows(IOException.class,()->target.installMetadataSnapshot(earlier,nativeTest.limits));
            target.installMetadataSnapshot(latest,nativeTest.limits);assertFalse(target.metadataRecoveryPending());assertEquals(checkpoint.position(),target.applied());
            assertThrows(IOException.class,()->target.installMetadataSnapshot(latest,nativeTest.limits,batch->{throw new IOException("Interrupt own metadata replacement");}));assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));
        }
        try(var pool=helper.pool(receiver)) {
            pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,8L<<30,java.time.Duration.ofSeconds(60),0));
            var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertNull(target.applied());assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            var cfg=new ArrayList<byte[]>();NativeRangeImage.visitMetadata(latest,nativeTest.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==1)cfg.add(value.clone());});assertEquals(1,cfg.size());byte[] changedCfg=cfg.getFirst().clone();changedCfg[changedCfg.length-1]^=1;
            Path changed=replaceMetadataBytes(latest,root.resolve("changed-pending-meta"),cfg.getFirst(),changedCfg);assertThrows(IOException.class,()->target.installMetadataSnapshot(changed,nativeTest.limits));
            target.installMetadataSnapshot(latest,nativeTest.limits);assertFalse(target.pendingInstallation());assertFalse(target.metadataRecoveryPending());assertEquals(checkpoint.position(),target.metadataOutcomeState().applied());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(outcome(target,definition,original.operationId(),original.receiptDigest()).result()).status());
            var child=helper.open(pool,definition.targets().getFirst());assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));NativeRangeImage.writeMetadata(root.resolve("cold-own-meta-reexport"),target,nativeTest.limits);assertTrue(pool.topologySeeds().isEmpty());
        }
    }
    private Path replaceMetadataBytes(Path source,Path destination,byte[] before,byte[] after)throws Exception {
        assertEquals(before.length,after.length);byte[] bytes=java.nio.file.Files.readAllBytes(source);int offset=-1;
        for(int i=0;i<=bytes.length-before.length-32;i++)if(Arrays.equals(bytes,i,i+before.length,before,0,before.length)){offset=i;break;}
        assertTrue(offset>=0);System.arraycopy(after,0,bytes,offset,after.length);byte[] checksum=java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes,bytes.length-32));System.arraycopy(checksum,0,bytes,bytes.length-32,32);java.nio.file.Files.write(destination,bytes);return destination;
    }

    @Test void completedCurrentAtomicallyBecomesMetadataAcrossUnknownForceAndColdReopen()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();
        for(boolean after:List.of(false,true)) {
            Path path=root.resolve("current-meta-receiver-"+after),groupImage=root.resolve("current-image-"+after),metaImage=root.resolve("sealed-image-"+after);
            NativeCurrentRangeRecovery current;NativeMetadataRecoveryRegistration metadata;NativeTopologySeed descendants;TopologyChange.Definition definition;ApplyPosition tail;
            try(var source=nativeTest.new Fixture(root.resolve("current-meta-source-"+after));var pool=helper.pool(path)) {
                source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));
                var image=NativeRangeImage.write(groupImage,source.parent,nativeTest.limits);var cfg=source.parent.configuration();definition=source.definition;
                current=new NativeCurrentRangeRecovery(new UUID(0,1200),nativeTest.catalogue,new ApplyPosition(1,10),definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"current-meta-"),Set.of(),currentHelper.schemaRows(source.parent));
                var target=pool.openCurrentRangeRecovery(current);configure(target,definition);target.installSnapshot(groupImage,nativeTest.limits);
                source.freeze();source.seed();source.parent.applyNoop(new ApplyPosition(1,3),new byte[]{2});
                var sealed=NativeRangeImage.writeMetadata(metaImage,source.parent,nativeTest.limits);cfg=source.parent.configuration();
                metadata=new NativeMetadataRecoveryRegistration(new UUID(0,1201),current.catalogue(),new ApplyPosition(1,20),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
                metadata.requireTransitionOf(current);
                assertTrue(pool.metadataRecoveries().isEmpty());assertEquals(current,target.currentRangeRecoveryRegistration());
                target.completeCurrentRangeRecovery(current,image.position(),target.configuration());
                var freeze=target.applyTopologyFreeze(new ApplyPosition(1,2),definition,nativeTest.admission(definition));assertEquals(source.freeze,freeze);
                var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(helper.topology.replica.replicaId()));
                descendants=pool.seedTopology(definition,authority,List.of(target),helper.topology.replica);
                var child=pool.reopenTopologyRange(descendants.birth(definition.targets().getFirst().groupId()),helper.outer.schema());child.initializeConfiguration(new byte[]{1});child.applyNoop(new ApplyPosition(3,0));child.applyTopologyActivation(new ApplyPosition(3,1),definition,authority.freezeDigest(),nativeTest.catalogue,new ApplyPosition(1,10),NativeTopologyActivation.publicationCommitment(definition,authority.freezeDigest(),nativeTest.catalogue));child.apply(new ApplyPosition(3,2),source.write(child,0,7));
                target.applyNoop(new ApplyPosition(1,3),new byte[]{2});
                pool.metadataRecoveryForceObserver(boundary->{if(boundary==after)throw new RocksDBException("Lost CURRENT-to-META force acknowledgement");});
                assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(metadata));
                assertNull(target.currentRangeRecoveryRegistration());assertFalse(target.currentRangeRecoveryReady());assertTrue(target.userHandlesDetached());assertFalse(target.admissionOpen());assertTrue(target.metadataRecoveryPending());
                assertThrows(IllegalStateException.class,()->pool.openCurrentRangeRecovery(current));
                tail=new ApplyPosition(1,4);
                if(after){assertTrue(target.metadataRecoveryRegistrationForced(metadata));target.installMetadataSnapshot(metaImage,nativeTest.limits);target.applyNoop(tail,new byte[]{3});target.completeMetadataRecovery(metadata,tail,target.configuration());}
                var barriers=new java.util.concurrent.atomic.AtomicInteger();pool.metadataRecoveryForceObserver(boundary->{if(boundary)barriers.incrementAndGet();});assertSame(target,pool.openMetadataRecovery(metadata));assertEquals(1,barriers.get());
                assertTrue(pool.currentRangeRecoveries().isEmpty());assertEquals(List.of(metadata),pool.metadataRecoveries());
                if(!after){target.installMetadataSnapshot(metaImage,nativeTest.limits);target.applyNoop(tail,new byte[]{3});target.completeMetadataRecovery(metadata,tail,target.configuration());}
                assertEquals(tail,target.applied());assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertArrayEquals(new byte[]{3},target.configuration().data());
                assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertEquals(new ApplyPosition(3,2),child.applied());
            }
            try(var pool=helper.pool(path)) {
                assertTrue(pool.currentRangeRecoveries().isEmpty());assertEquals(List.of(metadata),pool.metadataRecoveries());var target=pool.openMetadataRecovery(metadata);configure(target,definition);assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertEquals(tail,target.applied());assertFalse(target.admissionOpen());assertNull(target.currentRangeRecoveryRegistration());
                var child=pool.reopenTopologyRange(descendants.birth(definition.targets().getFirst().groupId()),helper.outer.schema());assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertEquals(new ApplyPosition(3,2),child.applied());
            }
        }
    }

    @Test void unfinishedCurrentSupersessionColdReopensWithoutCompletingOrRestoringItsGroupImage()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();
        for(int stage=0;stage<3;stage++)for(boolean after:List.of(false,true)) {
            String id=stage+"-"+after;Path path=root.resolve("unfinished-"+id),groupImage=root.resolve("unfinished-group-"+id),metaImage=root.resolve("unfinished-meta-"+id);
            NativeCurrentRangeRecovery current;NativeMetadataRecoveryRegistration metadata;TopologyChange.Definition definition;ApplyPosition actual;
            try(var source=nativeTest.new Fixture(root.resolve("unfinished-source-"+id));var pool=helper.pool(path)) {
                source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));definition=source.definition;
                var image=NativeRangeImage.write(groupImage,source.parent,nativeTest.limits);var cfg=source.parent.configuration();
                current=new NativeCurrentRangeRecovery(new UUID(0,1250),nativeTest.catalogue,new ApplyPosition(1,10),definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"unfinished-"),Set.of(),currentHelper.schemaRows(source.parent));
                var target=pool.openCurrentRangeRecovery(current);configure(target,definition);
                if(stage==1)target.installSnapshot(groupImage,nativeTest.limits);
                if(stage==2)assertThrows(IOException.class,()->target.installSnapshot(groupImage,nativeTest.limits,batch->{throw new IOException("Interrupted GROUP restore");}));
                actual=target.applied();assertFalse(target.currentRangeRecoveryReady());assertEquals(stage==2,target.pendingInstallation());
                source.freeze();source.seed();var sealed=NativeRangeImage.writeMetadata(metaImage,source.parent,nativeTest.limits);cfg=source.parent.configuration();
                metadata=new NativeMetadataRecoveryRegistration(new UUID(0,1251),current.catalogue(),new ApplyPosition(1,20),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
                pool.metadataRecoveryForceObserver(boundary->{if(boundary==after)throw new RocksDBException("Lost sealed supersession acknowledgement");});
                assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(metadata));
                assertEquals(actual,target.applied());assertNull(target.currentRangeRecoveryRegistration());assertFalse(target.pendingInstallation());assertTrue(target.userHandlesDetached());assertTrue(target.metadataRecoveryPending());assertFalse(target.metadataRecoveryImageInstalled());assertFalse(target.admissionOpen());
                assertThrows(IllegalStateException.class,target::capture);assertThrows(IllegalStateException.class,target::metadataOutcomeState);
                assertThrows(IllegalStateException.class,()->helper.open(pool,definition.targets().getFirst()),"Uncertain role force must not expose the newly freed descendant interval");
            }
            try(var pool=helper.pool(path)) {
                ReplicatedRangeStore target;
                if(after){assertTrue(pool.currentRangeRecoveries().isEmpty());target=pool.openMetadataRecovery(metadata);}
                else {assertEquals(List.of(current),pool.currentRangeRecoveries());target=pool.openCurrentRangeRecovery(current);assertEquals(stage==2,target.pendingInstallation());assertSame(target,pool.openMetadataRecovery(metadata));}
                configure(target,definition);assertEquals(actual,target.applied());assertTrue(target.userHandlesDetached());assertFalse(target.pendingInstallation());assertFalse(target.metadataRecoveryImageInstalled());assertTrue(target.metadataRecoveryPending());
                target.installMetadataSnapshot(metaImage,nativeTest.limits);target.completeMetadataRecovery(metadata,target.applied(),target.configuration());
                assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertFalse(target.admissionOpen());assertTrue(pool.currentRangeRecoveries().isEmpty());
            }
        }
    }

    @Test void supersessionPreservesFinalForcedCurrentMarkerDespiteLostInstallAcknowledgement()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();Path groupImage=root.resolve("forced-current-group"),metaImage=root.resolve("forced-current-meta");
        try(var source=nativeTest.new Fixture(root.resolve("forced-current-source"));var pool=helper.pool(root.resolve("forced-current-receiver"))) {
            source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));var image=NativeRangeImage.write(groupImage,source.parent,nativeTest.limits);var cfg=source.parent.configuration();var definition=source.definition;
            var current=new NativeCurrentRangeRecovery(new UUID(0,1260),nativeTest.catalogue,new ApplyPosition(1,10),definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"forced-current-"),Set.of(),currentHelper.schemaRows(source.parent));
            var target=pool.openCurrentRangeRecovery(current);configure(target,definition);
            assertThrows(IOException.class,()->target.installSnapshot(groupImage,nativeTest.limits,new ReplicatedRangeStore.InstallationObserver(){public void afterBatch(int count){}public void afterCompletionForce()throws IOException{throw new IOException("Lost final image force acknowledgement");}}));
            assertNull(target.applied());assertTrue(target.pendingInstallation());
            source.freeze();source.seed();var sealed=NativeRangeImage.writeMetadata(metaImage,source.parent,nativeTest.limits);cfg=source.parent.configuration();
            var metadata=new NativeMetadataRecoveryRegistration(new UUID(0,1261),current.catalogue(),new ApplyPosition(1,20),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
            assertSame(target,pool.openMetadataRecovery(metadata));assertEquals(image.position(),target.applied());assertFalse(target.pendingInstallation());assertFalse(target.metadataRecoveryImageInstalled());
            target.installMetadataSnapshot(metaImage,nativeTest.limits);target.completeMetadataRecovery(metadata,target.applied(),target.configuration());assertFalse(target.metadataRecoveryPending());
        }
    }

    @Test void bornCurrentWithInterruptedLaterCheckpointColdReopensMetadataBeforeItsImageArrives()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();var images=currentHelper.helper;
        Path path=root.resolve("born-superseded"),initial=root.resolve("born-initial"),later=root.resolve("born-later"),meta=root.resolve("born-meta");
        NativeMetadataRecoveryRegistration metadata;
        try(var source=images.new Fixture(root.resolve("born-source"),images.first,"born-source-");var pool=helper.pool(path)) {
            source.cdc();source.child.enableSessionControls();source.child.apply(new ApplyPosition(1,2),source.write(9));
            var image=NativeRangeImage.write(initial,source.child,images.limits);
            var current=currentHelper.registration(source.child,source.definition.targetDirectory(),image,images.first,images.first,"born-target-");
            var target=pool.openCurrentRangeRecovery(current);target.enableSessionControls();target.enableCdc(current.range().rangeId());target.installSnapshot(initial,images.limits);target.completeCurrentRangeRecovery(current,image.position(),target.configuration());
            source.child.applyNoop(new ApplyPosition(1,3));target.applyNoop(new ApplyPosition(1,3));NativeRangeImage.write(later,target,images.limits);target.fence();
            assertEquals("Interrupt born CURRENT checkpoint",assertThrows(IOException.class,()->target.installSnapshot(later,images.limits,batch->{throw new IOException("Interrupt born CURRENT checkpoint");})).getMessage());
            assertNull(target.applied());assertTrue(target.pendingInstallation());assertNotNull(target.topologyActivation());
            var directory=source.definition.targetDirectory();var merged=new RangeDescriptor(new UUID(0,1270),new UUID(0,1271),2,directory.ranges().getFirst().configurationVersion(),null,null,RangeDescriptor.TopologyState.ACTIVE,directory.ranges().getFirst().replicas());
            var definition=new TopologyChange.Definition(new UUID(0,1272),directory,helper.topology.directory(helper.outer.schema(),directory.generation()+1,List.of(merged)));
            var authority=TopologyChange.State.begin(definition.header()).beginFencing();NativeTopologyFreeze freeze=null;
            for(var child:source.children){var f=child.applyTopologyFreeze(new ApplyPosition(1,child.applied().index()+1),definition,images.admission(definition));authority=authority.frozen(f.proof(images.first.replicaId())).frozen(f.proof(images.second.replicaId()));if(child==source.child)freeze=f;}
            source.pool.seedTopology(definition,authority,source.children,images.first);
            var sealed=NativeRangeImage.writeMetadata(meta,source.child,images.limits);var cfg=source.child.configuration();
            metadata=new NativeMetadataRecoveryRegistration(new UUID(0,1273),current.catalogue(),new ApplyPosition(1,50),directory,freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
            assertSame(target,pool.openMetadataRecovery(metadata));assertNull(target.applied());assertFalse(target.pendingInstallation());assertTrue(target.userHandlesDetached());assertFalse(target.metadataRecoveryImageInstalled());
        }
        try(var pool=helper.pool(path)) {
            var target=pool.openMetadataRecovery(metadata);assertNull(target.applied());assertFalse(target.pendingInstallation());assertFalse(target.metadataRecoveryImageInstalled());assertTrue(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
            target.enableSessionControls();target.enableCdc(metadata.range().rangeId());target.installMetadataSnapshot(meta,images.limits);target.completeMetadataRecovery(metadata,target.applied(),target.configuration());assertFalse(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
        }
    }

    @Test void supersessionWaitsForActualInstallerAndExactForceRetryPreservesNewMetadataInstall()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();Path path=root.resolve("drained-receiver"),group=root.resolve("drained-group"),meta=root.resolve("drained-meta");
        NativeMetadataRecoveryRegistration metadata;
        try(var source=nativeTest.new Fixture(root.resolve("drained-source"));var pool=helper.pool(path);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));var image=NativeRangeImage.write(group,source.parent,nativeTest.limits);var cfg=source.parent.configuration();var definition=source.definition;
            var current=new NativeCurrentRangeRecovery(new UUID(0,1280),nativeTest.catalogue,new ApplyPosition(1,10),definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"drained-current-"),Set.of(),currentHelper.schemaRows(source.parent));
            var target=pool.openCurrentRangeRecovery(current);configure(target,definition);
            source.freeze();source.seed();var sealed=NativeRangeImage.writeMetadata(meta,source.parent,nativeTest.limits);cfg=source.parent.configuration();
            metadata=new NativeMetadataRecoveryRegistration(new UUID(0,1281),current.catalogue(),new ApplyPosition(1,20),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var installer=workers.submit(()->target.installSnapshot(group,nativeTest.limits,batch->{entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Installer barrier timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}throw new IOException("Interrupted original install");}));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IllegalStateException.class,()->pool.openMetadataRecovery(metadata));
                assertTrue(pool.metadataRecoveries().isEmpty());assertEquals(current,target.currentRangeRecoveryRegistration());assertTrue(target.pendingInstallation());
            }finally{release.countDown();}
            assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->installer.get(5,TimeUnit.SECONDS)).getCause());
            pool.metadataRecoveryForceObserver(after->{if(after)throw new RocksDBException("Lost supersession force acknowledgement");});
            assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(metadata));assertTrue(target.metadataRecoveryRegistrationForced(metadata));
            assertThrows(IOException.class,()->target.installMetadataSnapshot(meta,nativeTest.limits,batch->{throw new IOException("Interrupted new META install");}));assertTrue(target.pendingInstallation());
            byte[] key=java.nio.ByteBuffer.allocate(17).putLong(metadata.groupId().getMostSignificantBits()).putLong(metadata.groupId().getLeastSignificantBits()).put((byte)0x7f).array();
            byte[] pending=helper.db(pool).get(helper.handles(pool).get(DistributedStoragePool.STATE_COLUMN),key);assertNotNull(pending);
            pool.metadataRecoveryForceObserver(after->{});assertSame(target,pool.openMetadataRecovery(metadata));
            assertTrue(target.pendingInstallation());assertArrayEquals(pending,helper.db(pool).get(helper.handles(pool).get(DistributedStoragePool.STATE_COLUMN),key));assertNull(target.applied());
        }
        try(var pool=helper.pool(path)) {
            var target=pool.openMetadataRecovery(metadata);assertTrue(target.pendingInstallation());assertNull(target.applied());assertFalse(target.admissionOpen());
            target.enableSessionControls();target.enableCdc(metadata.range().rangeId());target.installMetadataSnapshot(meta,nativeTest.limits);target.completeMetadataRecovery(metadata,target.applied(),target.configuration());assertFalse(target.metadataRecoveryPending());assertTrue(pool.currentRangeRecoveries().isEmpty());
        }
    }

    @Test void supersessionRejectsMetadataBehindPendingOrFinallyForcedOwnCheckpoint()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();
        for(boolean forced:List.of(false,true))try(var source=nativeTest.new Fixture(root.resolve("floor-source-"+forced));var pool=helper.pool(root.resolve("floor-receiver-"+forced))) {
            Path group=root.resolve("floor-group-"+forced),own=root.resolve("floor-own-"+forced),meta=root.resolve("floor-meta-"+forced),latest=root.resolve("floor-latest-"+forced);
            source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));var image=NativeRangeImage.write(group,source.parent,nativeTest.limits);var cfg=source.parent.configuration();var definition=source.definition;
            var current=new NativeCurrentRangeRecovery(new UUID(0,1290),nativeTest.catalogue,new ApplyPosition(1,10),definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"floor-current-"),Set.of(),currentHelper.schemaRows(source.parent));
            var target=pool.openCurrentRangeRecovery(current);configure(target,definition);target.installSnapshot(group,nativeTest.limits);target.completeCurrentRangeRecovery(current,image.position(),target.configuration());
            source.freeze();source.seed();var sealed=NativeRangeImage.writeMetadata(meta,source.parent,nativeTest.limits);cfg=source.parent.configuration();
            var stale=new NativeMetadataRecoveryRegistration(new UUID(0,1291),current.catalogue(),new ApplyPosition(1,20),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
            target.applyTopologyFreeze(new ApplyPosition(1,2),definition,nativeTest.admission(definition));target.applyNoop(new ApplyPosition(1,3));NativeRangeImage.write(own,target,nativeTest.limits);target.fence();
            assertThrows(IOException.class,()->target.installSnapshot(own,nativeTest.limits,new ReplicatedRangeStore.InstallationObserver(){
                public void afterBatch(int count)throws IOException{if(!forced)throw new IOException("Interrupted own checkpoint");}
                public void afterCompletionForce()throws IOException{throw new IOException("Lost own checkpoint completion acknowledgement");}
            }));
            assertNull(target.applied());assertTrue(target.pendingInstallation());assertThrows(IllegalArgumentException.class,()->pool.openMetadataRecovery(stale));
            assertEquals(current,target.currentRangeRecoveryRegistration());assertTrue(pool.metadataRecoveries().isEmpty());assertTrue(target.pendingInstallation());
            source.parent.applyNoop(new ApplyPosition(1,3));var nextImage=NativeRangeImage.writeMetadata(latest,source.parent,nativeTest.limits);cfg=source.parent.configuration();
            var next=new NativeMetadataRecoveryRegistration(new UUID(0,1292),current.catalogue(),new ApplyPosition(1,21),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),nextImage.position(),nextImage.sha256(),Set.of());
            assertSame(target,pool.openMetadataRecovery(next));assertEquals(forced?new ApplyPosition(1,3):null,target.applied());assertFalse(target.pendingInstallation());
            target.installMetadataSnapshot(latest,nativeTest.limits);target.completeMetadataRecovery(next,target.applied(),target.configuration());assertFalse(target.metadataRecoveryPending());
        }
    }

    @Test void currentToMetadataUnknownForceColdReopensExactlyOneDurableRoleBeforeRetry()throws Exception {
        var currentHelper=new NativeCurrentRangeRecoveryTest();
        for(boolean after:List.of(false,true)) {
            Path path=root.resolve("cold-role-"+after),groupImage=root.resolve("cold-role-group-"+after),metaImage=root.resolve("cold-role-meta-"+after);
            NativeCurrentRangeRecovery current;NativeMetadataRecoveryRegistration metadata;TopologyChange.Definition definition;
            try(var source=nativeTest.new Fixture(root.resolve("cold-role-source-"+after));var pool=helper.pool(path)) {
                source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));definition=source.definition;
                var image=NativeRangeImage.write(groupImage,source.parent,nativeTest.limits);var cfg=source.parent.configuration();
                current=new NativeCurrentRangeRecovery(new UUID(0,1210),nativeTest.catalogue,new ApplyPosition(1,10),definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"cold-role-"),Set.of(),currentHelper.schemaRows(source.parent));
                var target=pool.openCurrentRangeRecovery(current);configure(target,definition);target.installSnapshot(groupImage,nativeTest.limits);target.completeCurrentRangeRecovery(current,image.position(),target.configuration());
                source.freeze();source.seed();var sealed=NativeRangeImage.writeMetadata(metaImage,source.parent,nativeTest.limits);cfg=source.parent.configuration();
                metadata=new NativeMetadataRecoveryRegistration(new UUID(0,1211),current.catalogue(),new ApplyPosition(1,20),definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),sealed.position(),sealed.sha256(),Set.of());
                pool.metadataRecoveryForceObserver(boundary->{if(boundary==after)throw new RocksDBException("Close immediately after uncertain role force");});
                assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(metadata));assertNull(target.currentRangeRecoveryRegistration());assertTrue(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
                // No warm registration retry, image installation, or completion precedes this close.
            }
            try(var pool=helper.pool(path)) {
                var db=helper.db(pool);var state=helper.handles(pool).get(DistributedStoragePool.STATE_COLUMN);
                for(int tag:List.of(0x72,0x73,0x74,0x6f,0x70,0x71)) {
                    byte[] key=java.nio.ByteBuffer.allocate(17).putLong(current.groupId().getMostSignificantBits()).putLong(current.groupId().getLeastSignificantBits()).put((byte)tag).array();
                    boolean expected=after ? tag==0x6f : tag==0x72 || tag==0x73 || tag==0x74;
                    assertEquals(expected,db.get(state,key)!=null,"Atomic durable role at tag "+Integer.toHexString(tag));
                }
                ReplicatedRangeStore target;
                if(after) {
                    assertTrue(pool.currentRangeRecoveries().isEmpty());assertEquals(List.of(metadata),pool.metadataRecoveries());
                    assertThrows(IllegalArgumentException.class,()->pool.openCurrentRangeRecovery(current));
                    target=pool.openMetadataRecovery(metadata);configure(target,definition);assertNull(target.currentRangeRecoveryRegistration());assertTrue(target.metadataRecoveryPending());assertFalse(target.metadataRecoveryImageInstalled());assertTrue(target.userHandlesDetached());assertFalse(target.admissionOpen());assertEquals(current.imageCut(),target.applied());
                } else {
                    assertEquals(List.of(current),pool.currentRangeRecoveries());assertTrue(pool.metadataRecoveries().isEmpty());
                    target=pool.openCurrentRangeRecovery(current);configure(target,definition);assertTrue(target.currentRangeRecoveryImageInstalled());assertTrue(target.currentRangeRecoveryReady());assertTrue(target.admissionOpen());assertEquals(current.imageCut(),target.applied());assertArrayEquals(helper.helper.key(9),target.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));
                    assertSame(target,pool.openMetadataRecovery(metadata));
                }
                target.installMetadataSnapshot(metaImage,nativeTest.limits);var tail=new ApplyPosition(1,metadata.imageCut().index()+1);target.applyNoop(tail,new byte[]{2});target.completeMetadataRecovery(metadata,tail,target.configuration());assertTrue(pool.currentRangeRecoveries().isEmpty());assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
            }
            try(var pool=helper.pool(path)) {
                assertTrue(pool.currentRangeRecoveries().isEmpty());assertEquals(List.of(metadata),pool.metadataRecoveries());var target=pool.openMetadataRecovery(metadata);assertNull(target.currentRangeRecoveryRegistration());assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertEquals(new ApplyPosition(1,metadata.imageCut().index()+1),target.applied());assertFalse(target.admissionOpen());
            }
        }
    }

    @Test void uncertainRegistrationRequiresExactRetryAndColdRegistryDoesNotInventReadiness()throws Exception {
        Path image=root.resolve("metadata");NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));source.freeze();source.seed();definition=source.definition;registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));}
        for(boolean after:List.of(false,true)) {
            Path path=root.resolve("receiver-"+after);
            try(var pool=helper.pool(path)) {
                pool.metadataRecoveryForceObserver(boundary->{if(boundary==after)throw new RocksDBException("Uncertain registration force");});
                assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(registration));assertTrue(pool.metadataRecoveries().isEmpty());
                var other=new NativeMetadataRecoveryRegistration(new UUID(0,1102),registration.catalogue(),registration.catalogueCut(),registration.sourceDirectory(),registration.freeze(),registration.receiver(),registration.sourceOwner(),registration.configurationTerm(),registration.configurationIndex(),registration.sourceConfiguration(),registration.imageCut(),registration.imageHash(),registration.mergeColumns());
                assertThrows(IllegalStateException.class,()->pool.openMetadataRecovery(other));assertThrows(IllegalStateException.class,()->helper.open(pool,definition.parents().getFirst()));
                pool.metadataRecoveryForceObserver(boundary->{});var target=pool.openMetadataRecovery(registration);assertSame(target,pool.openMetadataRecovery(registration));assertTrue(target.metadataRecoveryPending());
            }
            try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::retiredOutcomeState);target.installMetadataSnapshot(image,nativeTest.limits);target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());assertFalse(target.metadataRecoveryPending());}
        }
    }
    @Test void unknownSuccessorForceRetriesDurabilityWithoutResettingInstalledImageOrOwnTail()throws Exception {
        for(boolean after:List.of(false,true)) {
            Path path=root.resolve("progressed-receiver-"+after),first=root.resolve("first-"+after),latest=root.resolve("latest-"+after);
            NativeMetadataRecoveryRegistration next;ApplyPosition tail;
            try(var source=nativeTest.new Fixture(root.resolve("progressed-source-"+after));var pool=helper.pool(path)) {
                source.freeze();source.seed();var original=registration(source,NativeRangeImage.writeMetadata(first,source.parent,nativeTest.limits));
                var target=pool.openMetadataRecovery(original);configure(target,source.definition);target.installMetadataSnapshot(first,nativeTest.limits);target.completeMetadataRecovery(original,original.imageCut(),target.configuration());
                source.parent.applyNoop(new ApplyPosition(1,source.parent.applied().index()+1));var image=NativeRangeImage.writeMetadata(latest,source.parent,nativeTest.limits);var cfg=source.parent.configuration();
                var changed=new NativeMetadataRecoveryRegistration(original.transferId(),original.catalogue(),new ApplyPosition(1,21),original.sourceDirectory(),original.freeze(),original.receiver(),original.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of());
                assertThrows(IllegalArgumentException.class,()->pool.openMetadataRecovery(changed),"An attempt identity cannot acquire another image recipe");
                next=new NativeMetadataRecoveryRegistration(new UUID(0,1104),original.catalogue(),new ApplyPosition(1,21),original.sourceDirectory(),original.freeze(),original.receiver(),original.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of());
                pool.metadataRecoveryForceObserver(boundary->{if(boundary==after)throw new RocksDBException("Lost successor registration force acknowledgement");});
                assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(next));assertEquals(List.of(original),pool.metadataRecoveries());assertTrue(target.metadataRecoveryPending());
                if(after){assertTrue(target.metadataRecoveryRegistrationForced(next));target.installMetadataSnapshot(latest,nativeTest.limits);tail=new ApplyPosition(1,image.position().index()+1);target.applyNoop(tail,new byte[]{2});target.completeMetadataRecovery(next,tail,target.configuration());}
                else {assertFalse(target.metadataRecoveryRegistrationForced(next));tail=new ApplyPosition(1,image.position().index()+1);}
                var barriers=new java.util.concurrent.atomic.AtomicInteger();pool.metadataRecoveryForceObserver(boundary->{if(boundary)barriers.incrementAndGet();});
                assertSame(target,pool.openMetadataRecovery(next));assertEquals(1,barriers.get(),"Retry must establish a sync barrier even when the registration is readable");assertEquals(List.of(next),pool.metadataRecoveries());
                if(!after){assertFalse(target.metadataRecoveryImageInstalled());target.installMetadataSnapshot(latest,nativeTest.limits);target.applyNoop(tail,new byte[]{2});target.completeMetadataRecovery(next,tail,target.configuration());}
                assertEquals(tail,target.applied());assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertArrayEquals(new byte[]{2},target.configuration().data());assertTrue(target.recoveryCompletionRecorded(tail,target.configuration()));assertTrue(target.userHandlesDetached());assertFalse(target.admissionOpen());
            }
            try(var pool=helper.pool(path)){var target=pool.openMetadataRecovery(next);assertEquals(tail,target.applied());assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertArrayEquals(new byte[]{2},target.configuration().data());assertTrue(target.recoveryCompletionRecorded(tail,target.configuration()));assertTrue(target.userHandlesDetached());assertFalse(target.admissionOpen());}
        }
    }

    @Test void sourceCreditPublicationAndAckTailStayAuthoritativeAcrossNewMetadataImage()throws Exception {
        var promises=new NativeSourceOutcomePublicationTest();Path first=root.resolve("credit-image"),latest=root.resolve("ack-image"),path=root.resolve("receiver");NativeMetadataRecoveryRegistration next;NativeSourceOutcomeCredit.Request request;
        try(var source=nativeTest.new Fixture(root.resolve("source"));var home=promises.home.new Fixture(root.resolve("home"));var pool=helper.pool(path)) {
            source.freeze();source.parent.configureSourceOutcomeCapacity(4,352);request=promises.reserve(source,home,1,1);var credit=source.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);source.seed();
            var original=registration(source,NativeRangeImage.writeMetadata(first,source.parent,nativeTest.limits));var target=pool.openMetadataRecovery(original);configure(target,source.definition);target.installMetadataSnapshot(first,nativeTest.limits);target.configureSourceOutcomeCapacity(4,352);
            assertThrows(IllegalStateException.class,()->target.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));
            var replay=target.readCommittedSourceOutcome(request);assertEquals(credit,replay.credit());assertNull(replay.publication());assertFalse(replay.fenced());
            target.completeMetadataRecovery(original,original.imageCut(),target.configuration());assertEquals(credit,target.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));
            byte[] result=promises.success();var publication=source.parent.publishSourceOutcome(new ApplyPosition(1,3),request,result);var ack=new DistributedCommand(request.original().groupId(),1,source.parent.schemaDigest(),new OperationId(request.original().operationId().sessionId(),1,2),List.of(),List.of(),1);source.parent.apply(new ApplyPosition(1,4),ack);
            var saved=NativeRangeImage.writeMetadata(latest,source.parent,nativeTest.limits);var cfg=source.parent.configuration();next=new NativeMetadataRecoveryRegistration(new UUID(0,1103),original.catalogue(),new ApplyPosition(1,21),source.definition.sourceDirectory(),source.freeze,original.receiver(),original.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),saved.position(),saved.sha256(),Set.of());
            assertSame(target,pool.openMetadataRecovery(next));assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::sourceOutcomeCredits);
            // The current source legitimately consumed the credit and fenced the published result.
            // Replacing metadata must carry that state forward instead of requiring an obsolete credit row.
            target.installMetadataSnapshot(latest,nativeTest.limits);target.completeMetadataRecovery(next,next.imageCut(),target.configuration());assertTrue(target.sourceOutcomeCredits().isEmpty());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(target.operationResult(request.original().operationId(),request.original().receiptDigest())).status());
            assertEquals(source.parent.sourceOutcomePublication(request,result),target.sourceOutcomePublication(request,result));assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,publication.outcome());
        }
        try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(next);target.configureSourceOutcomeCapacity(4,352);assertFalse(target.metadataRecoveryPending());assertTrue(target.sourceOutcomeCredits().isEmpty());assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(target.operationResult(request.original().operationId(),request.original().receiptDigest())).status());}
    }

    @Test void committedCreditAndPublicationTailReplayDoesNotOpenFreshAuthority()throws Exception {
        var promises=new NativeSourceOutcomePublicationTest();Path image=root.resolve("metadata");
        try(var source=nativeTest.new Fixture(root.resolve("source"));var home=promises.home.new Fixture(root.resolve("home"));var pool=helper.pool(root.resolve("receiver"))) {
            source.freeze();source.seed();var registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));
            source.parent.configureSourceOutcomeCapacity(4,352);var request=promises.reserve(source,home,1,1);var credit=source.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);byte[] result=promises.success();var publication=source.parent.publishSourceOutcome(new ApplyPosition(1,3),request,result);
            var target=pool.openMetadataRecovery(registration);configure(target,source.definition);target.installMetadataSnapshot(image,nativeTest.limits);target.configureSourceOutcomeCapacity(4,352);
            assertNull(target.readCommittedSourceOutcome(request).credit());assertEquals(credit,target.applySourceOutcomeCredit(new ApplyPosition(1,2),request));assertEquals(credit,target.applySourceOutcomeCredit(new ApplyPosition(1,2),request));
            assertEquals(credit,target.readCommittedSourceOutcome(request).credit());assertThrows(IllegalStateException.class,()->target.estimateSourceOutcomeCredit(request));
            assertEquals(publication,target.publishSourceOutcome(new ApplyPosition(1,3),request,result));var replay=target.readCommittedSourceOutcome(request);assertNull(replay.credit());assertEquals(publication.publication(),replay.publication());assertArrayEquals(result,replay.result());
            assertThrows(IllegalStateException.class,()->target.sourceOutcomePublication(request,result));assertThrows(IllegalStateException.class,()->target.operationResult(request.original().operationId(),request.original().receiptDigest()));
            target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration());assertArrayEquals(result,target.operationResult(request.original().operationId(),request.original().receiptDigest()));
        }
    }

}
