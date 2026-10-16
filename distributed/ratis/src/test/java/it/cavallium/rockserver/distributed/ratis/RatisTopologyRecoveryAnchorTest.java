package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Real source images and cold receiver JNI replay; controlled transport is not a remote Raft quorum. */
class RatisTopologyRecoveryAnchorTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisTopologyControllerTest helper=new RatisTopologyControllerTest();
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,8);
    DistributedStoragePool pool(Path path)throws Exception{return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));}
    void activation(ReplicatedRangeStore child,NativeTopologyActivation source,TopologyChange.Definition definition)throws Exception {
        child.applyNoop(new ApplyPosition(1,0));child.applyTopologyActivation(source.position(),definition,source.commonFreezeDigest(),source.catalogueGroup(),source.catalogueCut(),source.publicationCommitment());
    }
    RatisTopologySourceSnapshotArtifacts artifacts(Path path)throws IOException{return new RatisTopologySourceSnapshotArtifacts(path,limits,4*limits.maxImageBytes(),8L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES,2);}
    long bytes(Path directory)throws IOException {try(var files=Files.list(directory)){return files.mapToLong(file->{try{return Files.size(file);}catch(IOException failure){throw new java.io.UncheckedIOException(failure);}}).sum();}}
    @Test void orphanScratchCannotStartThirdImageAndKnownIdleCleanupAllowsExactColdRetry()throws Exception {
        helper.root=root;try(var f=helper.new Fixture(root.resolve("scratch-source"),false,false)) {
            var journal=Files.createDirectory(root.resolve("scratch-journal"));try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(f.definition),journal,RatisTopologyController.Phase.SEED);}
            var source=f.store(f.definition.parents().getFirst().groupId());var freeze=source.topologyFreeze();var authority=f.authority(f.definition.header(),TopologyChange.Stage.SEED);var owned=Files.createDirectory(root.resolve("scratch-artifacts"));
            Path directory,first,second;byte[] originalCapture,expectedHash;
            try(var ledger=artifacts(owned)) {
                var anchors=new RatisTopologyRecoveryAnchors(ledger,limits);var saved=anchors.capture(f.definition,source,freeze,f.local,authority.cut());directory=saved.file().getParent();expectedHash=saved.image().sha256();originalCapture=Files.readAllBytes(directory.resolve("capture"));
                // Controlled idle fixture state: original capture retained, but no image publication survived.
                Files.delete(directory.resolve("anchor"));Files.delete(saved.file());first=directory.resolve(".native-range-building-first.tmp");second=directory.resolve(".native-range-building-second.tmp");Files.write(first,new byte[64]);Files.write(second,new byte[64]);SessionJournalFiles.force(directory);
                long before=bytes(directory);assertThrows(IOException.class,()->anchors.capture(f.definition,source,freeze,f.local,authority.cut()));assertEquals(before,bytes(directory));
                try(var files=Files.newDirectoryStream(directory,".native-range-building-*.tmp")){int count=0;for(var ignored:files)count++;assertEquals(2,count);}
                assertArrayEquals(originalCapture,Files.readAllBytes(directory.resolve("capture")));assertFalse(Files.exists(saved.file()));
            }
            // Both writers are proven absent: capture returned and its ledger owner has closed.
            Files.delete(first);Files.delete(second);SessionJournalFiles.force(directory);
            try(var ledger=artifacts(owned)){var saved=new RatisTopologyRecoveryAnchors(ledger,limits).capture(f.definition,source,freeze,f.local,authority.cut());assertArrayEquals(expectedHash,saved.image().sha256());assertArrayEquals(originalCapture,Files.readAllBytes(directory.resolve("capture")));}
        }
    }
    @Test void completeCaptureAndAnchorSidecarsRecoverOldImageAfterNativeCutAdvances()throws Exception {
        helper.root=root;try(var f=helper.new Fixture(root.resolve("sidecar-source"),false,false)) {
            var journal=Files.createDirectory(root.resolve("sidecar-journal"));try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(f.definition),journal,RatisTopologyController.Phase.SEED);}
            var source=f.store(f.definition.parents().getFirst().groupId());var freeze=source.topologyFreeze();var authority=f.authority(f.definition.header(),TopologyChange.Stage.SEED);var owned=Files.createDirectory(root.resolve("sidecar-artifacts"));
            byte[] hash;ApplyPosition cut;Path directory;
            try(var ledger=artifacts(owned)){var saved=new RatisTopologyRecoveryAnchors(ledger,limits).capture(f.definition,source,freeze,f.local,authority.cut());hash=saved.image().sha256();cut=saved.image().position();directory=saved.file().getParent();}
            Files.move(directory.resolve("capture"),directory.resolve("capture.pending"));Files.move(directory.resolve("anchor"),directory.resolve("anchor.pending"));SessionJournalFiles.force(directory);
            source.applyNoop(new ApplyPosition(cut.term(),cut.index()+1));
            try(var ledger=artifacts(owned)){var saved=new RatisTopologyRecoveryAnchors(ledger,limits).capture(f.definition,source,freeze,f.local,authority.cut());assertEquals(cut,saved.image().position());assertArrayEquals(hash,saved.image().sha256());assertFalse(Files.exists(directory.resolve("capture.pending")));assertFalse(Files.exists(directory.resolve("anchor.pending")));}
            // The preserved capture may not be replaced by a newer cut if its image is unavailable.
            Path image=directory.resolve("image"),backup=root.resolve("known-old-image");Files.move(image,backup);Files.delete(directory.resolve("anchor"));Files.move(directory.resolve("capture"),directory.resolve("capture.pending"));SessionJournalFiles.force(directory);
            byte[] pending=Files.readAllBytes(directory.resolve("capture.pending"));
            try(var ledger=artifacts(owned)){assertThrows(IOException.class,()->new RatisTopologyRecoveryAnchors(ledger,limits).capture(f.definition,source,freeze,f.local,authority.cut()));assertArrayEquals(pending,Files.readAllBytes(directory.resolve("capture")));assertFalse(Files.exists(image));}
        }
    }
    @Test void coldInventoryRejectsMetadataBeyondTheSharedReservationWithoutMutation()throws Exception {
        var owned=Files.createDirectory(root.resolve("metadata-artifacts"));Path directory;
        try(var ledger=artifacts(owned)){directory=ledger.reserveTopologyRecovery(new UUID(0,900));}
        for(var name:List.of("capture","capture.pending","anchor"))Files.write(directory.resolve(name),new byte[RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES]);
        long before=bytes(directory);assertThrows(IOException.class,()->artifacts(owned));assertEquals(before,bytes(directory));
    }
    @Test void conflictingAndPartialCaptureOrAnchorSidecarsRemainUnchanged()throws Exception {
        helper.root=root;try(var f=helper.new Fixture(root.resolve("unknown-source"),false,false)) {
            var journal=Files.createDirectory(root.resolve("unknown-journal"));try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(f.definition),journal,RatisTopologyController.Phase.SEED);}
            var source=f.store(f.definition.parents().getFirst().groupId());var freeze=source.topologyFreeze();var authority=f.authority(f.definition.header(),TopologyChange.Stage.SEED);
            for(var name:List.of("capture","anchor")) {
                var owned=Files.createDirectory(root.resolve("unknown-"+name));try(var ledger=artifacts(owned)) {
                    var anchors=new RatisTopologyRecoveryAnchors(ledger,limits);var saved=anchors.capture(f.definition,source,freeze,f.local,authority.cut());var directory=saved.file().getParent();Path original=directory.resolve(name),pending=directory.resolve(name+".pending");byte[] before=Files.readAllBytes(original);
                    var fields=new HashMap<>(RatisSchemaClientJournal.read(original));
                    if(name.equals("capture"))fields.put("cut",ByteBuffer.allocate(16).putLong(saved.image().position().term()).putLong(saved.image().position().index()+1).array());else {var hash=fields.get("hash").clone();hash[0]^=1;fields.put("hash",hash);}
                    RatisSchemaClientJournal.write(pending,fields);byte[] conflict=Files.readAllBytes(pending);
                    assertThrows(IOException.class,()->anchors.capture(f.definition,source,freeze,f.local,authority.cut()));assertArrayEquals(before,Files.readAllBytes(original));assertArrayEquals(conflict,Files.readAllBytes(pending));
                    byte[] partial={1,2,3};Files.write(pending,partial,StandardOpenOption.TRUNCATE_EXISTING);assertThrows(IOException.class,()->anchors.capture(f.definition,source,freeze,f.local,authority.cut()));assertArrayEquals(partial,Files.readAllBytes(pending));assertArrayEquals(before,Files.readAllBytes(original));
                    Files.move(original,root.resolve("known-"+name+"-backup"));assertThrows(IOException.class,()->anchors.capture(f.definition,source,freeze,f.local,authority.cut()));assertArrayEquals(partial,Files.readAllBytes(pending));assertFalse(Files.exists(original));assertArrayEquals(saved.image().sha256(),NativeRangeImage.inspectMetadata(saved.file(),limits).sha256());
                }
            }
        }
    }
    @Test void completedMetadataImageForceCanRetryExactAfterLostObservationAndRejectGroupReplacement()throws Exception {
        helper.root=root;try(var f=helper.new Fixture(root.resolve("force-source"),false,false)){
            var journal=Files.createDirectory(root.resolve("force-journal"));try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(f.definition),journal,RatisTopologyController.Phase.SEED);}
            var source=f.store(f.definition.parents().getFirst().groupId());var freeze=source.topologyFreeze();var authority=f.authority(f.definition.header(),TopologyChange.Stage.SEED);Path owned=Files.createDirectory(root.resolve("force-artifacts"));
            var failed=new java.util.concurrent.atomic.AtomicBoolean();var held=new java.util.concurrent.atomic.AtomicInteger();var ioCalls=new java.util.concurrent.atomic.AtomicInteger();
            var admission=new NativeRangeImage.IoAdmission(){
                public void beforeBytes(int bytes)throws IOException{ioCalls.incrementAndGet();try(var directories=Files.newDirectoryStream(owned,"*.recovery")){for(var directory:directories)if(Files.exists(directory.resolve("image")) && failed.compareAndSet(false,true))throw new IOException("Lost forced metadata image observation");}}
                public AutoCloseable reserveMetadata(int bytes){held.addAndGet(bytes);return ()->held.addAndGet(-bytes);}
            };
            Path image;byte[] hash;ApplyPosition cut;
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,4*limits.maxImageBytes(),8L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES,2,admission)){
                var anchors=new RatisTopologyRecoveryAnchors(ledger,limits);var failure=assertThrows(IOException.class,()->anchors.capture(f.definition,source,freeze,f.local,authority.cut()));assertEquals("Lost forced metadata image observation",failure.getMessage());assertTrue(failed.get(),"Failure must occur after the actual forced image is visible");assertEquals(0,held.get(),"Typed metadata workspace must retire on failure (this fixture may need none)");assertTrue(ioCalls.get()>0,"Actual image bytes pass through admission");
                try(var directories=Files.newDirectoryStream(owned,"*.recovery")){image=directories.iterator().next().resolve("image");}
                var actual=NativeRangeImage.inspectMetadata(image,limits);hash=actual.sha256();cut=actual.position();assertFalse(Files.exists(image.resolveSibling("anchor")));
                source.applyNoop(new ApplyPosition(cut.term(),cut.index()+1));var saved=anchors.capture(f.definition,source,freeze,f.local,authority.cut());assertEquals(cut,saved.image().position());assertArrayEquals(hash,saved.image().sha256());assertEquals(0,held.get());
                var foreign=new ReplicaDescriptor(f.local.nodeId(),f.local.nodeIncarnation(),f.local.replicaId(),f.local.peerId(),f.local.address(),ReplicaDescriptor.Role.LISTENER);assertThrows(IOException.class,()->anchors.read(freeze,foreign));
                var original=Files.readAllBytes(image.resolveSibling("capture"));var fields=new HashMap<>(RatisSchemaClientJournal.read(image.resolveSibling("capture")));var cfg=fields.get("configuration").clone();cfg[cfg.length-1]^=1;fields.put("configuration",cfg);RatisSchemaClientJournal.write(image.resolveSibling("capture.pending"),fields);assertThrows(IOException.class,()->anchors.read(freeze,f.local));assertArrayEquals(original,Files.readAllBytes(image.resolveSibling("capture")));Files.delete(image.resolveSibling("capture.pending"));
                Path group=root.resolve("wrong-group-kind");try(var pin=source.captureForRecovery()){NativeRangeImage.write(group,pin,pin.interval(),limits);}byte[] groupHash=NativeRangeImage.inspect(group,limits).sha256();Files.copy(group,image,StandardCopyOption.REPLACE_EXISTING);
                assertThrows(IOException.class,()->anchors.read(freeze,f.local));assertArrayEquals(groupHash,NativeRangeImage.inspect(image,limits).sha256());assertEquals(0,held.get());
            }
        }
    }
    @Test void frozenMetadataAncestorsAndCurrentLeafGroupSurviveSplitMergeAndColdRecovery()throws Exception {
        helper.root=root;
        try(var f=helper.new Fixture(root.resolve("source"),false,false)) {
            var schema=RatisTopologyControllerTest.logicalSchema();var split=f.definition;var source=split.parents().getFirst();
            Path journal=Files.createDirectory(root.resolve("journal"));
            try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(split),journal,RatisTopologyController.Phase.RETIRED);}
            var anchor=f.node.topologyRecoveryAnchor(source.groupId());assertMetadata(anchor);
            var firstSeed=f.node.nativePool(f.node.poolId(source.groupId())).topologySeed(split.intentId());assertNotNull(firstSeed);
            for(var child:split.targets())assertNotNull(firstSeed.birth(child.groupId()));
            var merged=new RangeDescriptor(RatisTopologyControllerTest.id(201),RatisTopologyControllerTest.id(202),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(f.local));
            var sourceDirectory=split.targetDirectory();var target=new RangeDirectory(sourceDirectory.clusterId(),sourceDirectory.databaseId(),sourceDirectory.domainId(),sourceDirectory.schemaEpoch(),sourceDirectory.schemaDigest(),sourceDirectory.generation()+1,sourceDirectory.columns(),sourceDirectory.routingPrefixBytes(),List.of(merged));
            var merge=new TopologyChange.Definition(RatisTopologyControllerTest.id(203),sourceDirectory,target);f.definition=merge;
            try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(merge),journal,RatisTopologyController.Phase.RETIRED);}
            assertNotNull(f.node.nativePool(f.node.poolId(merged.groupId())).topologySeed(merge.intentId()));
            for(var parent:merge.parents())assertMetadata(f.node.topologyRecoveryAnchor(parent.groupId()));
            assertArrayEquals(anchor.image().sha256(),NativeRangeImage.inspectMetadata(anchor.file(),limits).sha256());
            var leaf=f.store(merged.groupId());assertArrayEquals(new byte[]{42},leaf.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));
            Path groupFile=root.resolve("current-leaf-group");NativeRangeImage.Manifest group;var rows=new TreeMap<String,byte[]>();
            try(var pin=leaf.captureForRecovery()){group=NativeRangeImage.write(groupFile,pin,pin.interval(),limits);pin.visitState((key,value)->{if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value);});}
            var cfg=leaf.configuration();var current=merge.targetDirectory();var authority=f.authority(merge.header(),TopologyChange.Stage.ACTIVATE);
            var registration=new NativeCurrentRangeRecovery(UUID.randomUUID(),f.plan.catalogue().groupId(),authority.cut(),current,schema,merged.groupId(),f.local,f.local,group.position(),group.sha256(),cfg.term(),cfg.index(),cfg.data(),group.topologyOrigin(),Map.of(RatisTopologyControllerTest.COLUMN,"receiver-current"),group.deterministicMergeColumns(),rows);
            var ancestor=new NativeMetadataRecoveryRegistration(UUID.randomUUID(),f.plan.catalogue().groupId(),anchor.catalogueCut(),split.sourceDirectory(),anchor.freeze(),f.local,f.local,anchor.configuration().term(),anchor.configuration().index(),anchor.configuration().data(),anchor.image().position(),anchor.image().sha256(),anchor.image().deterministicMergeColumns(),null);
            Path receiver=root.resolve("receiver");
            try(var cold=pool(receiver)){
                var metadata=cold.openMetadataRecovery(ancestor);metadata.installMetadataSnapshot(anchor.file(),limits);metadata.completeMetadataRecovery(ancestor,anchor.image().position(),anchor.configuration());
                assertEquals(ancestor,metadata.metadataRecoveryRegistration());assertTrue(metadata.metadataRecoveryRegistrationForced(ancestor));assertFalse(metadata.metadataRecoveryPending());assertFalse(metadata.admissionOpen());assertThrows(IllegalStateException.class,()->metadata.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));
                var active=cold.openCurrentRangeRecovery(registration);assertThrows(IOException.class,()->active.installSnapshot(anchor.file(),limits));assertFalse(active.currentRangeRecoveryImageInstalled());
                active.installSnapshot(groupFile,limits);assertFalse(active.admissionOpen());active.completeCurrentRangeRecovery(registration,group.position(),cfg);assertTrue(active.admissionOpen());assertArrayEquals(new byte[]{42},active.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));
            }
            try(var cold=pool(receiver)){
                var metadata=cold.openMetadataRecovery(ancestor);assertEquals(ancestor,metadata.metadataRecoveryRegistration());assertTrue(metadata.metadataRecoveryRegistrationForced(ancestor));assertFalse(metadata.metadataRecoveryPending());assertFalse(metadata.admissionOpen());
                var active=cold.openCurrentRangeRecovery(registration);assertTrue(active.currentRangeRecoveryReady());assertArrayEquals(new byte[]{42},active.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));
            }
        }
    }
    private void assertMetadata(RatisTopologyRecoveryAnchors.Saved saved)throws Exception {
        assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(saved.file(),NativeRangeImage.IoAdmission.UNLIMITED));
        var users=new java.util.concurrent.atomic.AtomicInteger();var actual=NativeRangeImage.visitMetadata(saved.file(),limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.USER)users.incrementAndGet();});
        assertEquals(0,users.get());assertArrayEquals(saved.image().sha256(),actual.sha256());assertThrows(IOException.class,()->NativeRangeImage.inspect(saved.file(),limits));
        saved.freeze().requireParent(actual.owner().groupId(),actual.owner().epoch(),actual.interval().domainId(),actual.schemaDigest(),actual.interval().lowerInclusive(),actual.interval().upperExclusive());
    }
    @Test void metadataPinNeverVisitsUserAndLargeUserVolumeDoesNotConsumeAnchorBudget()throws Exception {
        helper.root=root;try(var f=helper.new Fixture(root.resolve("large-source"),false,false)){
            var parent=f.definition.parents().getFirst();var store=f.store(parent.groupId());byte[] value=new byte[256*1024];Arrays.fill(value,(byte)7);
            var write=new DistributedCommand(parent.groupId(),parent.rangeEpoch(),f.definition.header().schemaDigest(),new OperationId(RatisTopologyControllerTest.id(300),0,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{2},value)));
            long index=store.applied().index()+1;assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,index),write)).status());f.server.machines.get(org.apache.ratis.protocol.RaftGroupId.valueOf(parent.groupId())).notifyTermIndexUpdated(1,index);
            // Advance genuine CAT authority and replay its exact controls. The normal CDC age
            // policy retires the old large event, and minimumEpoch retires its epoch-zero receipt.
            // USER bytes remain; no retained metadata row is deleted by the fixture.
            store.enableSessionControls();var epochs=f.env.catalogue.epochs;var sessionLedger=epochs.read();assertNotNull(sessionLedger);long time=sessionLedger.logicalTimeMillis()+1;
            for(int tick=0;tick<2;tick++){
                var next=sessionLedger.advanceTime(time+tick*24L*60*60*1000);assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(f.env.outer.operation(700+tick),sessionLedger,next)).status());sessionLedger=next;
                var control=epochs.prepareAuthorityAdvance(f.definition.sourceDirectory(),parent.groupId(),new OperationId(RatisTopologyControllerTest.id(301),sessionLedger.minimumEpoch(),tick),-1);
                epochs.sessionControlAdmission(f.definition.sourceDirectory(),parent.groupId()).accept(control);
                long nextIndex=store.applied().index()+1;assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,nextIndex),control)).status());f.server.machines.get(org.apache.ratis.protocol.RaftGroupId.valueOf(parent.groupId())).notifyTermIndexUpdated(1,nextIndex);
            }
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(store.operationResult(write.operationId(),write.receiptDigest())).status());
            var lineage=new DistributedCdcCheckpoint.Lineage(parent.rangeId(),parent.groupId(),parent.rangeEpoch());var retained=store.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,index)),8,1024*1024);assertTrue(retained.records().isEmpty());
            assertArrayEquals(value,store.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{2}));
            var journal=Files.createDirectory(root.resolve("large-journal"));try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(f.definition),journal,RatisTopologyController.Phase.SEED);}
            var bounded=new NativeRangeImage.Limits(limits.maxRowBytes(),64*1024,limits.maxColumns());
            try(var pin=store.captureMetadataForRecovery()){
                assertThrows(IllegalArgumentException.class,()->pin.visitColumn(RatisTopologyControllerTest.COLUMN,null,null,(key,bytes)->fail("Metadata pin cannot create a USER iterator")));
                var image=NativeRangeImage.writeMetadataAdmitted(root.resolve("same-metadata-pin"),pin,pin.interval(),bounded,NativeRangeImage.IoAdmission.UNLIMITED);assertTrue(image.bytes()<value.length);
            }
            var owned=Files.createDirectory(root.resolve("large-artifacts"));try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,bounded,4*bounded.maxImageBytes(),8L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES,2)){
                var authority=f.authority(f.definition.header(),TopologyChange.Stage.SEED);var saved=new RatisTopologyRecoveryAnchors(ledger,bounded).capture(f.definition,store,store.topologyFreeze(),f.local,authority.cut());assertTrue(saved.image().bytes()<bounded.maxImageBytes());assertMetadata(saved);assertEquals(2*bounded.maxImageBytes(),ledger.reservedImageBytes());
            }
            try(var driver=f.controller(journal)){helper.until(driver,driver.reopen(f.definition.intentId()),journal,RatisTopologyController.Phase.RETIRED);}
            var child=f.definition.targets().stream().filter(range->range.lowerInclusive()==null).findFirst().orElseThrow();assertArrayEquals(value,f.store(child.groupId()).readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{2}));
        }
    }
}
