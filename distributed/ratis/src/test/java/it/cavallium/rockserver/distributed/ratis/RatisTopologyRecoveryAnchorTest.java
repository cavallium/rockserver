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
                    Files.move(original,root.resolve("known-"+name+"-backup"));assertThrows(IOException.class,()->anchors.capture(f.definition,source,freeze,f.local,authority.cut()));assertArrayEquals(partial,Files.readAllBytes(pending));assertFalse(Files.exists(original));assertArrayEquals(saved.image().sha256(),NativeRangeImage.inspect(saved.file(),limits).sha256());
                }
            }
        }
    }
    @Test void frozenGroupAnchorSurvivesSplitMergeAndColdReplayNeverServesIntermediateChildren()throws Exception {
        helper.root=root;
        try(var f=helper.new Fixture(root.resolve("source"),false,false)) {
            var schema=RatisTopologyControllerTest.logicalSchema();var split=f.definition;var source=split.parents().getFirst();
            var original=new DistributedCommand(source.groupId(),source.rangeEpoch(),schema.digest(),new OperationId(RatisTopologyControllerTest.id(91),0,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{1},new byte[]{42})));
            byte[] result=f.store(source.groupId()).operationResult(original.operationId(),original.receiptDigest());
            Path journal=Files.createDirectory(root.resolve("journal"));
            try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(split),journal,RatisTopologyController.Phase.RETIRED);}
            var anchor=f.node.topologyRecoveryAnchor(source.groupId());
            assertTrue(Files.isRegularFile(anchor.file()));assertTrue(anchor.image().position().index()>=anchor.freeze().position().index());
            var firstSeed=f.node.nativePool(f.node.poolId(source.groupId())).topologySeed(split.intentId());
            var merged=new RangeDescriptor(RatisTopologyControllerTest.id(201),RatisTopologyControllerTest.id(202),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(f.local));
            var sourceDirectory=split.targetDirectory();var target=new RangeDirectory(sourceDirectory.clusterId(),sourceDirectory.databaseId(),sourceDirectory.domainId(),sourceDirectory.schemaEpoch(),sourceDirectory.schemaDigest(),sourceDirectory.generation()+1,sourceDirectory.columns(),sourceDirectory.routingPrefixBytes(),List.of(merged));
            var merge=new TopologyChange.Definition(RatisTopologyControllerTest.id(203),sourceDirectory,target);f.definition=merge;
            try(var driver=f.controller(journal)){helper.until(driver,driver.prepare(merge),journal,RatisTopologyController.Phase.RETIRED);}
            var secondSeed=f.node.nativePool(f.node.poolId(merged.groupId())).topologySeed(merge.intentId());
            var mergeAnchors=new ArrayList<RatisTopologyRecoveryAnchors.Saved>();for(var parent:merge.parents())mergeAnchors.add(f.node.topologyRecoveryAnchor(parent.groupId()));
            assertArrayEquals(anchor.image().sha256(),NativeRangeImage.inspect(anchor.file(),limits).sha256());
            // A receiver absent during both source transitions starts with only the retained frozen GROUP image.
            Path receiver=root.resolve("receiver");
            try(var cold=pool(receiver)) {
                var parent=cold.openTopologyRecoveryRange(anchor.fence(),anchor.image().interval(),schema.digest(),firstSeed.physicalNames(),firstSeed.mergeColumns());
                assertTrue(parent.topologyRecoveryOnly());assertEquals(RangeOwnership.State.FENCED,parent.ownership().state());
                parent.installSnapshot(anchor.file(),limits);assertTrue(parent.topologyRecoveryOnly());assertEquals(RangeOwnership.State.FENCED,parent.ownership().state());
                assertArrayEquals(result,parent.operationResult(original.operationId(),original.receiptDigest()));
                var seeded=cold.seedTopologyRecovery(split,firstSeed.authority(),List.of(parent),f.local,anchor.fence());
                for(var range:split.targets()) {
                    var child=cold.reopenTopologyRange(seeded.birth(range.groupId()),schema);child.initializeConfiguration(f.store(range.groupId()).configuration().data());activation(child,f.store(range.groupId()).topologyActivation(),split);
                    assertTrue(child.topologyRecoveryOnly());assertFalse(child.admissionOpen());
                    assertThrows(IllegalStateException.class,()->child.validate(new DistributedCommand(range.groupId(),range.rangeEpoch(),schema.digest(),new OperationId(RatisTopologyControllerTest.id(204),0,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{1},new byte[]{77})))));
                    assertThrows(IllegalStateException.class,()->child.captureReadable(new TransactionScope(List.of(TransactionScope.Span.range(RatisTopologyControllerTest.COLUMN,range.lowerInclusive(),range.upperExclusive())))));
                    assertThrows(IllegalStateException.class,()->NativeRangeImage.write(root.resolve("forbidden-"+range.groupId()),child,limits));
                }
            }
            try(var cold=pool(receiver)) {
                var parents=new ArrayList<ReplicatedRangeStore>();
                for(var range:merge.parents()) {
                    var child=cold.reopenTopologyRange(firstSeed.birth(range.groupId()),schema);assertTrue(child.topologyRecoveryOnly());assertFalse(child.admissionOpen());
                    var freeze=f.store(range.groupId()).topologyFreeze();child.applyTopologyFreeze(freeze.position(),merge,freeze.admissionDigest());parents.add(child);
                }
                byte[] hashes=new byte[64];for(int i=0;i<mergeAnchors.size();i++)System.arraycopy(mergeAnchors.get(i).image().sha256(),0,hashes,i*32,32);
                var fence=new NativeTopologyRecoveryFence(merge.header(),merge.parents().getFirst().groupId(),TopologyChange.Owner.of(f.local),mergeAnchors.getFirst().fence().catalogueCut(),RangeImportAdmission.sha256(hashes));
                var seeded=cold.seedTopologyRecovery(merge,secondSeed.authority(),parents,f.local,fence);var leaf=cold.reopenTopologyRange(seeded.birth(merged.groupId()),schema);leaf.initializeConfiguration(f.store(merged.groupId()).configuration().data());activation(leaf,f.store(merged.groupId()).topologyActivation(),merge);
                assertTrue(leaf.topologyRecoveryOnly());assertFalse(leaf.admissionOpen());
                var write=new DistributedCommand(merged.groupId(),merged.rangeEpoch(),schema.digest(),new OperationId(RatisTopologyControllerTest.id(205),0,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{1},new byte[]{77})));
                leaf.apply(new ApplyPosition(1,leaf.applied().index()+1),write); // Trusted committed tail remains executable.
                var current=f.authority(merge.header(),TopologyChange.Stage.ACTIVATE);assertEquals(TopologyChange.Phase.RETIRED,current.state().phase());
                var localFence=leaf.topologyRecoveryFence();assertThrows(IllegalArgumentException.class,()->leaf.completeTopologyRecovery(localFence,split,leaf.applied(),leaf.configuration()));
                leaf.completeTopologyRecovery(localFence,current.definition(),leaf.applied(),leaf.configuration());assertTrue(leaf.admissionOpen());assertArrayEquals(new byte[]{77},leaf.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));
                leaf.completeTopologyRecovery(localFence,current.definition(),leaf.applied(),leaf.configuration());
            }
            try(var cold=pool(receiver)){var leaf=cold.reopenTopologyRange(secondSeed.birth(merged.groupId()),schema);assertFalse(leaf.topologyRecoveryOnly());assertTrue(leaf.admissionOpen());assertArrayEquals(new byte[]{77},leaf.readCurrentPoint(RatisTopologyControllerTest.COLUMN,new byte[]{1}));}
        }
    }
}
