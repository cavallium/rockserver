package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRecoveryObsoleteArtifactsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,8<<20,16);
    RatisTopologySourceSnapshotArtifacts open(Path path)throws IOException{return new RatisTopologySourceSnapshotArtifacts(path,limits,2*limits.maxImageBytes(),128L<<20,1);}
    RatisTopologySourceSnapshotRpc.Request current(RatisTopologySourceSnapshotFixture f,int id){return new RatisTopologySourceSnapshotRpc.Request(new UUID(13210,id),null,new UUID(13210,1),f.donor,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,f.group,f.definition.sourceDirectory());}
    TopologyChange.Definition split(RatisTopologySourceSnapshotFixture f){var source=f.definition.sourceDirectory();var parent=source.ranges().getFirst();var children=List.of(new RangeDescriptor(new UUID(13210,2),new UUID(13210,3),1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,parent.replicas()),new RangeDescriptor(new UUID(13210,4),new UUID(13210,5),1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,parent.replicas()));return new TopologyChange.Definition(new UUID(13210,6),source,new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),children));}
    RatisRecoveryObsoleteRpc.Request obsolete(RatisTopologySourceSnapshotFixture f,RatisTopologySourceSnapshotRpc.Request old){var hash=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,old));var definition=split(f);var scope=new NativeRecoverySupersession.Scope(old.catalogue(),new ApplyPosition(1,20),definition.header(),definition.sourceDirectory(),old.sourceGroup(),old.receiver(),new byte[32],List.of(hash));var fence=new NativeRecoverySupersession(scope,new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,null));return new RatisRecoveryObsoleteRpc.Request(old,scope,fence.digest());}
    void retain(RatisTopologySourceSnapshotArtifacts ledger,RatisTopologySourceSnapshotService.Lease lease,Path managed)throws Exception {Files.createDirectories(managed);Path file=managed.resolve(org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.getSnapshotFileName(lease.snapshot().getTerm(),lease.snapshot().getIndex()));Files.createLink(file,lease.snapshot().getFiles().getFirst().getPath());ledger.retain(lease.prepared(),new SingleFileSnapshotInfo(new FileInfo(file,MD5FileUtil.computeMd5ForFile(file.toFile())),lease.snapshot().getTerm(),lease.snapshot().getIndex()));}
    void assertClosed(RatisTopologySourceSnapshotArtifacts ledger,RatisRecoveryObsoleteRpc.Request obsolete)throws Exception {var old=obsolete.original();assertThrows(IOException.class,()->ledger.reserve(old));assertThrows(IOException.class,()->ledger.lookup(old));assertThrows(IOException.class,()->ledger.isReleased(old),"OBSOLETE cannot claim installed RELEASE");}
    NativeTopologyFreeze sealSource(RatisTopologySourceSnapshotFixture f)throws Exception {var definition=split(f);var freeze=f.store.applyTopologyFreeze(new ApplyPosition(1,f.store.applied().index()+1),definition,new byte[32]);var state=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(f.donor.replicaId()));f.pool.seedTopology(definition,state,List.of(f.store),f.donor);return freeze;}
    void prepareRealMetadata(RatisTopologySourceSnapshotArtifacts ledger,RatisTopologySourceSnapshotFixture f,RatisRecoveryObsoleteRpc.Request obsolete)throws Exception {
        var definition=split(f);var freeze=sealSource(f);
        var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(13210,4000),definition.header(),obsolete.scope().catalogue(),f.donor,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,f.group,definition.sourceDirectory());
        ledger.reserve(request);try(var pin=f.store.captureMetadataForRecovery()){
            ledger.capture(request,pin,f.schema,f.donor);Path image=ledger.originalImagePath(request,pin.position());var manifest=NativeRangeImage.writeMetadataAdmitted(image,pin,pin.interval(),limits,NativeRangeImage.IoAdmission.UNLIMITED);
            var prepared=new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,f.schema,f.donor,pin.configuration(),Map.of(),freeze);var snapshot=new SingleFileSnapshotInfo(new FileInfo(image,MD5FileUtil.computeMd5ForFile(image.toFile())),pin.position().term(),pin.position().index());ledger.retain(prepared,snapshot);assertEquals(manifest.position(),ledger.lookup(request).prepared().image().position());
        }
    }
    @Test void oneSlotCurrentCaptureIsReclaimedBeforeGenuineMetadataPreparation()throws Exception {
        Path owned=Files.createDirectory(root.resolve("artifacts"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,10),NativeRangeImage.IoAdmission.UNLIMITED);var ledger=open(owned)){
            retain(ledger,lease,root.resolve("managed"));var request=obsolete(f,lease.prepared().request());long charge=ledger.reservedImageBytes();assertEquals(2*limits.maxImageBytes(),charge);
            ledger.forceObsolete(request);assertFalse(ledger.isObsolete(request));assertEquals(charge,ledger.reservedImageBytes());assertTrue(f.store.activeSnapshotPins()>0);assertThrows(IOException.class,()->ledger.reserve(current(f,11)));
            lease.close();ledger.obsolete(request);assertTrue(ledger.isObsolete(request));assertEquals(0,ledger.reservedImageBytes());assertClosed(ledger,request);prepareRealMetadata(ledger,f,request);
        }
    }
    @Test void beforeAndAfterForceFailuresRetryExactTombstoneWithoutEarlyReclamation()throws Exception {
        for(boolean after:List.of(false,true)){
            Path owned=Files.createDirectory(root.resolve("force-"+after));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source-"+after));var lease=f.capture(current(f,20),NativeRangeImage.IoAdmission.UNLIMITED)){
                var request=obsolete(f,lease.prepared().request());
                try(var ledger=open(owned)){
                    retain(ledger,lease,root.resolve("managed-"+after));var once=new AtomicBoolean(true);ledger.obsoleteForceObserver(boundary->{if(boundary==after && once.getAndSet(false))throw new IOException("Lost obsolete force acknowledgement");});
                    assertThrows(IOException.class,()->ledger.forceObsolete(request));assertFalse(once.get());assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());assertThrows(IOException.class,()->ledger.reserve(current(f,21)));assertThrows(IOException.class,()->ledger.lookup(request.original()));
                    byte[] changed=request.fenceDigest();changed[0]^=1;assertThrows(IOException.class,()->ledger.forceObsolete(new RatisRecoveryObsoleteRpc.Request(request.original(),request.scope(),changed)));
                }
                lease.close();try(var ledger=open(owned)){assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());ledger.forceObsolete(request);ledger.obsolete(request);assertTrue(ledger.isObsolete(request));assertEquals(0,ledger.reservedImageBytes());assertClosed(ledger,request);}
            }
        }
    }
    @Test void deletionLostAckRetainsDiskCreditUntilColdExactSettlement()throws Exception {
        Path owned=Files.createDirectory(root.resolve("delete"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,30),NativeRangeImage.IoAdmission.UNLIMITED)){
            var request=obsolete(f,lease.prepared().request());try(var ledger=open(owned)){
                retain(ledger,lease,root.resolve("managed"));ledger.forceObsolete(request);lease.close();var once=new AtomicBoolean(true);ledger.removalObserver(path->{Files.delete(path);if(path.getFileName().toString().endsWith(".image")&&once.getAndSet(false))throw new IOException("Lost image deletion acknowledgement");});
                assertThrows(IOException.class,()->ledger.obsolete(request));assertFalse(once.get());assertFalse(ledger.isObsolete(request));assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());assertThrows(IOException.class,()->ledger.reserve(current(f,31)));
            }
            try(var ledger=open(owned)){ledger.obsolete(request);assertEquals(0,ledger.reservedImageBytes());assertTrue(ledger.isObsolete(request));assertClosed(ledger,request);prepareRealMetadata(ledger,f,request);}
        }
    }
    @Test void unknownPrepareTombstoneSurvivesColdAndRejectsResurrectionWithoutInventingImage()throws Exception {
        Path owned=Files.createDirectory(root.resolve("unknown"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"))){var request=obsolete(f,current(f,40));
            try(var ledger=open(owned)){ledger.obsolete(request);assertTrue(ledger.isObsolete(request));assertEquals(0,ledger.reservedImageBytes());assertFalse(Files.exists(owned.resolve(request.original().transferId()+".prepared")));assertFalse(Files.exists(owned.resolve(request.original().transferId()+".publication")));}
            try(var ledger=open(owned)){assertTrue(ledger.isObsolete(request));assertClosed(ledger,request);ledger.obsolete(request);prepareRealMetadata(ledger,f,request);}
        }
    }
    @Test void abandonedSuccessorAndInstalledReleaseRetainTheirDistinctOriginalSemantics()throws Exception {
        Path owned=Files.createDirectory(root.resolve("dispositions"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var ledger=open(owned)){
            var old=current(f,60);ledger.reserve(old);try(var pin=f.store.capture()){ledger.capture(old,pin,f.schema,f.donor);}
            var abandoned=ledger.abandon(old,f.store.configuration(),f.store.applied());var terminal=obsolete(f,old);ledger.obsolete(terminal);
            var retry=ledger.abandoned(old);assertEquals(abandoned.successorId(),retry.successorId());assertEquals(abandoned.cut(),retry.cut());assertArrayEquals(abandoned.configurationDigest(),retry.configurationDigest());
            var repeated=assertThrows(RatisTopologySourceSnapshotRpc.AbandonedException.class,()->ledger.lookup(old));assertEquals(abandoned.successorId(),repeated.receipt().successorId());assertTrue(ledger.isObsolete(terminal));
            try(var lease=f.capture(current(f,61),NativeRangeImage.IoAdmission.UNLIMITED)){
                retain(ledger,lease,root.resolve("released-managed"));lease.close();ledger.released(lease.prepared().request());assertTrue(ledger.isReleased(lease.prepared().request()));
                assertThrows(IOException.class,()->ledger.forceObsolete(obsolete(f,lease.prepared().request())));assertTrue(ledger.isReleased(lease.prepared().request()));
            }
        }
    }
    @Test void tombstoneMetadataAdmissionPrecedesForceAndExactColdRetryUsesTheSameCredit()throws Exception {
        Path owned=Files.createDirectory(root.resolve("metadata-quota"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"))){
            var request=obsolete(f,current(f,70));long metadata=3L*(17L+request.encodedSize()+32)+128;
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),metadata,1)){
                ledger.obsolete(request);assertTrue(ledger.isObsolete(request));assertThrows(IOException.class,()->ledger.forceObsolete(obsolete(f,current(f,71))));
                assertFalse(Files.exists(owned.resolve(current(f,71).transferId()+".obsolete")));ledger.obsolete(request);
            }
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),metadata,1)){assertTrue(ledger.isObsolete(request));ledger.obsolete(request);assertEquals(0,ledger.reservedImageBytes());}
        }
    }
    @Test void installedReleaseCleanupFailureDefinitivelyRejectsNewObsoleteWithoutPoisoningReleaseRetry()throws Exception {
        Path owned=Files.createDirectory(root.resolve("release-cleanup"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,200),NativeRangeImage.IoAdmission.UNLIMITED);var ledger=open(owned)){
            retain(ledger,lease,root.resolve("release-managed"));var request=obsolete(f,lease.prepared().request());lease.close();
            ledger.removalObserver(path->{throw new IOException("Release cleanup unavailable");});assertThrows(IOException.class,()->ledger.released(request.original()));
            assertTrue(Files.exists(owned.resolve(request.original().transferId()+".released")));assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());sealSource(f);
            var forces=new AtomicInteger();ledger.obsoleteForceObserver(after->forces.incrementAndGet());
            assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->ledger.forceObsolete(request));assertEquals(0,forces.get());assertFalse(Files.exists(owned.resolve(request.original().transferId()+".obsolete")));
            ledger.removalObserver(SessionJournalFiles.FILESYSTEM);assertTrue(ledger.isReleased(request.original()));assertEquals(0,ledger.reservedImageBytes());
        }
    }
    @Test void coldCompleteReleaseSidecarPromotesFromExactLocalAuthorityWithoutReceiverRpc()throws Exception {
        Path owned=Files.createDirectory(root.resolve("complete-release-sidecar"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,210),NativeRangeImage.IoAdmission.UNLIMITED)){
            var request=obsolete(f,lease.prepared().request());Path released=owned.resolve(request.original().transferId()+".released"),pending=released.resolveSibling(released.getFileName()+".pending");
            try(var ledger=open(owned)){retain(ledger,lease,root.resolve("complete-managed"));lease.close();ledger.removalObserver(path->{throw new IOException("Keep original release artifacts");});assertThrows(IOException.class,()->ledger.released(request.original()));}
            Files.move(released,pending);SessionJournalFiles.force(pending);SessionJournalFiles.force(owned);byte[] exact=Files.readAllBytes(pending);sealSource(f);
            try(var ledger=open(owned)){
                assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->ledger.forceObsolete(request));assertArrayEquals(exact,Files.readAllBytes(pending));assertFalse(Files.exists(owned.resolve(request.original().transferId()+".obsolete")));
                var old=request.original();var foreign=new RatisTopologySourceSnapshotRpc.Request(old.transferId(),null,new UUID(13210,9998),old.receiver(),old.purpose(),old.sourceGroup(),old.sourceDirectory());
                assertThrows(IOException.class,()->ledger.isReleased(foreign));assertArrayEquals(exact,Files.readAllBytes(pending));assertFalse(Files.exists(released));
                assertTrue(ledger.isReleased(request.original()));assertTrue(Files.exists(released));assertFalse(Files.exists(pending));assertEquals(0,ledger.reservedImageBytes());
            }
        }
    }
    @Test void coldPartialReleaseSidecarIsNotInstalledAndOnlyQualifiedObsoleteMayReclaimItsExactPrefix()throws Exception {
        for(boolean foreign:List.of(false,true)){
            Path owned=Files.createDirectory(root.resolve("partial-release-"+foreign));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source-"+foreign));var lease=f.capture(current(f,220),NativeRangeImage.IoAdmission.UNLIMITED)){
                var request=obsolete(f,lease.prepared().request());Path released=owned.resolve(request.original().transferId()+".released"),pending=released.resolveSibling(released.getFileName()+".pending");
                try(var ledger=open(owned)){retain(ledger,lease,root.resolve("partial-managed-"+foreign));lease.close();ledger.removalObserver(path->{throw new IOException("Keep partial release original artifacts");});assertThrows(IOException.class,()->ledger.released(request.original()));}
                byte[] prefix=Arrays.copyOf(Files.readAllBytes(released),64);if(foreign)prefix[0]^=1;Files.delete(released);Files.write(pending,prefix);SessionJournalFiles.force(pending);SessionJournalFiles.force(owned);sealSource(f);
                try(var ledger=open(owned)){
                    assertFalse(ledger.isReleased(request.original()));assertFalse(Files.exists(released));assertArrayEquals(prefix,Files.readAllBytes(pending));
                    if(foreign){assertThrows(IOException.class,()->ledger.forceObsolete(request));assertFalse(Files.exists(owned.resolve(request.original().transferId()+".obsolete")));assertArrayEquals(prefix,Files.readAllBytes(pending));}
                    else {ledger.forceObsolete(request);assertTrue(Files.exists(pending));assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());ledger.obsolete(request);assertFalse(Files.exists(pending));assertTrue(ledger.isObsolete(request));assertFalse(Files.exists(released));assertEquals(0,ledger.reservedImageBytes());}
                }
            }
        }
    }
    @Test void exactReadyMetadataCeilingFundsOrdinaryInstalledReleaseBeforeFilesAreReclaimed()throws Exception {
        Path owned=Files.createDirectory(root.resolve("release-ceiling"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,230),NativeRangeImage.IoAdmission.UNLIMITED)){
            var first=obsolete(f,current(f,231));var second=obsolete(f,current(f,232));long occupied;
            try(var ledger=open(owned)){retain(ledger,lease,root.resolve("release-ceiling-managed"));occupied=metadataBytes(ledger);}
            long ceiling=occupied+unknownTerminalBytes(first)+unknownTerminalBytes(second);
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),ceiling,1)){
                ledger.obsolete(first);ledger.obsolete(second);assertEquals(ceiling,metadataBytes(ledger));assertThrows(IOException.class,()->ledger.forceObsolete(obsolete(f,current(f,233))));
                assertTrue(Files.exists(owned.resolve(lease.prepared().request().transferId()+".image")));assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());lease.close();
                ledger.released(lease.prepared().request());assertTrue(ledger.isReleased(lease.prepared().request()));assertEquals(0,ledger.reservedImageBytes());assertFalse(Files.exists(owned.resolve(lease.prepared().request().transferId()+".obsolete")));
            }
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),ceiling,1)){assertTrue(ledger.isReleased(lease.prepared().request()));}
        }
    }
    @Test void exactReadyNonCurrentMetadataCeilingAlsoFundsItsInstalledRelease()throws Exception {
        Path owned=Files.createDirectory(root.resolve("bootstrap-release-ceiling"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(f.request(250),NativeRangeImage.IoAdmission.UNLIMITED)){
            var first=obsolete(f,current(f,251));var second=obsolete(f,current(f,252));long occupied;
            try(var ledger=open(owned)){retain(ledger,lease,root.resolve("bootstrap-release-managed"));occupied=metadataBytes(ledger);}
            long ceiling=occupied+unknownTerminalBytes(first)+unknownTerminalBytes(second);
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),ceiling,1)){
                ledger.obsolete(first);ledger.obsolete(second);assertEquals(ceiling,metadataBytes(ledger));assertThrows(IOException.class,()->ledger.forceObsolete(obsolete(f,current(f,253))));
                lease.close();ledger.released(lease.prepared().request());assertTrue(ledger.isReleased(lease.prepared().request()));assertEquals(0,ledger.reservedImageBytes());
            }
            try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),ceiling,1)){assertTrue(ledger.isReleased(lease.prepared().request()));}
        }
    }
    @Test void coldValidNamespaceCollisionRejectsBeforeForceAndLeavesOriginalCaptureUsable()throws Exception {
        Path owned=Files.createDirectory(root.resolve("namespace-collision"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,240),NativeRangeImage.IoAdmission.UNLIMITED)){
            var old=lease.prepared().request();try(var ledger=open(owned)){retain(ledger,lease,root.resolve("collision-managed"));}
            try(var ledger=open(owned)){
                var foreign=new RatisTopologySourceSnapshotRpc.Request(old.transferId(),null,new UUID(13210,9999),old.receiver(),old.purpose(),old.sourceGroup(),old.sourceDirectory());var forged=obsolete(f,foreign);var attempts=new AtomicInteger();ledger.obsoleteForceObserver(after->attempts.incrementAndGet());
                assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->ledger.forceObsolete(forged));assertEquals(0,attempts.get());assertFalse(Files.exists(owned.resolve(old.transferId()+".obsolete")));
                assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());assertTrue(f.store.activeSnapshotPins()>0);assertArrayEquals(lease.prepared().image().sha256(),ledger.lookup(old).prepared().image().sha256());
            }
            Path anchor=Files.createDirectory(root.resolve("anchor-collision"));try(var ledger=open(anchor)){
                var incoming=current(f,241);ledger.reserveTopologyRecovery(incoming.transferId());assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->ledger.forceObsolete(obsolete(f,incoming)));
                assertTrue(Files.isDirectory(ledger.topologyRecoveryDirectory(incoming.transferId())));assertFalse(Files.exists(anchor.resolve(incoming.transferId()+".obsolete")));
            }
        }
    }
    long metadataBytes(RatisTopologySourceSnapshotArtifacts ledger)throws Exception {var method=RatisTopologySourceSnapshotArtifacts.class.getDeclaredMethod("metadataUsage");method.setAccessible(true);return (long)method.invoke(ledger);}
    long unknownTerminalBytes(RatisRecoveryObsoleteRpc.Request request){return 3L*(17L+request.encodedSize()+32)+128;}
    @Test void durableRetryNeverCreatesAuthorityAndRejectsForeignBindingBeforeAnyForce()throws Exception {
        Path owned=Files.createDirectory(root.resolve("durable-retry"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"))){
            var request=obsolete(f,current(f,80));var canonical=owned.resolve(request.original().transferId()+".obsolete");
            try(var ledger=open(owned)){
                assertFalse(ledger.retryObsolete(request));assertFalse(Files.exists(canonical));ledger.forceObsolete(request);var original=Files.readAllBytes(canonical);
                byte[] changed=request.fenceDigest();changed[0]^=1;var attempts=new AtomicInteger();ledger.obsoleteForceObserver(after->attempts.incrementAndGet());
                assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->ledger.retryObsolete(new RatisRecoveryObsoleteRpc.Request(request.original(),request.scope(),changed)));
                assertEquals(0,attempts.get());assertArrayEquals(original,Files.readAllBytes(canonical));assertTrue(ledger.retryObsolete(request));assertEquals(2,attempts.get());assertFalse(ledger.isObsolete(request));
                ledger.obsoleteForceObserver(after->{if(after)throw new IOException("Lost existing-terminal reforce acknowledgement");});assertThrows(IOException.class,()->ledger.retryObsolete(request));assertArrayEquals(original,Files.readAllBytes(canonical));
            }
            Path pending=canonical.resolveSibling(canonical.getFileName()+".pending");Files.move(canonical,pending);SessionJournalFiles.force(owned);
            try(var ledger=open(owned)){assertTrue(ledger.retryObsolete(request));assertTrue(Files.exists(canonical));assertFalse(Files.exists(pending));ledger.obsolete(request);assertTrue(ledger.isObsolete(request));}
            var unknown=obsolete(f,current(f,81));var partial=owned.resolve(unknown.original().transferId()+".obsolete.pending");Files.write(partial,new byte[]{1,2,3});SessionJournalFiles.force(partial);SessionJournalFiles.force(owned);
            try(var ledger=open(owned)){assertFalse(ledger.retryObsolete(unknown));assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(partial));assertFalse(Files.exists(owned.resolve(unknown.original().transferId()+".obsolete")));}
        }
    }
    @Test void readyAndCapturingCurrentKeepCleanupHeadroomAtTheExactMetadataCeiling()throws Exception {
        for(boolean ready:List.of(false,true)){
            Path owned=Files.createDirectory(root.resolve("ceiling-"+ready));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source-"+ready));var lease=f.capture(current(f,90),NativeRangeImage.IoAdmission.UNLIMITED)){
                var request=obsolete(f,lease.prepared().request());var first=obsolete(f,current(f,91));var second=obsolete(f,current(f,92));long occupied;
                try(var ledger=open(owned)){
                    if(ready)retain(ledger,lease,root.resolve("managed-"+ready));else {ledger.reserve(request.original());try(var pin=f.store.capture()){ledger.capture(request.original(),pin,f.schema,f.donor);}}
                    occupied=metadataBytes(ledger);
                }
                long ceiling=occupied+unknownTerminalBytes(first)+unknownTerminalBytes(second);
                try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),ceiling,1)){
                    ledger.obsolete(first);ledger.obsolete(second);assertEquals(ceiling,metadataBytes(ledger));assertThrows(IOException.class,()->ledger.forceObsolete(obsolete(f,current(f,93))));
                    long images=ledger.reservedImageBytes();assertTrue(images>0);assertTrue(f.store.activeSnapshotPins()>0);
                    ledger.forceObsolete(request);assertFalse(ledger.isObsolete(request));assertEquals(images,ledger.reservedImageBytes());assertTrue(f.store.activeSnapshotPins()>0);assertTrue(Files.exists(owned.resolve(request.original().transferId()+".reserved")));
                    lease.close();ledger.obsolete(request);assertEquals(0,ledger.reservedImageBytes());assertTrue(ledger.isObsolete(request));
                }
                try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),ceiling,1)){assertTrue(ledger.retryObsolete(request));assertTrue(ledger.isObsolete(request));}
            }
        }
    }
    @Test void unknownTombstonesCannotTakeAnAdmittedCurrentTerminalIdentitySlot()throws Exception {
        Path owned=Files.createDirectory(root.resolve("terminal-slots"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,100),NativeRangeImage.IoAdmission.UNLIMITED);var ledger=open(owned)){
            // Retain an actual positive RELEASE receipt without its redundant publication sidecar.
            // Terminal identity admission must count the receipt itself, not depend on that sidecar.
            try(var released=f.capture(current(f,99),NativeRangeImage.IoAdmission.UNLIMITED)){
                retain(ledger,released,root.resolve("released-slot-managed"));released.close();ledger.released(released.prepared().request());
                Files.delete(owned.resolve(released.prepared().request().transferId()+".publication"));SessionJournalFiles.force(owned);
            }
            retain(ledger,lease,root.resolve("slot-managed"));var request=obsolete(f,lease.prepared().request());
            for(int n=1;n<1023;n++)ledger.forceObsolete(obsolete(f,current(f,100+n)));
            assertThrows(IOException.class,()->ledger.reserve(current(f,1125)));assertFalse(Files.exists(owned.resolve(current(f,1125).transferId()+".reserved")),"Identity preflight must reject before forcing a new reservation");
            assertThrows(IOException.class,()->ledger.forceObsolete(obsolete(f,current(f,1124))));
            assertFalse(Files.exists(owned.resolve(current(f,1124).transferId()+".obsolete")));assertTrue(f.store.activeSnapshotPins()>0);assertEquals(2*limits.maxImageBytes(),ledger.reservedImageBytes());
            ledger.forceObsolete(request);assertFalse(ledger.isObsolete(request));lease.close();ledger.obsolete(request);assertTrue(ledger.isObsolete(request));assertEquals(0,ledger.reservedImageBytes());
            long floors;try(var files=Files.list(owned)){floors=files.filter(path->path.getFileName().toString().endsWith(".obsolete")).count();}
            for(int n=0;n<65;n++)assertTrue(ledger.acknowledgeAbsentObsolete(obsolete(f,current(f,2000+n))));
            try(var files=Files.list(owned)){assertEquals(floors,files.filter(path->path.getFileName().toString().endsWith(".obsolete")).count());}assertEquals(0,ledger.reservedImageBytes());
        }
    }
    @Test void absentNamespaceForceFailuresNeverCreateFloorsAndExactRetryRechecksTheSameNamespace()throws Exception {
        for(boolean after:List.of(false,true)){
            Path owned=Files.createDirectory(root.resolve("absent-force-"+after));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source-"+after));var ledger=open(owned)){
                var request=obsolete(f,current(f,8000));var once=new AtomicBoolean(true);ledger.obsoleteForceObserver(boundary->{if(boundary==after&&once.getAndSet(false))throw new IOException("Unknown absence force acknowledgement");});
                assertThrows(IOException.class,()->ledger.acknowledgeAbsentObsolete(request));assertFalse(once.get());assertFalse(Files.exists(owned.resolve(request.original().transferId()+".obsolete")));assertEquals(0,ledger.reservedImageBytes());
                ledger.obsoleteForceObserver(boundary->{});assertTrue(ledger.acknowledgeAbsentObsolete(request));try(var files=Files.list(owned)){assertEquals(List.of("artifacts.lock"),files.map(path->path.getFileName().toString()).toList());}
                ledger.reserve(request.original());assertFalse(ledger.acknowledgeAbsentObsolete(request));
            }
        }
    }
    @Test void absenceAcknowledgementOwnsTheNamespaceThroughActualDirectoryForceAndRecheck()throws Exception {
        Path owned=Files.createDirectory(root.resolve("absent-active"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var ledger=open(owned);var worker=Executors.newSingleThreadExecutor()){
            var request=obsolete(f,current(f,8001));var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            ledger.obsoleteForceObserver(after->{if(!after){entered.countDown();try{assertTrue(release.await(5,TimeUnit.SECONDS));}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}}});
            var actual=worker.submit(()->ledger.acknowledgeAbsentObsolete(request));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->ledger.reserve(request.original()));assertThrows(IOException.class,ledger::close);assertFalse(actual.isDone());}finally{release.countDown();}
            assertTrue(actual.get(5,TimeUnit.SECONDS));assertFalse(Files.exists(owned.resolve(request.original().transferId()+".reserved")));assertFalse(Files.exists(owned.resolve(request.original().transferId()+".obsolete")));
        }
    }
    @Test void activeBulkOwnerRefusesForceUntilActualValidationReturns()throws Exception {
        Path owned=Files.createDirectory(root.resolve("bulk"));try(var f=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=f.capture(current(f,50),NativeRangeImage.IoAdmission.UNLIMITED);var ledger=open(owned);var worker=Executors.newSingleThreadExecutor()){
            retain(ledger,lease,root.resolve("managed"));var request=obsolete(f,lease.prepared().request());var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var actual=worker.submit(()->ledger.lookupAdmitted(request.original(),bytes->{entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Timed out held validation");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}}));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->ledger.forceObsolete(request));assertFalse(Files.exists(owned.resolve(request.original().transferId()+".obsolete")));}finally{release.countDown();}
            assertNotNull(actual.get(10,TimeUnit.SECONDS));ledger.forceObsolete(request);lease.close();ledger.obsolete(request);assertTrue(ledger.isObsolete(request));
        }
    }
}
