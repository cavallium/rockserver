package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Actual source GROUP/transfer ports, controlled fence/terminal ports, and genuine journal fsync/reopen. */
class RatisConfiguredRecoverySupersessionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static UUID id(int n){return new UUID(12060,n);}
    final class Fixture implements AutoCloseable,RatisConfiguredRecovery.Host {
        final RatisConfiguredRecoveryTest helper=new RatisConfiguredRecoveryTest();final RatisConfiguredRecoveryTest.Fixture base;
        final List<Request> aliases;final Request selected;final NativeRecoverySupersession.Scope scope;final Path journal;
        final List<String> calls=new ArrayList<>();final List<RatisConfiguredRecovery.CleanupAction> actions=new ArrayList<>();final Map<String,RatisConfiguredRecovery.CleanupDisposition> disposed=new LinkedHashMap<>();
        final byte[] fence=new byte[32];boolean loseFence,loseCleanup,localMetadata,originalMetadata,registrationOrigin;RatisConfiguredRecovery.CleanupWitness observedWitness;int fences,metadataPrepares;
        Fixture(String name)throws Exception {
            helper.root=root.resolve(name);Files.createDirectories(helper.root);base=helper.new Fixture();journal=Files.createDirectory(helper.root.resolve("actor"));fence[0]=19;
            var a=base.initial;aliases=List.of(a,a.successor(id(2)),a.successor(id(3)));var directory=a.sourceDirectory();var parent=directory.ranges().getFirst();
            var left=new RangeDescriptor(id(10),id(11),parent.rangeEpoch(),parent.configurationVersion(),null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,parent.replicas());var right=new RangeDescriptor(id(12),id(13),parent.rangeEpoch(),parent.configurationVersion(),new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
            var target=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation()+1,directory.columns(),directory.routingPrefixBytes(),List.of(left,right));var consuming=new TopologyChange.Definition(id(14),directory,target);
            selected=new Request(id(4),consuming.header(),a.catalogue(),a.receiver(),Purpose.ANCESTOR_METADATA,a.sourceGroup(),directory);
            scope=new NativeRecoverySupersession.Scope(a.catalogue(),new ApplyPosition(1,100),consuming.header(),directory,a.sourceGroup(),a.receiver(),digest(selected),aliases.stream().map(RatisConfiguredRecoverySupersessionTest::digest).toList());
        }
        RatisConfiguredRecovery open(RatisJournalAccess files)throws Exception{return new RatisConfiguredRecovery(journal,base.binding,Files.exists(journal.resolve("recovery"))?null:base.initial,this,base.io,files,RatisConfiguredRecoveryTest.metadata(()->{}),request->{if(!request.sourceGroup().equals(base.initial.sourceGroup()))throw new IOException("Foreign group");});}
        public Request resolveCurrentAlias(RatisConfiguredRecovery.CurrentAlias reference)throws IOException{return aliases.stream().filter(alias->alias.transferId().equals(reference.transferId()) && Arrays.equals(digest(alias),reference.digest())).findFirst().orElseThrow(()->new IOException("Missing immutable graph alias"));}
        public CompletableFuture<RatisConfiguredRecovery.Preparation> prepare(Request request,RatisConfiguredRecovery.CleanupWitness witness) {
            if(request.metadataOnly()){metadataPrepares++;if(!originalMetadata)assertEquals(aliases.size()*2,disposed.size());observedWitness=witness;if(localMetadata)try{return CompletableFuture.completedFuture(new RatisConfiguredRecovery.LocalCompletion(localReceipt(request)));}catch(Exception failed){return CompletableFuture.failedFuture(failed);}return CompletableFuture.failedFuture(new IOException("META dispatch reached after exact old cleanup"));}
            for(int i=0;i<2;i++)if(request.transferId().equals(aliases.get(i).transferId()))try{return CompletableFuture.failedFuture(new AbandonedException(new Abandoned(request,aliases.get(i+1).transferId(),base.source.store.applied(),base.source.donor,OperatorUpload.sha256(base.source.store.configuration().data()))));}catch(Exception failed){return CompletableFuture.failedFuture(failed);}
            return base.prepare(request,witness);
        }
        RatisConfiguredRecovery.LocalMetadataCompletion localReceipt(Request request)throws Exception {var cut=base.source.store.applied();var cfg=base.source.store.configuration();var head=new RatisSourceHeadRpc.Head(new RatisSourceHeadRpc.Request(request,cut),new ApplyPosition(1,100),request.sourceDirectory(),base.source.donor,cut,cfg);return new RatisConfiguredRecovery.LocalMetadataCompletion(digest(request),cut,new byte[32],cut,cfg,head,registrationOrigin?RatisConfiguredRecovery.LocalMetadataOrigin.METADATA_REGISTRATION:RatisConfiguredRecovery.LocalMetadataOrigin.LOCAL_SEED,OperatorUpload.sha256(new byte[]{1}),OperatorUpload.sha256(new byte[]{2}));}
        void finishCleanup(RatisConfiguredRecovery actor)throws Exception {actor.reconcile(selected,scope,aliases);for(int n=0;n<40 && actor.phase()!=RatisConfiguredRecovery.Phase.PREPARE;n++)RatisConfiguredRecoveryTest.step(actor);assertEquals(RatisConfiguredRecovery.Phase.PREPARE,actor.phase());}
        public CompletableFuture<byte[]> register(Prepared value){return base.register(value);}
        public CompletableFuture<Prepared> transfer(Request request){return base.transfer(request);}
        public CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request){return base.probe(request);}
        public CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request){return base.head(request);}
        public CompletableFuture<Boolean> complete(Prepared value,RatisSourceHeadRpc.Head head){return base.complete(value,head);}
        public CompletableFuture<Void> release(Prepared value){return base.release(value);}
        public CompletableFuture<byte[]> forceSupersession(NativeRecoverySupersession.Scope value){assertEquals(scope,value);fences++;if(loseFence){loseFence=false;return CompletableFuture.failedFuture(new IOException("Lost native fence ACK"));}return CompletableFuture.completedFuture(fence.clone());}
        public CompletableFuture<RatisConfiguredRecovery.CleanupDisposition> cleanup(Request alias,ReplicaDescriptor voter,RatisConfiguredRecovery.CleanupAction action,NativeRecoverySupersession.Scope value,byte[] frozenFence) {
            assertEquals(scope,value);assertArrayEquals(fence,frozenFence);assertTrue(alias.sourceOwner(voter));var key=alias.transferId()+"/"+voter.replicaId();calls.add(key);actions.add(action);
            var disposition=alias.transferId().equals(aliases.getLast().transferId()) && voter.equals(base.source.donor)?RatisConfiguredRecovery.CleanupDisposition.RELEASED:RatisConfiguredRecovery.CleanupDisposition.OBSOLETE;
            if(action!=(disposition==RatisConfiguredRecovery.CleanupDisposition.RELEASED?RatisConfiguredRecovery.CleanupAction.RELEASE:RatisConfiguredRecovery.CleanupAction.OBSOLETE))return CompletableFuture.failedFuture(new IOException("Requested source disposition has no canonical receipt"));
            assertEquals(disposition,disposed.computeIfAbsent(key,ignored->disposition));if(loseCleanup){loseCleanup=false;return CompletableFuture.failedFuture(new IOException("Lost source cleanup ACK"));}return CompletableFuture.completedFuture(disposition);
        }
        void reachFrozenCurrent(RatisConfiguredRecovery actor)throws Exception {for(int i=0;i<12 && actor.phase()!=RatisConfiguredRecovery.Phase.COMPLETE;i++)RatisConfiguredRecoveryTest.step(actor);assertEquals(RatisConfiguredRecovery.Phase.COMPLETE,actor.phase());assertEquals(0,metadataPrepares);}
        public void close()throws Exception{base.close();}
    }
    static byte[] digest(Request request){return OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(PREPARE,request));}
    @Test void abandonedAliasChainAndOldEvidenceSurviveFenceAndCleanupAcknowledgementLossAcrossColdRetry()throws Exception {
        try(var f=new Fixture("chain")) {
            try(var actor=f.open(RatisJournalAccess.standalone())){f.reachFrozenCurrent(actor);var abandoned=actor.retainedRequests(bytes->{});assertEquals(3,abandoned.requests().size());requireRequest(f.aliases.getLast(),abandoned.selected());actor.reconcile(f.selected,f.scope,f.aliases);var retained=actor.retainedRequests(bytes->{});assertEquals(4,retained.requests().size());requireRequest(f.selected,retained.selected());assertEquals(RatisConfiguredRecovery.Phase.FENCE_RECEIVER,actor.phase());assertEquals(0,f.fences);var fields=RatisSchemaClientJournal.read(f.journal.resolve("recovery"));for(String name:List.of("initial","request","prepared","registration","head","abandoned","aliases","selected","supersessionScope"))assertTrue(fields.containsKey(name));assertEquals(4+48*f.aliases.size(),fields.get("aliases").length);f.loseFence=true;assertThrows(ExecutionException.class,()->RatisConfiguredRecoveryTest.step(actor));}
            try(var actor=f.open(RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.FENCE_RECEIVER,actor.phase());assertEquals(RatisConfiguredRecovery.Phase.CLEANUP_OLD,RatisConfiguredRecoveryTest.step(actor));assertEquals(2,f.fences);f.loseCleanup=true;RatisConfiguredRecoveryTest.step(actor);RatisConfiguredRecoveryTest.step(actor);assertEquals(1,f.disposed.size());assertEquals(0,RatisSchemaClientJournal.read(f.journal.resolve("recovery")).get("cleanup").length);assertEquals(0,f.metadataPrepares);}
            try(var actor=f.open(RatisJournalAccess.standalone())) {
                assertEquals(RatisConfiguredRecovery.Phase.CLEANUP_OLD,actor.phase());RatisConfiguredRecoveryTest.step(actor);RatisConfiguredRecoveryTest.step(actor);assertEquals(f.calls.get(0),f.calls.get(3));assertEquals(List.of(RatisConfiguredRecovery.CleanupAction.RELEASE,RatisConfiguredRecovery.CleanupAction.OBSOLETE,RatisConfiguredRecovery.CleanupAction.RELEASE,RatisConfiguredRecovery.CleanupAction.OBSOLETE),f.actions.subList(0,4));
                for(int i=0;i<40 && actor.phase()==RatisConfiguredRecovery.Phase.CLEANUP_OLD;i++)RatisConfiguredRecoveryTest.step(actor);assertEquals(RatisConfiguredRecovery.Phase.PREPARE,actor.phase());requireRequest(f.selected,actor.initial());assertEquals(6,f.disposed.size());assertEquals(0,f.metadataPrepares);
                var fields=RatisSchemaClientJournal.read(f.journal.resolve("recovery"));assertFalse(fields.containsKey("supersessionScope"));assertTrue(fields.containsKey("cleanupScope"));assertTrue(fields.containsKey("cleanupAliases"));assertTrue(fields.containsKey("cleanupDispositions"));assertFalse(fields.containsKey("prepared"));assertThrows(ExecutionException.class,()->RatisConfiguredRecoveryTest.step(actor));assertEquals(1,f.metadataPrepares);
            }
        }
    }
    @Test void journalForceLossRetriesFrozenCandidateBeforeAnyFenceOrNextCleanupRpc()throws Exception {
        for(boolean after:List.of(false,true))try(var f=new Fixture("journal-"+after)) {
            var lose=new AtomicBoolean();var files=new RatisConfiguredRecoveryTest.Forwarding(){public void write(Path path,byte[] body,int maximum)throws IOException{if(!after && lose.getAndSet(false))throw new IOException("Lost before journal force");super.write(path,body,maximum);if(after && lose.getAndSet(false))throw new IOException("Lost after journal force");}};
            try(var actor=f.open(files)){f.reachFrozenCurrent(actor);lose.set(true);assertThrows(IOException.class,()->actor.reconcile(f.selected,f.scope,f.aliases));assertEquals(0,f.fences);assertEquals(RatisConfiguredRecovery.Phase.FENCE_RECEIVER,RatisConfiguredRecoveryTest.step(actor));assertEquals(0,f.fences);RatisConfiguredRecoveryTest.step(actor);lose.set(true);assertThrows(ExecutionException.class,()->RatisConfiguredRecoveryTest.step(actor));assertEquals(1,f.calls.size());RatisConfiguredRecoveryTest.step(actor);assertEquals(1,f.calls.size(),"Exact dirty action/cursor force must settle before another RPC");}
            try(var actor=f.open(files)){assertEquals(RatisConfiguredRecovery.Phase.CLEANUP_OLD,actor.phase());assertEquals(0,f.metadataPrepares);for(int i=0;i<40 && actor.phase()==RatisConfiguredRecovery.Phase.CLEANUP_OLD;i++)RatisConfiguredRecoveryTest.step(actor);assertEquals(6,f.disposed.size());}
        }
    }
    @Test void completedCleanupWitnessSurvivesColdLocalCompletionAndMetadataUuidRenewal()throws Exception {
        try(var f=new Fixture("local-cold")){
            f.localMetadata=true;RatisConfiguredRecovery.CleanupWitness witness;byte[] localDigest;
            try(var actor=f.open(RatisJournalAccess.standalone())){
                f.reachFrozenCurrent(actor);f.finishCleanup(actor);var retained=actor.retainedRequests(bytes->{});witness=retained.witness();assertNotNull(witness);assertEquals(6,witness.dispositions().size());requireRequest(f.selected,witness.originalSelected());assertEquals(RatisConfiguredRecovery.Phase.PREPARE,retained.phase());
                int registers=f.base.registers,releases=f.base.releases,completions=f.base.completions;assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,RatisConfiguredRecoveryTest.step(actor));assertSame(witness,f.observedWitness);
                assertEquals(registers,f.base.registers);assertEquals(releases,f.base.releases);assertEquals(completions,f.base.completions);assertEquals(1,f.metadataPrepares);assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,RatisConfiguredRecoveryTest.step(actor));assertEquals(1,f.metadataPrepares);
                var done=actor.retainedRequests(bytes->{});assertEquals(witness,done.witness());assertNotNull(done.localCompletion());localDigest=done.localCompletion().requestDigest();var fields=RatisSchemaClientJournal.read(f.journal.resolve("recovery"));assertTrue(fields.containsKey("localCompletion"));for(String key:List.of("prepared","registration","head","abandoned","selected"))assertFalse(fields.containsKey(key));
            }
            try(var actor=f.open(RatisJournalAccess.standalone())){
                assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,actor.phase());var retained=actor.retainedRequests(bytes->{});assertEquals(witness,retained.witness());assertArrayEquals(localDigest,retained.localCompletion().requestDigest());assertEquals(1,f.metadataPrepares);
                var renewed=f.selected.successor(id(80));actor.ensureSelected(renewed);assertEquals(RatisConfiguredRecovery.Phase.PREPARE,actor.phase());var selected=actor.retainedRequests(bytes->{});requireRequest(renewed,selected.selected());assertEquals(witness,selected.witness());assertNull(selected.localCompletion());
                actor.reconcile(renewed,f.scope,f.aliases);assertEquals(witness,actor.retainedRequests(bytes->{}).witness());assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,RatisConfiguredRecoveryTest.step(actor));assertEquals(2,f.metadataPrepares);assertEquals(witness,f.observedWitness);assertArrayEquals(f.scope.selectedMetadataRequestDigest(),digest(f.observedWitness.originalSelected()));assertEquals(1,f.fences);
            }
            try(var actor=f.open(RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,actor.phase());assertEquals(witness,actor.retainedRequests(bytes->{}).witness());assertEquals(2,f.metadataPrepares);}
        }
    }
    @Test void localTerminalLostJournalAcknowledgementRetriesExactCandidateAndColdReceipt()throws Exception {
        for(boolean after:List.of(false,true))try(var f=new Fixture("local-force-"+after)){
            f.localMetadata=true;var lose=new AtomicBoolean();var files=new RatisConfiguredRecoveryTest.Forwarding(){public void write(Path path,byte[] body,int maximum)throws IOException{if(!after&&lose.getAndSet(false))throw new IOException("Lost local receipt beforeforce");super.write(path,body,maximum);if(after&&lose.getAndSet(false))throw new IOException("Lost local receipt force acknowledgement");}};
            try(var actor=f.open(files)){
                f.reachFrozenCurrent(actor);f.finishCleanup(actor);lose.set(true);assertThrows(ExecutionException.class,()->RatisConfiguredRecoveryTest.step(actor));assertEquals(1,f.metadataPrepares);
                if(!after){assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,RatisConfiguredRecoveryTest.step(actor));assertEquals(1,f.metadataPrepares,"Dirty local receipt is forced before another Host call");}
            }
            try(var actor=f.open(files)){assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,actor.phase());assertEquals(1,f.metadataPrepares);assertNotNull(actor.retainedRequests(bytes->{}).localCompletion());assertNotNull(actor.retainedRequests(bytes->{}).witness());}
        }
    }
    @Test void originalMetadataHostCanReturnLocalCompletionWithoutSupersessionOrRemotePorts()throws Exception {
        try(var f=new Fixture("local-original")){
            f.localMetadata=true;f.originalMetadata=true;f.registrationOrigin=true;
            try(var actor=new RatisConfiguredRecovery(f.journal,f.base.binding,f.selected,f,f.base.io,RatisJournalAccess.standalone(),RatisConfiguredRecoveryTest.metadata(()->{}),request->requireRequest(f.selected,request))){
                assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,RatisConfiguredRecoveryTest.step(actor));var retained=actor.retainedRequests(bytes->{});assertNull(retained.witness());assertNotNull(retained.localCompletion());assertEquals(0,f.fences);assertTrue(f.disposed.isEmpty());assertEquals(0,f.base.registers);assertEquals(0,f.base.releases);assertEquals(0,f.base.completions);
                var good=retained.localCompletion();byte[] wrong=good.requestDigest();wrong[0]^=1;var bad=new RatisConfiguredRecovery.LocalMetadataCompletion(wrong,good.imageCut(),good.imageHash(),good.completedCut(),good.configuration(),good.head(),good.originKind(),good.originDigest(),good.freezeDigest());assertThrows(IllegalArgumentException.class,()->bad.require(f.selected));assertThrows(IllegalArgumentException.class,()->good.require(f.aliases.getFirst()));
            }
            try(var actor=f.open(RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.LOCAL_DONE,actor.phase());var receipt=actor.retainedRequests(bytes->{}).localCompletion();assertEquals(RatisConfiguredRecovery.LocalMetadataOrigin.METADATA_REGISTRATION,receipt.originKind());assertArrayEquals(OperatorUpload.sha256(new byte[]{1}),receipt.originDigest());assertEquals(1,f.metadataPrepares);}
        }
    }
    @Test void managerReconcilesInsideOwnedDiscoveryWithoutNestedQueueAndRetainsOneSlot()throws Exception {
        try(var f=new Fixture("manager")) {
            var pool=id(30);var limits=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1<<20,16),new RatisNodeResourceBudget.Lane(64L<<20,16));var resources=new RatisNodeResourceBudget(limits,Map.of(pool,limits));var path=Files.createDirectory(root.resolve("manager-journals"));var reference=new AtomicReference<RatisConfiguredRecoveries>();
            var manager=new RatisConfiguredRecoveries(path,f.base.binding,2,Map.of(pool,2),resources,new RatisConfiguredRecoveries.Owner(){public UUID pool(Request request)throws IOException{if(!request.sourceGroup().equals(f.selected.sourceGroup()))throw new IOException("Foreign group");return pool;}public RatisJournalAccess journal(UUID ignored,UUID group,Path directory){assertTrue(reference.get().ownsDiscoveryIo());return RatisJournalAccess.standalone();}},admission->f);reference.set(manager);
            try{assertThrows(IOException.class,()->manager.reconcileOnIo(f.selected,f.scope,f.aliases));manager.discover(()->{assertTrue(manager.ownsDiscoveryIo());manager.reconcileOnIo(f.selected,f.scope,f.aliases);return null;}).get(5,TimeUnit.SECONDS);assertTrue(manager.contains(f.selected.sourceGroup()));assertEquals(1,manager.size());assertEquals(RatisConfiguredRecovery.Phase.FENCE_RECEIVER,manager.phase(f.selected.sourceGroup()));assertEquals(0,f.fences);manager.step().get(5,TimeUnit.SECONDS);assertEquals(1,f.fences);assertEquals(0,f.metadataPrepares);}finally{manager.close(Duration.ofSeconds(10));}assertEquals(0,resources.stats().protectedLane().owners());
        }
    }
}
