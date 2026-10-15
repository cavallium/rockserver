package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

class RatisConfiguredRecoveryGraphTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(long n){return new UUID(988,n);}
    static final ReplicaDescriptor LOCAL=new ReplicaDescriptor(id(1),id(2),id(3),"local","127.0.0.1:32101",ReplicaDescriptor.Role.VOTER);
    static final UUID CLUSTER=id(4),DATABASE=id(5),DOMAIN=id(6),COLUMN=id(7);
    static RangeDescriptor range(long n,byte[] lo,byte[] hi){return new RangeDescriptor(id(n),id(n+100),1,0,lo,hi,RangeDescriptor.TopologyState.ACTIVE,List.of(LOCAL));}
    static RangeDirectory directory(long generation,List<RangeDescriptor> ranges){return new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],generation,Set.of(COLUMN),1,ranges);}
    final RangeDirectory genesis=directory(1,List.of(range(20,null,null)));
    final TopologyChange.Definition split=new TopologyChange.Definition(id(30),genesis,directory(2,List.of(range(21,null,new byte[]{64}),range(22,new byte[]{64},null))));
    final TopologyChange.Definition merge=new TopologyChange.Definition(id(31),split.targetDirectory(),directory(3,List.of(range(23,null,null))));
    RatisNodePlan node(String physical){var catalogue=new RatisNodePlan.Catalogue(id(8),1,new byte[32],id(9),id(10),List.of(LOCAL));var domains=List.of(new RatisNodePlan.Domain(genesis,Map.of(COLUMN,physical),Set.of()));return new RatisNodePlan(LOCAL.nodeId(),LOCAL.nodeIncarnation(),domains,catalogue,List.of(),List.of(),Map.of(),8,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(domains),RatisPoolFixtures.groups(catalogue,List.of()));}
    RatisConfiguredRecoveryDiscovery.Plan discover(RatisNodePlan node,RangeDirectory current,long index,TopologyChange.Definition... definitions)throws IOException {
        var births=new HashMap<UUID,TopologyChange.Definition>();for(var d:definitions)for(var r:d.targets())births.put(r.groupId(),d);var cut=new ApplyPosition(1,index);
        return RatisConfiguredRecoveryDiscovery.discover(node,new RatisConfiguredRecoveryDiscovery.Source(){
            public RatisCatalogueViewRpc.View view(RatisCatalogueViewRpc.Request r,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException{var wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(r,cut,current));admission.beforeReply(wire.length);return RatisCatalogueViewRpc.reply(wire,r);}
            public RatisRecoveryLineageRpc.Reply birth(RatisRecoveryLineageRpc.Request r,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException{var wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(r,cut,births.get(r.group())));admission.beforeReply(wire.length);return RatisRecoveryLineageRpc.reply(wire,r);}
        },(retained,incoming)->{});
    }
    List<Request> requests(RatisNodePlan node,RatisConfiguredRecoveryDiscovery.Plan plan,long offset){var result=new ArrayList<Request>();for(var o:plan.localObligations())result.add(new Request(id(offset++),o.header(),node.catalogue().groupId(),o.localReplica(),o.purpose(),o.range().groupId(),o.sourceDirectory()));return result;}
    Path directory(String name)throws IOException{return Files.createDirectory(root.resolve(name));}
    static final RatisConfiguredRecoveryGraph.Admission HEAP=(retained,incoming)->{};
    static class FilesAccess implements RatisJournalAccess {
        final RatisJournalAccess delegate=RatisJournalAccess.standalone();int reads,writes;boolean before,after,pending;
        public RootLease backendRoot(Path root)throws IOException{return delegate.backendRoot(root);}public void register(Path path,int maximum)throws IOException{delegate.register(path,maximum);}
        public byte[] read(Path path,int maximum)throws IOException{reads++;return delegate.read(path,maximum);}
        public void write(Path path,byte[] body,int maximum)throws IOException{writes++;if(before)throw new IOException("Before publication");if(pending){delegate.write(path.resolveSibling("graph.pending"),body,maximum);throw new IOException("Before canonical rename");}delegate.write(path,body,maximum);if(after)throw new IOException("After forced publication");}
        public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException{delegate.remove(path,body,maximum,removal);}public void deleted(Path path)throws IOException{delegate.deleted(path);}public void reclaimedTree(Path directory)throws IOException{delegate.reclaimedTree(directory);}public void reserve(Reservation reservation)throws IOException{delegate.reserve(reservation);}public void fulfilled(Reservation reservation)throws IOException{delegate.fulfilled(reservation);}
    }
    @Test void frozenGraphColdReopensAndTopologyProgressRetainsDefinitionsAndAttempts()throws Exception {
        var node=node("data");var initial=discover(node,genesis,10);var first=requests(node,initial,1000);var next=discover(node,split.targetDirectory(),20,split);var second=requests(node,next,1100);Path path=directory("progress");
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertNull(graph.snapshot());graph.force(initial,first);assertEquals(1,graph.snapshot().frozenRequests().size());graph.force(next,second);assertEquals(1,graph.snapshot().retainedDefinitions().size());assertEquals(4,graph.snapshot().frozenRequests().size());assertTrue(graph.retainedEncodedBytes()>0);}
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertEquals(new ApplyPosition(1,20),graph.snapshot().plan().catalogueCut());assertEquals(4,graph.snapshot().frozenRequests().size());assertEquals(first.getFirst().transferId(),graph.snapshot().frozenRequests().getFirst().transferId());var finalPlan=discover(node,merge.targetDirectory(),30,split,merge);graph.force(finalPlan,requests(node,finalPlan,1200));assertEquals(2,graph.snapshot().retainedDefinitions().size());assertEquals(8,graph.snapshot().frozenRequests().size());}
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertEquals(new ApplyPosition(1,30),graph.snapshot().plan().catalogueCut());assertEquals(2,graph.snapshot().retainedDefinitions().size());assertEquals(8,graph.snapshot().frozenRequests().size());}
    }
    @Test void unknownForceRequiresExactCandidateAndColdPublicationIsOldOrNew()throws Exception {
        for(boolean after:List.of(false,true)) {
            var node=node("data");var initial=discover(node,genesis,10);var next=discover(node,split.targetDirectory(),20,split);var first=requests(node,initial,2000);var second=requests(node,next,2100);Path path=directory("unknown-"+after);var files=new FilesAccess();
            try(var graph=new RatisConfiguredRecoveryGraph(path,node,files,HEAP)){graph.force(initial,first);files.before=!after;files.after=after;assertThrows(IOException.class,()->graph.force(next,second));assertEquals(initial.catalogueCut(),graph.snapshot().plan().catalogueCut());assertTrue(graph.retainedEncodedBytes()>Files.size(path.resolve("graph")));assertThrows(IOException.class,()->graph.force(discover(node,split.targetDirectory(),21,split),second));}
            try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertEquals(after?next.catalogueCut():initial.catalogueCut(),graph.snapshot().plan().catalogueCut());assertEquals(after?4:1,graph.snapshot().frozenRequests().size());graph.force(next,second);assertEquals(4,graph.snapshot().frozenRequests().size());}
            // A separate live lost-ACK retry must perform another sync even if the candidate is readable.
            var retryFiles=new FilesAccess();try(var graph=new RatisConfiguredRecoveryGraph(path,node,retryFiles,HEAP)){retryFiles.after=true;assertThrows(IOException.class,()->graph.force(next,second));retryFiles.after=false;int writes=retryFiles.writes;assertTrue(graph.retryPending());assertFalse(graph.retryPending());assertEquals(writes+1,retryFiles.writes);}
        }
    }
    @Test void validPendingSettlesExactCandidateAndPartialPendingPreservesMain()throws Exception {
        var node=node("data");var initial=discover(node,genesis,10);var next=discover(node,split.targetDirectory(),20,split);var first=requests(node,initial,3000);var second=requests(node,next,3100);Path path=directory("pending");var files=new FilesAccess();
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,files,HEAP)){graph.force(initial,first);files.pending=true;assertThrows(IOException.class,()->graph.force(next,second));}
        assertTrue(Files.exists(path.resolve("graph.pending")));try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertEquals(next.catalogueCut(),graph.snapshot().plan().catalogueCut());assertEquals(4,graph.snapshot().frozenRequests().size());}assertFalse(Files.exists(path.resolve("graph.pending")));
        byte[] original=Files.readAllBytes(path.resolve("graph"));Files.write(path.resolve("graph.pending"),new byte[]{1,2,3});try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertEquals(next.catalogueCut(),graph.snapshot().plan().catalogueCut());assertEquals(first.getFirst().transferId(),graph.snapshot().frozenRequests().getFirst().transferId());}assertArrayEquals(original,Files.readAllBytes(path.resolve("graph")));assertFalse(Files.exists(path.resolve("graph.pending")));
        Path empty=directory("initial-partial");Files.write(empty.resolve("graph.pending"),new byte[]{1});try(var graph=new RatisConfiguredRecoveryGraph(empty,node,RatisJournalAccess.standalone(),HEAP)){assertNull(graph.snapshot());}assertFalse(Files.exists(empty.resolve("graph.pending")));
    }
    @Test void rejectsChangedUuidBindingTruncatedMainAndValidForeignPendingWithoutOverwriting()throws Exception {
        var node=node("data");var initial=discover(node,genesis,10);var first=requests(node,initial,4000);Path path=directory("integrity");
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){graph.force(initial,first);var changed=new Request(first.getFirst().transferId(),split.header(),node.catalogue().groupId(),LOCAL,Purpose.ANCESTOR_METADATA,genesis.ranges().getFirst().groupId(),genesis);assertThrows(IOException.class,()->graph.force(discover(node,split.targetDirectory(),20,split),List.of(changed)));}
        byte[] original=Files.readAllBytes(path.resolve("graph"));assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node("renamed"),RatisJournalAccess.standalone(),HEAP));assertArrayEquals(original,Files.readAllBytes(path.resolve("graph")));
        Path foreign=directory("foreign");var other=node("renamed");try(var graph=new RatisConfiguredRecoveryGraph(foreign,other,RatisJournalAccess.standalone(),HEAP)){var plan=discover(other,genesis,10);graph.force(plan,requests(other,plan,4100));}Files.copy(foreign.resolve("graph"),path.resolve("graph.pending"));assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP));assertTrue(Files.exists(path.resolve("graph.pending")));assertArrayEquals(original,Files.readAllBytes(path.resolve("graph")));Files.delete(path.resolve("graph.pending"));Files.write(path.resolve("graph"),Arrays.copyOf(original,original.length-1));assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP));
    }
    @Test void admissionPrecedesReadAndEncodingAndOversizeFramesAndAttemptsAreRejected()throws Exception {
        var node=node("data");var plan=discover(node,genesis,10);var requests=requests(node,plan,5000);Path path=directory("admission");var access=new FilesAccess();
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,access,(retained,incoming)->{throw new IOException("No heap");})){assertThrows(IOException.class,()->graph.force(plan,requests));assertEquals(0,access.writes);assertNull(graph.snapshot());}
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){graph.force(plan,requests);}
        assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node,access,(retained,incoming)->{throw new IOException("No read heap");}));assertEquals(0,access.reads);
        Path large=directory("large");try(var file=java.nio.channels.FileChannel.open(large.resolve("graph"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)){file.position(RatisConfiguredRecoveryGraph.MAX_BYTES);file.write(java.nio.ByteBuffer.wrap(new byte[]{0}));}assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(large,node,access,HEAP));assertEquals(0,access.reads);
        Path many=directory("many");try(var graph=new RatisConfiguredRecoveryGraph(many,node,access,HEAP)){var additions=new ArrayList<Request>();for(int i=0;i<=RatisNodePlan.MAX_LOCAL_GROUPS;i++)additions.add(requests.getFirst().successor(id(10000+i)));assertThrows(IOException.class,()->graph.force(plan,additions));assertEquals(0,access.writes);}
    }
    @Test void initialValidPendingPromotesUnderRealSharedBudgetWithoutDuplicateDiskDebt()throws Exception {
        var node=node("data");var plan=discover(node,genesis,10);var attempts=requests(node,plan,6000);Path path=directory("initial-shared-pending");
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){graph.force(plan,attempts);}
        Files.move(path.resolve("graph"),path.resolve("graph.pending"));byte[] exact=Files.readAllBytes(path.resolve("graph.pending"));
        Object inode=Files.readAttributes(path.resolve("graph.pending"),java.nio.file.attribute.BasicFileAttributes.class).fileKey();
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())) {
            budget.register(path.resolve("graph"),RatisConfiguredRecoveryGraph.MAX_BYTES);assertEquals(exact.length,budget.chargedDiskBytes());
            try(var graph=new RatisConfiguredRecoveryGraph(path,node,budget,HEAP)){assertEquals(attempts.getFirst().transferId(),graph.snapshot().frozenRequests().getFirst().transferId());assertFalse(graph.retryPending());assertEquals(exact.length,budget.chargedDiskBytes());}
            assertFalse(Files.exists(path.resolve("graph.pending")));assertArrayEquals(exact,Files.readAllBytes(path.resolve("graph")));if(inode!=null)assertEquals(inode,Files.readAttributes(path.resolve("graph"),java.nio.file.attribute.BasicFileAttributes.class).fileKey());
            try(var graph=new RatisConfiguredRecoveryGraph(path,node,budget,HEAP)){assertEquals(plan.catalogueCut(),graph.snapshot().plan().catalogueCut());assertEquals(exact.length,budget.chargedDiskBytes());}
            assertEquals(0,budget.retainedRequests());assertEquals(exact.length,budget.chargedDiskBytes());
        }
    }
    @Test void canonicalFramesWithDuplicateDefinitionIntentOrTransferUuidAreRejected()throws Exception {
        var node=node("data");var plan=discover(node,split.targetDirectory(),20,split);Path path=directory("duplicate-frame");
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){graph.force(plan,requests(node,plan,7000));}
        byte[] original=SessionJournalFiles.read(path.resolve("graph"),RatisConfiguredRecoveryGraph.MAX_BYTES);
        for(boolean definition:List.of(true,false)) {
            byte[] malformed=duplicateEntry(original,definition);SessionJournalFiles.write(path.resolve("graph"),malformed,RatisConfiguredRecoveryGraph.MAX_BYTES);
            var failure=assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP));assertTrue(failure.getMessage().contains(definition?"Duplicate recovery definition intent":"Duplicate frozen recovery transfer"));assertArrayEquals(malformed,SessionJournalFiles.read(path.resolve("graph"),RatisConfiguredRecoveryGraph.MAX_BYTES));
        }
    }
    private static byte[] duplicateEntry(byte[] original,boolean definition) {
        var input=java.nio.ByteBuffer.wrap(original);input.position(52);int directories=input.getInt();for(int i=0;i<directories;i++){int size=input.getInt();input.position(input.position()+size);}int roots=input.getInt();input.position(input.position()+4*roots);
        int countOffset=input.position(),count=input.getInt(),width=57;
        if(!definition){input.position(input.position()+57*count);countOffset=input.position();count=input.getInt();width=45;}
        assertTrue(count>0);int first=input.position(),insert=first+width*count;byte[] result=new byte[original.length+width];System.arraycopy(original,0,result,0,insert);System.arraycopy(original,first,result,insert,width);System.arraycopy(original,insert,result,insert+width,original.length-insert);java.nio.ByteBuffer.wrap(result).putInt(countOffset,count+1);return result;
    }
    @Test void oversizedCatalogueDirectoryIsRejectedBeforeGraphPublication()throws Exception {
        int width=(RatisCatalogue.MAX_DIRECTORY_BYTES+1)/2;byte[] boundary=new byte[width];boundary[0]=64;
        var ranges=List.of(range(90,null,boundary),range(91,boundary,null));
        var oversized=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],2,Set.of(COLUMN),width,ranges);assertTrue(RangeDirectoryCodec.encodedSize(oversized)>RatisCatalogue.MAX_DIRECTORY_BYTES);
        var proposed=new RatisConfiguredRecoveryDiscovery.Plan(new ApplyPosition(1,20),ranges.stream().map(r->new RatisConfiguredRecoveryDiscovery.Root(oversized,r)).toList(),List.of(),List.of(),List.of());Path path=directory("oversized-directory");var access=new FilesAccess();
        try(var graph=new RatisConfiguredRecoveryGraph(path,node("data"),access,HEAP)){var failure=assertThrows(IOException.class,()->graph.force(proposed,List.of()));assertTrue(failure.getMessage().contains("directory exceeds catalogue bound"));assertEquals(0,access.writes);assertNull(graph.snapshot());assertFalse(Files.exists(path.resolve("graph")));}
    }

    @Test void validatedColdMainPublishesWithoutRewritingItsCanonicalFile()throws Exception {
        var node=node("data");var plan=discover(node,genesis,10);Path path=directory("cold-main-durability");
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){graph.force(plan,requests(node,plan,8000));}
        var file=path.resolve("graph");byte[] exact=Files.readAllBytes(file);var before=Files.readAttributes(file,java.nio.file.attribute.BasicFileAttributes.class);var access=new FilesAccess();
        // Standalone reads perform no fsync; the graph constructor owns the durability barrier.
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,access,HEAP)){assertEquals(plan.catalogueCut(),graph.snapshot().plan().catalogueCut());assertEquals(1,access.reads);assertEquals(0,access.writes);}
        var after=Files.readAttributes(file,java.nio.file.attribute.BasicFileAttributes.class);assertArrayEquals(exact,Files.readAllBytes(file));assertEquals(before.lastModifiedTime(),after.lastModifiedTime());if(before.fileKey()!=null)assertEquals(before.fileKey(),after.fileKey());
    }

    @Test void activeRenewalSurvivesColdReopenAndCannotResurrectHistoricalRequest()throws Exception {
        var node=node("data");var plan=discover(node,genesis,10);Path path=directory("active-renewal");var first=requests(node,plan,9000);var renewed=List.of(first.getFirst().successor(id(9001)));var group=first.getFirst().sourceGroup();
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)) {
            graph.force(plan,first);graph.force(plan,renewed);assertEquals(2,graph.snapshot().frozenRequests().size());assertEquals(renewed.getFirst().transferId(),graph.snapshot().activeRequests().get(group).transferId());
            assertThrows(IOException.class,()->graph.force(plan,first));assertThrows(IOException.class,()->graph.force(plan,List.of()));assertThrows(IOException.class,()->graph.force(plan,List.of(renewed.getFirst(),renewed.getFirst())));
        }
        var access=new FilesAccess();try(var graph=new RatisConfiguredRecoveryGraph(path,node,access,HEAP)) {
            assertEquals(renewed.getFirst().transferId(),graph.snapshot().activeRequests().get(group).transferId());assertEquals(2,graph.snapshot().frozenRequests().size());assertThrows(IOException.class,()->graph.force(plan,first));assertEquals(0,access.writes);
            access.after=true;assertThrows(IOException.class,()->graph.force(plan,renewed));access.after=false;int uncertainWrites=access.writes;
            assertThrows(IOException.class,()->graph.force(plan,first),"Identical retained history cannot change an uncertain active selection");assertEquals(uncertainWrites,access.writes);assertTrue(graph.retryPending());
            var next=List.of(renewed.getFirst().successor(id(9002)));access.after=true;assertThrows(IOException.class,()->graph.force(plan,next));assertEquals(renewed.getFirst().transferId(),graph.snapshot().activeRequests().get(group).transferId());access.after=false;assertTrue(graph.retryPending());assertEquals(next.getFirst().transferId(),graph.snapshot().activeRequests().get(group).transferId());
        }
        try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){assertEquals(id(9002),graph.snapshot().activeRequests().get(group).transferId());assertEquals(3,graph.snapshot().frozenRequests().size());}
    }

    @Test void malformedActiveSelectionCannotAuthorizeAGroupOnColdLoad()throws Exception {
        var node=node("data");var plan=discover(node,genesis,10);Path path=directory("active-frame");try(var graph=new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP)){graph.force(plan,requests(node,plan,9100));}
        byte[] original=SessionJournalFiles.read(path.resolve("graph"),RatisConfiguredRecoveryGraph.MAX_BYTES);
        for(int index:List.of(-1,1)) {var malformed=original.clone();java.nio.ByteBuffer.wrap(malformed).putInt(malformed.length-4,index);SessionJournalFiles.write(path.resolve("graph"),malformed,RatisConfiguredRecoveryGraph.MAX_BYTES);assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP));}
        var missing=Arrays.copyOf(original,original.length-4);java.nio.ByteBuffer.wrap(missing).putInt(missing.length-4,0);SessionJournalFiles.write(path.resolve("graph"),missing,RatisConfiguredRecoveryGraph.MAX_BYTES);assertThrows(IOException.class,()->new RatisConfiguredRecoveryGraph(path,node,RatisJournalAccess.standalone(),HEAP));
    }

}
