package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native metadata workflow with separately configured owner evidence; no DATA/server/quorum acceptance claim. */
class RatisTopologyCatalogueWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisRangeImportCatalogueTest outer=new RatisRangeImportCatalogueTest();
        final RatisRangeImportCatalogueTest.Fixture nativeOwner;
        final RatisTopologyCatalogue catalogue;
        final TopologyChange.Definition definition;
        final AtomicInteger reads=new AtomicInteger();
        final Map<String,RatisTopologyCatalogue.QualifiedOwner> actual=new HashMap<>();
        final List<byte[]> inputs=new ArrayList<>();
        long sequence=10;
        boolean loseReply;
        Fixture() throws Exception {
            outer.root=root; nativeOwner=outer.new Fixture(); var helper=new RatisTopologyCatalogueTest();
            var source=helper.source(nativeOwner); definition=helper.split(source,1,128);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if(method.getName().equals("sendReadOnly")) return nativeOwner.client.io().sendReadOnly((Message)args[0]);
                if(!method.getName().equals("send")) throw new AssertionError(method);
                var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray()); nativeOwner.pure.accept(command);
                boolean execution=RatisTopologyCatalogue.requiresOwnerAdmission(command); inputs.add(DistributedCommandCodec.encode(command));
                if(execution && nativeOwner.nativeCatalogue.store.operationResult(command.operationId(),command.payloadDigest())==null) admit(command,(d,proof) -> { reads.incrementAndGet(); assertArrayEquals(definition.encode(),d.encode()); return actual.get(key(proof)); });
                var result=nativeOwner.client.io().send((Message)args[0]);
                if(loseReply && execution) { loseReply=false; throw new IOException("Lost original topology progress reply"); }
                return result;
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> {
                if(method.getName().equals("getGroupId")) return nativeOwner.client.getGroupId(); if(method.getName().equals("io")) return io; throw new AssertionError(method);
            });
            catalogue=new RatisTopologyCatalogue(client,nativeOwner.outer.cluster,nativeOwner.outer.database,nativeOwner.outer.catalogue,1,new byte[32],nativeOwner.outer.directoryColumn);
            if(nativeOwner.nativeCatalogue.store.applied()!=null) nativeOwner.nativeCatalogue.next.set(nativeOwner.nativeCatalogue.store.applied().index()+1);
            if(catalogue.readDefinition(definition.digest())==null) {
                nativeOwner.seed(source); catalogue.execute(catalogue.prepareDefinition(nativeOwner.operation(1),definition,null));
                catalogue.execute(catalogue.prepareBegin(nativeOwner.operation(2),definition,null,null,null));
                catalogue.execute(catalogue.prepareFencing(nativeOwner.operation(3),definition,catalogue.readHead(source.domainId()),null,null));
            }
        }
        String key(TopologyChange.Proof proof) { return proof.stage()+"/"+proof.groupId()+"/"+proof.owner().replicaId(); }
        void admit(DistributedCommand command,RatisTopologyCatalogue.OwnerProofReader owner) throws IOException { RatisTopologyCatalogue.admitExecution(command,nativeOwner.outer.cluster,nativeOwner.outer.database,nativeOwner.outer.catalogue,1,new byte[32],nativeOwner.outer.directoryColumn,nativeOwner.immutable::read,owner); }
        OperationId operation() { return nativeOwner.operation(sequence++); }
        TopologyChange.State state() throws Exception { return catalogue.readHead(definition.header().domainId()); }
        void provide(TopologyChange.Proof proof,ApplyPosition cut) { actual.put(key(proof),new RatisTopologyCatalogue.QualifiedOwner(proof,cut)); }
        TopologyChange.Proof proof(TopologyChange.Stage stage,TopologyChange.State previous,TopologyChange.Participant participant,TopologyChange.Owner owner) {
            byte[] artifact=new byte[32]; artifact[0]=(byte)(stage.ordinal()+1);
            var predecessor=stage==TopologyChange.Stage.FREEZE ? definition.header().sourceDirectoryDigest() : stage==TopologyChange.Stage.SEED ? previous.freezeDigest() : previous.seeded().get(new TopologyChange.Key(participant.groupId(),owner.replicaId())).digest();
            return TopologyChange.Proof.of(stage,definition.header(),participant.groupId(),owner.replicaId(),stage==TopologyChange.Stage.SEED ? new ApplyPosition(0,0) : new ApplyPosition(1,5),artifact,predecessor);
        }
        void progress(TopologyChange.Stage stage) throws Exception {
            for(var participant:stage==TopologyChange.Stage.FREEZE ? definition.header().parents() : definition.header().targets()) for(var owner:participant.owners()) {
                var previous=state(); var proof=proof(stage,previous,participant,owner); provide(proof,new ApplyPosition(1,5));
                assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareProof(operation(),definition,previous,proof)).status());
            }
        }
        RangeDirectory directory() { byte[] bytes=nativeOwner.immutable.read(ByteBuffer.allocate(16).putLong(definition.header().domainId().getMostSignificantBits()).putLong(definition.header().domainId().getLeastSignificantBits()).array()); return RangeDirectoryCodec.decode(bytes); }
        @Override public void close() throws Exception { nativeOwner.close(); }
    }
    @Test void qualifiedProgressPublishesAtomicallyAndLostReplyReplaysOriginalThenReopensActive() throws Exception {
        TopologyChange.State active;
        try(var f=new Fixture()) {
            var previous=f.state(); var participant=f.definition.header().parents().getFirst(); var owner=participant.owners().getFirst(); var proof=f.proof(TopologyChange.Stage.FREEZE,previous,participant,owner);
            var command=f.catalogue.prepareProof(f.operation(),f.definition,previous,proof); assertTrue(RatisTopologyCatalogue.requiresOwnerAdmission(command)); assertEquals(proof,RatisTopologyCatalogue.executionProof(command));
            assertThrows(IOException.class,()->f.admit(command,null)); assertEquals(previous,f.state());
            f.provide(proof,new ApplyPosition(1,5)); f.loseReply=true; byte[] original=DistributedCommandCodec.encode(command);
            assertThrows(IOException.class,()->f.catalogue.execute(command)); int reads=f.reads.get(); assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(command).status()); assertEquals(reads,f.reads.get());
            assertArrayEquals(original,f.inputs.get(f.inputs.size()-1));
            // Complete any additional configured parent owners without synthesizing their evidence from the first row.
            for(var parent:f.definition.header().parents()) for(var member:parent.owners()) if(!f.state().frozen().containsKey(new TopologyChange.Key(parent.groupId(),member.replicaId()))) {
                var nextProof=f.proof(TopologyChange.Stage.FREEZE,f.state(),parent,member); f.provide(nextProof,new ApplyPosition(1,5)); f.catalogue.execute(f.catalogue.prepareProof(f.operation(),f.definition,f.state(),nextProof));
            }
            f.progress(TopologyChange.Stage.SEED); assertEquals(TopologyChange.Phase.SEEDED,f.state().phase());
            var publication=f.catalogue.preparePublication(f.operation(),f.definition,f.state(),null,null); assertEquals(3,publication.mutations().size()); assertFalse(RatisTopologyCatalogue.publicationProofs(publication).isEmpty());
            var before=f.state(); f.actual.clear(); assertThrows(IOException.class,()->f.catalogue.execute(publication)); assertEquals(before,f.state()); assertArrayEquals(f.definition.sourceDirectory().payloadDigest(),f.directory().payloadDigest());
            for(var seeded:before.seeded().values()) f.provide(seeded,new ApplyPosition(0,0));
            assertThrows(IOException.class,()->f.catalogue.execute(publication)); assertEquals(before,f.state());
            for(var seeded:before.seeded().values()) f.provide(seeded,new ApplyPosition(1,0));
            f.loseReply=true; assertThrows(IOException.class,()->f.catalogue.execute(publication)); assertEquals(TopologyChange.Phase.PUBLISHED,f.state().phase()); assertArrayEquals(f.definition.targetDirectory().payloadDigest(),f.directory().payloadDigest());
            reads=f.reads.get(); assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(publication).status()); assertEquals(reads,f.reads.get());
            f.progress(TopologyChange.Stage.ACTIVATE); active=f.state(); assertEquals(TopologyChange.Phase.ACTIVE,active.phase());
            assertArrayEquals(f.definition.targetDirectory().payloadDigest(),f.directory().payloadDigest());
            var retire=f.catalogue.prepareRetirement(f.operation(),f.definition,active,null,null);
            assertEquals(2,retire.mutations().size()); assertEquals(6,retire.predicates().size());
            assertDoesNotThrow(()->f.nativeOwner.pure.accept(retire));
            f.actual.clear(); assertThrows(IOException.class,()->f.catalogue.execute(retire)); assertEquals(active,f.state());
            var firstTarget=active.header().targets().getFirst().groupId();
            for(var activationProof:active.activated().values()) if(activationProof.groupId().equals(firstTarget)) f.provide(activationProof,new ApplyPosition(1,5));
            assertThrows(IOException.class,()->f.catalogue.execute(retire)); assertEquals(active,f.state());
            f.actual.clear();
            for(var activationProof:active.activated().values()) f.provide(activationProof,new ApplyPosition(0,0));
            assertThrows(IOException.class,()->f.catalogue.execute(retire)); assertEquals(active,f.state());
            for(var activationProof:active.activated().values()) f.provide(activationProof,new ApplyPosition(1,5));
            f.loseReply=true; assertThrows(IOException.class,()->f.catalogue.execute(retire));
            reads=f.reads.get(); f.actual.clear(); assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(retire).status()); assertEquals(reads,f.reads.get());
            active=active.retire(); assertEquals(active,f.state());
            assertArrayEquals(f.definition.encode(),f.catalogue.readDefinition(f.definition.digest()).encode());

        }
        try(var reopened=new Fixture()) { assertEquals(active,reopened.state()); assertArrayEquals(reopened.definition.targetDirectory().payloadDigest(),reopened.directory().payloadDigest()); }
    }
    @Test void wrongNativeOwnerOrUncommittedCutCannotAdvanceAndPureReplayDoesNotReadOwners() throws Exception {
        try(var f=new Fixture()) {
            var state=f.state(); var participant=f.definition.header().parents().getFirst(); var owner=participant.owners().getFirst(); var proof=f.proof(TopologyChange.Stage.FREEZE,state,participant,owner);
            var command=f.catalogue.prepareProof(f.operation(),f.definition,state,proof);
            assertDoesNotThrow(()->f.nativeOwner.pure.accept(command)); assertEquals(0,f.reads.get());
            assertThrows(IOException.class,()->f.admit(command,(d,expected) -> new RatisTopologyCatalogue.QualifiedOwner(expected,new ApplyPosition(0,0))));
            var changed=TopologyChange.Proof.of(proof.stage(),f.definition.header(),proof.groupId(),proof.owner().replicaId(),proof.position(),new byte[32],proof.predecessorDigest());
            assertThrows(IOException.class,()->f.admit(command,(d,expected) -> new RatisTopologyCatalogue.QualifiedOwner(changed,new ApplyPosition(1,5))));
            assertEquals(state,f.state()); assertArrayEquals(f.definition.sourceDirectory().payloadDigest(),f.directory().payloadDigest());
        }
    }
}
