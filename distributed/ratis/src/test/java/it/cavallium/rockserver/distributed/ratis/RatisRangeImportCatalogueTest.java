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
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue CAS with configured controlled owner reads; no owner RPC/server/quorum claim. */
class RatisRangeImportCatalogueTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(long value) { return new UUID(650,value); }
    static ReplicaDescriptor owner(long value,ReplicaDescriptor.Role role) { return new ReplicaDescriptor(id(value),id(value+100),id(value+200),"owner"+value,"127.0.0.1:"+(9000+value),role); }
    static RangeImportDefinition definition(UUID cluster,UUID database,OperationId publication) {
        var owners=List.of(owner(1,ReplicaDescriptor.Role.VOTER),owner(2,ReplicaDescriptor.Role.LISTENER)); var range=new RangeDescriptor(id(10),id(11),2,3,null,null,RangeDescriptor.TopologyState.FENCED,owners);
        var directory=new RangeDirectory(cluster,database,id(20),1,new byte[32],7,Set.of(id(21)),0,List.of(range));
        var manifest=new NativeSstImportManifest(id(22),range.groupId(),range.rangeEpoch(),directory.domainId(),directory.schemaDigest(),null,null,List.of(new NativeSstImportManifest.Column(id(21),"imported",false,List.of())));
        return new RangeImportDefinition(id(22),publication,directory,List.of(manifest));
    }
    static RangeImportAdmission admission(RangeImportDefinition definition,long completed) {
        var entries=new ArrayList<RangeImportAdmission.OwnerReceipt>();
        for(var manifest:definition.manifests()) { var range=definition.sourceDirectory().ranges().stream().filter(r -> r.groupId().equals(manifest.groupId())).findFirst().orElseThrow(); var receipt=new NativeSstImportManifest.Receipt(definition.importId(),range.groupId(),range.rangeEpoch(),manifest.digest(),new ApplyPosition(1,3)); for(var owner:range.replicas()) entries.add(new RangeImportAdmission.OwnerReceipt(range.groupId(),range.rangeId(),range.configurationVersion(),owner,receipt,new ApplyPosition(2,completed))); }
        return new RangeImportAdmission(definition,entries);
    }
    final class Fixture implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
        final RatisSessionEpochsTest.Fixture nativeCatalogue=outer.new Fixture(root);
        final RatisRangeImportCatalogue.ImmutableReader immutable=key -> { try { return nativeCatalogue.store.readCurrentPoint(outer.directoryColumn,key); } catch(Exception failure) { throw new IllegalStateException(failure); } };
        final java.util.function.Consumer<DistributedCommand> pure=RatisCatalogue.validator(outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,command -> { throw new IllegalArgumentException("No operators in import fixture"); },immutable::read);
        final AtomicInteger ownerReads=new AtomicInteger(); final List<byte[]> publications=new ArrayList<>();
        boolean missingListener,missingActivationListener,loseReply;
        RangeImportActivationAdmission activations;
        final RaftClient client;
        final RatisRangeImportCatalogue catalogue;
        Fixture() throws Exception {
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                byte[] result;
                if(method.getName().equals("send")) {
                    byte[] encoded=((Message)args[0]).getContent().toByteArray(); var command=DistributedCommandCodec.decode(encoded); pure.accept(command);
                    if(RatisRangeImportCatalogue.isPublication(command)) {
                        RatisRangeImportCatalogue.admitPublication(command,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,immutable,(definition,range,owner) -> { ownerReads.incrementAndGet(); if(missingListener && owner.role()==ReplicaDescriptor.Role.LISTENER) return null; return admission(definition,10).owner(range.groupId(),owner.replicaId()); }); publications.add(encoded);
                    }
                    if(RatisRangeImportCatalogue.isCompletion(command)) {
                        RatisRangeImportCatalogue.admitCompletion(command,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,immutable,(definition,published,range,owner) -> {
                            ownerReads.incrementAndGet(); if(missingActivationListener && owner.role()==ReplicaDescriptor.Role.LISTENER) return null;
                            return activations.owners().stream().filter(entry -> entry.groupId().equals(range.groupId()) && entry.owner().equals(owner)).findFirst().orElseThrow();
                        });
                    }
                    result=nativeCatalogue.store.apply(new ApplyPosition(1,nativeCatalogue.next.getAndIncrement()),command);
                    if(loseReply && RatisRangeImportCatalogue.isPublication(command)) { loseReply=false; throw new IOException("Lost original publication reply"); }
                } else if(method.getName().equals("sendReadOnly")) {
                    var packet=ScopedTransactionRpc.parse(((Message)args[0]).getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES); assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var in=ByteBuffer.wrap(packet.body()); assertEquals(outer.directoryColumn,ScopedTransactionRpc.uuid(in)); byte[] key=ScopedTransactionRpc.blob(in,65536,false); assertFalse(in.hasRemaining()); byte[] value=immutable.read(key); var out=ByteBuffer.allocate(4+(value==null ? 0 : value.length)).putInt(value==null ? -1 : value.length); if(value!=null) out.put(value); result=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,out.array());
                } else throw new AssertionError(method);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue-fixture")).setGroupId(RaftGroupId.valueOf(outer.catalogue)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(result))).build();
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) { case "getGroupId" -> RaftGroupId.valueOf(outer.catalogue); case "io" -> io; default -> throw new AssertionError(method); }); catalogue=new RatisRangeImportCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
        }
        OperationId operation(long index) { return new OperationId(id(99),1,index); }
        void seed(RangeDirectory directory) throws Exception { var key=ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array(); var command=new DistributedCommand(outer.catalogue,1,new byte[32],operation(0),List.of(new Mutation(Mutation.Kind.PUT,outer.directoryColumn,key,RangeDirectoryCodec.encode(directory))),List.of(new KeyPredicate(outer.directoryColumn,key,null))); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(nativeCatalogue.store.apply(new ApplyPosition(1,nativeCatalogue.next.getAndIncrement()),command)).status()); }
        void stage(RangeImportDefinition definition,RangeImportAdmission admissions) throws Exception {
            long seq=1;
            for(int i=0;i<RatisRangeImportCatalogue.definitionArtifact(definition).chunks();i++) assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareDefinitionChunk(operation(seq++),definition,i)).status());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareDefinitionCommit(operation(seq++),definition)).status());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareBegin(operation(seq++),definition,null,null)).status());
            for(int i=0;i<RatisRangeImportCatalogue.admissionArtifact(admissions).chunks();i++) assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareAdmissionChunk(operation(seq++),definition,admissions,i)).status());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareAdmissionCommit(operation(seq),definition,admissions)).status());
        }
        public void close() throws Exception { nativeCatalogue.close(); }
    }
    @Test void nativeStagingBeginAllOwnerPublicationAndExactLostReplyRetry() throws Exception {
        try(var f=new Fixture()) {
            var definition=definition(f.outer.cluster,f.outer.database,f.operation(20)); var admissions=admission(definition,5); f.seed(definition.sourceDirectory()); f.stage(definition,admissions);
            assertEquals(0,f.ownerReads.get()); var prepared=f.catalogue.readHead(definition.sourceDirectory().domainId()); assertEquals(RatisRangeImportCatalogue.Phase.PREPARED,prepared.phase()); assertArrayEquals(definition.encode(),f.catalogue.readDefinition(definition.digest()).encode()); assertArrayEquals(admissions.encode(),f.catalogue.readAdmission(definition,admissions.digest()).encode());
            var publication=f.catalogue.preparePublication(definition,prepared,admissions,null); f.missingListener=true;
            assertThrows(IOException.class,() -> f.catalogue.execute(publication)); assertEquals(2,f.ownerReads.get()); assertEquals(RatisRangeImportCatalogue.Phase.PREPARED,f.catalogue.readImport(definition.importId()).phase());
            f.missingListener=false; f.loseReply=true; assertThrows(IOException.class,() -> f.catalogue.execute(publication)); assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,f.catalogue.readImport(definition.importId()).phase());
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(publication).status()); assertArrayEquals(f.publications.getFirst(),f.publications.getLast()); assertArrayEquals(RangeDirectoryCodec.encode(definition.targetDirectory()),f.immutable.read(ByteBuffer.allocate(16).putLong(definition.sourceDirectory().domainId().getMostSignificantBits()).putLong(definition.sourceDirectory().domainId().getLeastSignificantBits()).array()));
            int reads=f.ownerReads.get(); f.pure.accept(publication); nativeReplay(f,publication); assertEquals(reads,f.ownerReads.get(),"Deterministic replay never reads owners");
            var changed=new DistributedCommand(publication.groupId(),publication.rangeEpoch(),publication.schemaDigest(),f.operation(21),publication.mutations(),publication.predicates()); assertThrows(IllegalArgumentException.class,() -> f.pure.accept(changed));
            var altered=new ArrayList<>(publication.mutations()); altered.set(2,new Mutation(Mutation.Kind.PUT,f.outer.directoryColumn,altered.get(2).key(),RangeDirectoryCodec.encode(definition.sourceDirectory()))); var changedBytes=new DistributedCommand(publication.groupId(),publication.rangeEpoch(),publication.schemaDigest(),publication.operationId(),altered,publication.predicates()); assertThrows(IllegalArgumentException.class,() -> f.pure.accept(changedBytes));
            assertThrows(IllegalArgumentException.class,() -> RatisRangeImportCatalogue.validator(f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,null).accept(publication));
        }
    }
    @Test void orderedImportGuardBlocksTopologyAndRequiresQualifiedTerminalImportHead() throws Exception {
        try(var f=new Fixture()) {
            var definition=definition(f.outer.cluster,f.outer.database,f.operation(20)); var admissions=admission(definition,5); f.seed(definition.sourceDirectory());
            var ordinary=new RatisCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertThrows(IllegalArgumentException.class,() -> ordinary.prepare(f.operation(30),definition.sourceDirectory(),definition.targetDirectory()));
            var before=definition.sourceDirectory(); var fencedAdvance=new RangeDirectory(before.clusterId(),before.databaseId(),before.domainId(),before.schemaEpoch(),before.schemaDigest(),before.generation()+1,before.columns(),before.routingPrefixBytes(),before.ranges());
            var raced=ordinary.prepare(f.operation(31),before,fencedAdvance); f.stage(definition,admissions);
            assertEquals(CommandResult.Status.CONFLICT,ordinary.execute(raced).status(),"An import BEGIN winning after topology preparation is excluded by native predicate");
            var prepared=f.catalogue.readHead(before.domainId()); assertThrows(IllegalArgumentException.class,() -> ordinary.prepareAfterAdministration(f.operation(32),before,fencedAdvance,null,prepared));
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.preparePublication(definition,prepared,admissions,null)).status());
            var active=definition.targetDirectory(); var later=new RangeDirectory(active.clusterId(),active.databaseId(),active.domainId(),active.schemaEpoch(),active.schemaDigest(),active.generation()+1,active.columns(),active.routingPrefixBytes(),active.ranges());
            assertEquals(CommandResult.Status.CONFLICT,ordinary.execute(ordinary.prepare(f.operation(33),active,later)).status(),"Published import remains an explicit native expected-head guard");
            var published=f.catalogue.readHead(active.domainId());
            assertThrows(IllegalArgumentException.class,() -> ordinary.prepareAfterAdministration(f.operation(34),active,later,null,published),"Publication is not terminal while an owner may still be import-gated");
            byte[] originalPublished=f.catalogue.readImport(definition.importId()).encode();
            var activationOwners=new ArrayList<RangeImportActivationAdmission.OwnerActivation>();
            for(var installed:admissions.owners()) {
                var nativeActivation=new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport.Activation(installed.receipt(),f.outer.catalogue,new ApplyPosition(1,f.nativeCatalogue.next.get()-1),RangeImportAdmission.sha256(published.encode()),new ApplyPosition(2,6));
                var activation=RangeImportActivationAdmission.Activation.decode(nativeActivation.encode());
                activationOwners.add(new RangeImportActivationAdmission.OwnerActivation(installed.groupId(),installed.rangeId(),installed.configurationVersion(),installed.owner(),activation,new ApplyPosition(2,10)));
            }
            f.activations=new RangeImportActivationAdmission(definition,admissions,f.outer.catalogue,RangeImportAdmission.sha256(published.encode()),activationOwners);
            assertArrayEquals(f.activations.encode(),RangeImportActivationAdmission.decode(f.activations.encode(),definition,admissions,f.outer.catalogue,RangeImportAdmission.sha256(published.encode())).encode());
            assertThrows(IllegalArgumentException.class,() -> new RangeImportActivationAdmission(definition,admissions,f.outer.catalogue,RangeImportAdmission.sha256(published.encode()),List.of(activationOwners.getFirst())));
            long seq=40;
            for(int i=0;i<RatisRangeImportCatalogue.activationArtifact(f.activations).chunks();i++) assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.prepareActivationChunk(f.operation(seq++),definition,admissions,published,f.activations,i)).status());
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.prepareActivationCommit(f.operation(seq),definition,admissions,published,f.activations)).status());
            var completion=f.catalogue.prepareCompletion(f.operation(50),definition,admissions,published,f.activations,null);
            assertTrue(RatisRangeImportCatalogue.requiresOwnerAdmission(completion)); assertFalse(RatisRangeImportCatalogue.isPublication(completion));
            f.missingActivationListener=true; assertThrows(IOException.class,() -> f.catalogue.execute(completion));
            assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,f.catalogue.readHead(active.domainId()).phase());
            assertArrayEquals(originalPublished,f.catalogue.readImport(definition.importId()).encode());
            f.missingActivationListener=false; assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(completion).status());
            var completed=f.catalogue.readHead(active.domainId()); assertEquals(RatisRangeImportCatalogue.Phase.COMPLETE,completed.phase());
            assertArrayEquals(originalPublished,f.catalogue.readImport(definition.importId()).encode(),"Original publication authority remains immutable for delayed owner retries");
            int reads=f.ownerReads.get(); f.pure.accept(completion); nativeReplay(f,completion); assertEquals(reads,f.ownerReads.get(),"Completion replay never invokes configured owners");
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(completion).status(),"Original operation returns its retained completion result");
            assertEquals(CommandResult.Status.SUCCESS,ordinary.execute(ordinary.prepareAfterAdministration(f.operation(60),active,later,null,completed)).status());
            var missing=new ArrayList<>(raced.predicates()); missing.removeLast(); var bypass=new DistributedCommand(raced.groupId(),raced.rangeEpoch(),raced.schemaDigest(),f.operation(35),raced.mutations(),missing); assertThrows(IllegalArgumentException.class,() -> f.pure.accept(bypass));
        }
    }
    private static void nativeReplay(Fixture f,DistributedCommand publication) throws Exception { assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.nativeCatalogue.store.apply(new ApplyPosition(1,f.nativeCatalogue.next.getAndIncrement()),publication)).status()); }
}
