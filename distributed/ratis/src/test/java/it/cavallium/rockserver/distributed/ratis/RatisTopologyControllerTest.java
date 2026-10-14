package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual local native/Ratis adapters and catalogue CAS. Transport boundaries are controlled, with no sockets/quorum claim. */
class RatisTopologyControllerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final UUID COLUMN=new UUID(2,3);
    static DistributedSchema logicalSchema() {
        var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);
        return new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,1,List.of(new DistributedSchema.Column(1,COLUMN,"data",shape,null)));
    }
    static UUID id(int n) { return new UUID(175,n); }
    static Message message(byte[] value) { return Message.valueOf(ByteString.copyFrom(value)); }
    final class Fixture implements RatisTopologyController.DataPlane,AutoCloseable {
        final RatisNodeRuntimeTest.Env env;
        final RatisNodeRuntimeTest.FakeServer server=new RatisNodeRuntimeTest.FakeServer();
        final RatisNodePlan plan;
        final RatisRoutedClient routed;
        final RatisNodeRuntime node;
        TopologyChange.Definition definition;
        final ReplicaDescriptor local;
        final RaftClient client;
        final RatisTopologyCatalogue catalogue;
        final Set<String> loseCatalogue=new HashSet<>();
        final Map<String,List<byte[]>> catalogueInputs=new HashMap<>();
        final List<byte[]> freezeInputs=new ArrayList<>(),seedInputs=new ArrayList<>(),activationInputs=new ArrayList<>();
        boolean loseFreeze,loseSeed,loseActivation,unavailableLocalProof,minorityAvailable;
        it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool minorityPool;
        final Map<UUID,ReplicatedRangeStore> minorityStores=new HashMap<>();
        Exception minorityFailure;
        @FunctionalInterface interface Observer { void run() throws IOException; }
        Observer beforeFreeze=()->{};
        Fixture(Path path,boolean merge,boolean listener) throws Exception {
            var schema=logicalSchema(); env=new RatisNodeRuntimeTest.Env(path,false,schema.digest());
            RatisNodeRuntime opened=null; RatisRoutedClient route=null;
            try {
            var original=env.plan.domains().getFirst().directory().ranges().getFirst(); local=original.replicas().stream().filter(r->r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
            var replicas=new ArrayList<ReplicaDescriptor>(List.of(local)); if(listener) { var remote=original.replicas().stream().filter(r->!r.nodeId().equals(local.nodeId())).findFirst().orElseThrow(); replicas.add(new ReplicaDescriptor(remote.nodeId(),remote.nodeIncarnation(),remote.replicaId(),remote.peerId(),remote.address(),ReplicaDescriptor.Role.LISTENER)); }
            var parents=merge ? List.of(new RangeDescriptor(id(1),new UUID(3,1),1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,replicas),new RangeDescriptor(id(2),new UUID(3,2),1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,replicas)) : List.of(new RangeDescriptor(id(1),new UUID(3,1),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,replicas));
            var source=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),1,schema.digest(),7,schema.columnIds(),schema.routingPrefixBytes(),parents);
            var children=merge ? List.of(new RangeDescriptor(id(10),id(11),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,replicas)) : List.of(new RangeDescriptor(id(10),id(11),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,replicas),new RangeDescriptor(id(12),id(13),1,0,new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,replicas));
            var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),1,source.schemaDigest(),8,source.columns(),source.routingPrefixBytes(),children); definition=new TopologyChange.Definition(id(20),source,target);
            plan=new RatisNodePlan(env.plan.nodeId(), env.plan.nodeIncarnation(), List.of(new RatisNodePlan.Domain(source, env.plan.columns(), Set.of())), env.plan.catalogue(), List.of(), List.of(), Map.of(), 16,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(source, env.plan.columns(), Set.of()))),RatisPoolFixtures.groups(env.plan.catalogue(),List.of()));
            route=routed=new RatisRoutedClient(source,new RatisRoutedClient.Transport() { public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); } public CommandResult command(RangeDescriptor range,DistributedCommand command) { throw new AssertionError(); } });
            opened=node=RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(routed.directory().domainId(), routed), env.scheduler, env.timer, ()->20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, server::create, List.of(schema));
            RatisTopologySeedRuntimeTest.field(node,"seedAuthority",(RatisNodeRuntime.SeedAuthorityReader)this::authority); bindReaders(); node.start();
            var metadata=env.catalogue.store; byte[] domain=ByteBuffer.allocate(16).putLong(source.domainId().getMostSignificantBits()).putLong(source.domainId().getLeastSignificantBits()).array();
            metadata.apply(new ApplyPosition(1,env.catalogue.next.getAndIncrement()),new DistributedCommand(env.outer.catalogue,1,new byte[32],new OperationId(id(90),0,0),List.of(new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,domain,RangeDirectoryCodec.encode(source)))));
            for(int i=0;i<parents.size();i++) {
                var parent=parents.get(i); var store=store(parent.groupId()); byte[] key={(byte)(i==0 ? 1 : 200)};
                var put=new DistributedCommand(parent.groupId(),1,schema.digest(),new OperationId(id(91+i),0,0),List.of(new Mutation(Mutation.Kind.PUT,COLUMN,key,new byte[]{(byte)(42+i)})));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,1),put)).status()); server.machines.get(RaftGroupId.valueOf(parent.groupId())).notifyTermIndexUpdated(1,1);
            }
            var validator=RatisCatalogue.validator(source.clusterId(),source.databaseId(),env.outer.catalogue,1,new byte[32],env.outer.directoryColumn,env.outer.sessions,command->{throw new IllegalArgumentException("No operators");},key->{try { return metadata.readCurrentPoint(env.outer.directoryColumn,key); } catch(Exception error) { throw new IllegalStateException(error); }});
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                byte[] bytes=((Message)args[0]).getContent().toByteArray();
                if(method.getName().equals("sendReadOnly")) {
                    if(ScopedTransactionRpc.kind(bytes,RatisTopologyExecutionRpc.MAX_BYTES)==RatisTopologyExecutionRpc.PREPARE) {
                        var command=RatisTopologyExecutionRpc.decode(bytes); validator.accept(command);
                        if(metadata.operationResult(command.operationId(),command.payloadDigest())==null) RatisTopologyCatalogue.admitExecution(command,source.clusterId(),source.databaseId(),env.outer.catalogue,1,new byte[32],env.outer.directoryColumn,key->{try{return metadata.readCurrentPoint(env.outer.directoryColumn,key);}catch(Exception error){throw new IllegalStateException(error);}},(d,expected)->{
                            var range=(expected.stage()==TopologyChange.Stage.FREEZE ? d.parents() : d.targets()).stream().filter(r->r.groupId().equals(expected.groupId())).findFirst().orElseThrow(); var owner=range.replicas().stream().filter(r->r.replicaId().equals(expected.owner().replicaId())).findFirst().orElseThrow(); var actual=proof(d,expected.stage(),range,owner); return new RatisTopologyCatalogue.QualifiedOwner(actual.proof(),actual.completedCut());
                        });
                        return reply(env.outer.catalogue,bytes);
                    }
                    var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES); assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var in=ByteBuffer.wrap(packet.body()); assertEquals(env.outer.directoryColumn,ScopedTransactionRpc.uuid(in)); byte[] key=ScopedTransactionRpc.blob(in,65536,false); assertFalse(in.hasRemaining());
                    byte[] value=metadata.readCurrentPoint(env.outer.directoryColumn,key); assertTrue(value==null || value.length<=ScopedTransactionRpc.MAX_REPLY_BYTES-ScopedTransactionRpc.HEADER_BYTES-4); var out=ByteBuffer.allocate(4+(value==null ? 0 : value.length)).putInt(value==null ? -1 : value.length); if(value!=null) out.put(value); return reply(env.outer.catalogue,ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,out.array()));
                }
                if(!method.getName().equals("send")) throw new AssertionError(method); var command=DistributedCommandCodec.decode(bytes); validator.accept(command); String action=action(command); catalogueInputs.computeIfAbsent(action,k->new ArrayList<>()).add(bytes.clone());
                byte[] result=metadata.apply(new ApplyPosition(1,env.catalogue.next.getAndIncrement()),command); if(loseCatalogue.remove(action)) throw new IOException("Lost original catalogue "+action+" reply"); return reply(env.outer.catalogue,result);
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(env.outer.catalogue);case "io"->io;default->throw new AssertionError(method);}); catalogue=new RatisTopologyCatalogue(client,source.clusterId(),source.databaseId(),env.outer.catalogue,1,new byte[32],env.outer.directoryColumn);
            } catch(Exception | Error failure) {
                if(opened!=null) try { opened.close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); }
                if(route!=null) try { route.close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); }
                try { env.close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        String action(DistributedCommand command) { if(command.mutations().isEmpty()) return "ACK"; if(command.predicates().size()==3) return "DEFINITION"; if(command.predicates().getFirst().expectedValue()==null) return "BEGIN"; var old=TopologyChange.State.decode(command.predicates().getFirst().expectedValue()); if(old.phase()==TopologyChange.Phase.PREPARED) return "FENCING"; if(command.mutations().size()==3) return "PUBLISH"; if(TopologyChange.State.decode(command.mutations().getFirst().value()).phase()==TopologyChange.Phase.RETIRED) return "RETIRE"; return RatisTopologyCatalogue.executionProof(command).stage().name(); }
        void bindReaders() throws Exception { for(var machine:server.machines==null ? List.<RatisRangeStateMachine>of() : server.machines.values()) RatisTopologySeedRuntimeTest.field(machine,"topologyAuthority",(RatisRangeStateMachine.TopologyAuthorityReader)this::authority); }
        ReplicatedRangeStore store(UUID group) throws Exception { return RatisTopologySeedRuntimeTest.field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class); }
        RaftClientReply reply(UUID group,byte[] bytes) { return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setSuccess(true).setMessage(message(bytes)).build(); }
        RaftClient parentClient(UUID group) {
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                var machine=server.machines.get(RaftGroupId.valueOf(group)); var wire=(Message)args[0];
                if(method.getName().equals("sendReadOnly")) return reply(group,machine.queryLinearizable(wire).get(5,TimeUnit.SECONDS).getContent().toByteArray());
                if(!method.getName().equals("send")) throw new AssertionError(method);
                var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(wire).build()); assertNull(transaction.getException()); machine.preAppendTransaction(transaction); transaction.initLogEntry(1,store(group).applied().index()+1);
                var applied=machine.applyTransaction(transaction);
                // The controlled transport is this fixture's updater; pump the production native continuation it staged.
                machine.committedApplyAdmission().pump();
                byte[] result=applied.get(5,TimeUnit.SECONDS).getContent().toByteArray();
                machine.topologyCleanup().get(5,TimeUnit.SECONDS); return reply(group,result);
            });
            return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(group);case "io"->io;default->throw new AssertionError(method);});
        }
        public NativeTopologyFreeze freeze(RatisTopologyFreezeRecord record) throws IOException {
            beforeFreeze.run(); freezeInputs.add(record.encode()); var transport=new RatisTopologyFreezeClient(parentClient(record.groupId())); transport.prepare(record); var freeze=transport.append(record); if(loseFreeze) { loseFreeze=false; throw new IOException("Lost committed parent FREEZE reply"); } return freeze;
        }
        public NativeTopologySeed seed(RatisTopologySeedRpc.Request request) throws IOException {
            seedInputs.add(RatisTopologySeedRpc.request(request));
            if(!request.owner().equals(local)) {
                if(!minorityAvailable) throw new IOException("Unready listener remains pending");
                try { return seedMinority(request); } catch(Exception error) { minorityFailure=error; throw new IOException(error); }
            }
            try { var seed=node.seedTopology(request); bindReaders(); if(loseSeed) { loseSeed=false; throw new IOException("Lost completed local SEED reply"); } return seed; }
            catch(IOException error) { throw error; } catch(Exception error) { throw new IOException(error); }
        }
        public RatisTopologyOwnerRpc.Evidence proof(TopologyChange.Definition d,TopologyChange.Stage stage,RangeDescriptor range,ReplicaDescriptor owner) throws IOException {
            if(!owner.equals(local)) {
                if(!minorityAvailable) throw new IOException("Actual addressed owner unavailable");
                try { return minorityProof(d,stage,range,owner); } catch(Exception error) { if(minorityFailure==null)minorityFailure=error; throw new IOException(error); }
            }
            if(unavailableLocalProof) throw new IOException("Actual addressed owner unavailable");
            var target=new RatisTopologyOwnerRpc.Target(d,stage,plan.catalogue().groupId(),range.groupId(),owner.replicaId());
            try { var machine=server.machines.get(RaftGroupId.valueOf(range.groupId())); if(machine==null) throw new IOException("Actual child division not registered"); var wire=machine.queryStale(message(RatisTopologyOwnerRpc.request(target)),0).get(5,TimeUnit.SECONDS); return RatisTopologyOwnerRpc.decodeReply(wire.getContent().toByteArray(),target); }
            catch(IOException error) { throw error; } catch(Exception error) { throw new IOException(error); }
        }
        NativeTopologySeed seedMinority(RatisTopologySeedRpc.Request request) throws Exception {
            if(minorityPool==null) minorityPool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(env.nodeRoot.resolve("controlled-listener-native"),ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()));
            var parents=new ArrayList<ReplicatedRangeStore>();
            for(var range:request.definition().parents()) {
                var parent=minorityStores.get(range.groupId());
                if(parent==null) {
                    parent=minorityPool.openRange(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Owner(range.groupId(),range.rangeEpoch()),new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(request.definition().header().domainId(),Set.of(COLUMN),range.lowerInclusive(),range.upperExclusive()),logicalSchema().digest(),Map.of(COLUMN,"data"),Set.of());
                    parent.enableLogicalSchema(logicalSchema()); parent.initializeConfiguration(store(range.groupId()).configuration().data()); parent.applyNoop(new ApplyPosition(1,0));
                    var original=new DistributedCommand(range.groupId(),range.rangeEpoch(),logicalSchema().digest(),new OperationId(id(91),0,0),List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{1},new byte[]{42})));
                    parent.apply(new ApplyPosition(1,1),original); minorityStores.put(range.groupId(),parent);
                }
                var freeze=store(range.groupId()).topologyFreeze();
                if(parent.topologyFreeze()==null) parent.applyTopologyFreeze(freeze.position(),request.definition(),freeze.admissionDigest());
                parents.add(parent);
            }
            var seed=minorityPool.seedTopology(request.definition(),authority(request.definition().header(),TopologyChange.Stage.SEED).state(),parents,request.owner());
            for(var range:request.definition().targets()) if(!minorityStores.containsKey(range.groupId())) {
                var child=minorityPool.reopenTopologyRange(seed.birth(range.groupId()),logicalSchema());
                child.initializeConfiguration(store(range.groupId()).configuration().data()); child.applyNoop(new ApplyPosition(1,0)); minorityStores.put(range.groupId(),child);
            }
            return seed;
        }
        RatisTopologyOwnerRpc.Evidence minorityProof(TopologyChange.Definition d,TopologyChange.Stage stage,RangeDescriptor range,ReplicaDescriptor replica) throws Exception {
            var seed=minorityPool==null ? null : minorityPool.topologySeed(d.intentId());
            if(seed==null) throw new IOException("Listener has no actual native seed");
            var child=minorityStores.get(range.groupId()); byte[] artifact; TopologyChange.Proof proof;
            if(stage==TopologyChange.Stage.SEED) { artifact=seed.encode(); proof=seed.birth(range.groupId()).proof(seed); }
            else if(stage==TopologyChange.Stage.ACTIVATE) {
                var activation=store(range.groupId()).topologyActivation();
                if(child.topologyActivation()==null) child.applyTopologyActivation(activation.position(),d,activation.commonFreezeDigest(),activation.catalogueGroup(),activation.catalogueCut(),activation.publicationCommitment());
                artifact=child.topologyActivation().encode(); proof=child.topologyActivation().proof();
            } else throw new IOException("Listener FREEZE proof is not queried in this controlled workflow");
            var configuration=child.configuration();
            return new RatisTopologyOwnerRpc.Evidence(new RatisTopologyOwnerRpc.Target(d,stage,plan.catalogue().groupId(),range.groupId(),replica.replicaId()),replica,proof,child.applied(),configuration.term(),configuration.index(),configuration.data(),child.applied().term(),artifact);
        }
        public RatisTopologyAuthority.View authority(TopologyChange.Header header,TopologyChange.Stage stage) throws IOException { return RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,definition.sourceDirectory().clusterId(),definition.sourceDirectory().databaseId(),RatisTopologyAuthority.Request.of(header,stage)); }
        public NativeTopologyActivation activate(RatisTopologyActivationRecord record) throws IOException { activationInputs.add(record.encode()); var transport=new RatisTopologyActivationClient(parentClient(record.groupId())); transport.prepare(record); var result=transport.append(record); if(loseActivation) { loseActivation=false; throw new IOException("Lost committed child ACTIVATE reply"); } return result; }
        RatisTopologyController controller(Path journal) throws IOException { return new RatisTopologyController(journal,definition.sourceDirectory().clusterId(),definition.sourceDirectory().databaseId(),plan.catalogue(),client,this); }
        public void close() throws Exception { node.close(); if(minorityPool!=null)minorityPool.close(); routed.close(); env.close(); }
    }
    void until(RatisTopologyController controller,RatisTopologyController.Handle handle,Path journal,RatisTopologyController.Phase phase) throws Exception {
        for(int i=0;i<200 && handle.phase()!=phase;i++) controller.advance(handle,1); assertEquals(phase,handle.phase());
        if(RatisSchemaClientJournal.read(journal.resolve(handle.intentId()+".topology")).containsKey("pendingAck")) controller.advance(handle,1);
    }
    @Test void executableLocalSplitAndMergeUseActualOwnerArtifactsThroughActivationAndBusinessData() throws Exception {
        for(boolean merge:List.of(false,true)) try(var f=new Fixture(root.resolve("workflow-"+merge),merge,true)) {
            Path journal=Files.createDirectory(root.resolve("journal-"+merge)); try(var controller=f.controller(journal)) {
                var handle=controller.prepare(f.definition); until(controller,handle,journal,RatisTopologyController.Phase.ACTIVE); var progress=controller.advance(handle,6); assertEquals(TopologyChange.Phase.ACTIVE,progress.state().phase()); assertTrue(progress.retirementBlocked()); assertTrue(progress.pendingOwners()>0,"Unready configured listener is not synthesized from the local voter");
                var current=new RatisCatalogue(f.client,f.definition.sourceDirectory().clusterId(),f.definition.sourceDirectory().databaseId(),f.plan.catalogue().groupId(),1,new byte[32],f.plan.catalogue().directoryColumn()).read(f.definition.header().domainId()); assertArrayEquals(f.definition.targetDirectory().payloadDigest(),current.payloadDigest());
                for(var child:f.definition.targets()) { var store=f.store(child.groupId()); assertFalse(store.topologyBirthPending()); assertTrue(store.admissionOpen()); }
                var first=f.definition.targets().getFirst(); var store=f.store(first.groupId()); assertArrayEquals(new byte[]{42},store.readCurrentPoint(COLUMN,new byte[]{1})); if(merge) assertArrayEquals(new byte[]{43},store.readCurrentPoint(COLUMN,new byte[]{(byte)200}));
                var put=new DistributedCommand(first.groupId(),first.rangeEpoch(),f.definition.header().schemaDigest(),new OperationId(id(99),0,0),List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{1},new byte[]{77}))); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),put)).status()); assertArrayEquals(new byte[]{77},store.readCurrentPoint(COLUMN,new byte[]{1}));
            }
            try(var reopened=f.controller(journal)) { var handle=reopened.reopen(f.definition.intentId()); assertEquals(TopologyChange.Phase.ACTIVE,reopened.advance(handle,3).state().phase()); }
        }
    }
    @Test void sameCatalogueSplitRetirementThenMergeRetainsReadonlyPinOriginalReceiptAndHistory() throws Exception {
        try(var f=new Fixture(root.resolve("repeated"),false,true)) {
            var split=f.definition; var parent=f.store(split.parents().getFirst().groupId());
            var original=new DistributedCommand(parent.ownership().owner().groupId(),1,split.header().schemaDigest(),new OperationId(id(91),0,0),List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{1},new byte[]{42})));
            byte[] receipt=parent.operationResult(original.operationId(),original.receiptDigest()); assertNotNull(receipt);
            Path journal=Files.createDirectory(root.resolve("repeated-journal"));
            try(var pin=parent.captureReadable(new TransactionScope(List.of(TransactionScope.Span.point(COLUMN,new byte[]{1}))))) {
                try(var controller=f.controller(journal)) {
                    var handle=controller.prepare(split); until(controller,handle,journal,RatisTopologyController.Phase.ACTIVE);
                    var blocked=controller.advance(handle,12); assertTrue(blocked.retirementBlocked()); assertTrue(blocked.pendingOwners()>0);
                    assertThrows(IllegalArgumentException.class,()->f.catalogue.prepareRetirement(new OperationId(id(34),0,0),split,handle.state(),null,null));
                    var active=handle.state(); var column=f.plan.catalogue().directoryColumn(); var header=active.header();
                    var bypass=new DistributedCommand(f.plan.catalogue().groupId(),1,new byte[32],new OperationId(id(35),0,0),List.of(new Mutation(Mutation.Kind.PUT,column,RatisTopologyCatalogue.stateKey(header.intentId()),active.retire().encode()),new Mutation(Mutation.Kind.PUT,column,RatisTopologyCatalogue.headKey(header.domainId()),active.retire().encode())),List.of(
                            new KeyPredicate(column,RatisTopologyCatalogue.stateKey(header.intentId()),active.encode()),new KeyPredicate(column,RatisTopologyCatalogue.headKey(header.domainId()),active.encode()),
                            new KeyPredicate(column,ByteBuffer.allocate(16).putLong(header.domainId().getMostSignificantBits()).putLong(header.domainId().getLeastSignificantBits()).array(),RangeDirectoryCodec.encode(split.targetDirectory())),
                            new KeyPredicate(column,RatisTopologyCatalogue.identityKey(header.intentId()),split.digest()),new KeyPredicate(column,RatisSchemaAdministration.headKey(header.domainId()),null),new KeyPredicate(column,RatisRangeImportCatalogue.headKey(header.domainId()),null)));
                    assertThrows(IllegalArgumentException.class,()->f.catalogue.execute(bypass));
                    f.minorityAvailable=true;
                    RatisTopologyController.Progress recovered=blocked;
                    for(int i=0;i<200 && recovered.pendingOwners()!=0;i++) recovered=controller.advance(handle,1);
                    assertEquals(0,recovered.pendingOwners(),()->"Listener recovery did not finish: "+f.minorityFailure);
                    until(controller,handle,journal,RatisTopologyController.Phase.ACTIVE);
                    assertEquals(TopologyChange.Phase.ACTIVE,handle.state().phase());
                    f.loseCatalogue.add("RETIRE"); assertThrows(IOException.class,()->controller.advance(handle,1));
                    assertEquals(TopologyChange.Phase.RETIRED,f.catalogue.readIntent(split.intentId()).phase());
                }
                try(var controller=f.controller(journal)) {
                    var handle=controller.reopen(split.intentId()); assertEquals(TopologyChange.Phase.RETIRED,controller.advance(handle,3).state().phase());
                    assertArrayEquals(f.catalogueInputs.get("RETIRE").getFirst(),f.catalogueInputs.get("RETIRE").getLast());
                    var source=split.targetDirectory(); var merged=new RangeDescriptor(id(30),id(31),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,source.ranges().getFirst().replicas());
                    var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),List.of(merged));
                    f.definition=new TopologyChange.Definition(id(32),source,target);
                    var merge=controller.prepare(f.definition); until(controller,merge,journal,RatisTopologyController.Phase.RETIRED);
                    assertEquals(TopologyChange.Phase.RETIRED,f.catalogue.readIntent(split.intentId()).phase());
                    assertEquals(f.definition.intentId(),f.catalogue.readHead(source.domainId()).header().intentId());
                    assertArrayEquals(split.encode(),f.catalogue.readDefinition(split.digest()).encode());
                    assertArrayEquals(new byte[]{42},f.store(merged.groupId()).readCurrentPoint(COLUMN,new byte[]{1}));
                    var write=new DistributedCommand(merged.groupId(),1,source.schemaDigest(),new OperationId(id(33),0,0),List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{1},new byte[]{77})));
                    assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store(merged.groupId()).apply(new ApplyPosition(1,f.store(merged.groupId()).applied().index()+1),write)).status());
                    assertArrayEquals(new byte[]{42},pin.get(COLUMN,new byte[]{1}));
                    assertArrayEquals(receipt,parent.operationResult(original.operationId(),original.receiptDigest()));
                    assertFalse(parent.userHandlesDetached()); assertEquals(1,parent.activeSnapshotPins());
                }
            }
            assertArrayEquals(receipt,parent.operationResult(original.operationId(),original.receiptDigest()));
        }
    }
    @Test void unknownDefinitionFreezePublicationAndActivationRepliesReopenExactInputsAndReceiptOwnership() throws Exception {
        try(var f=new Fixture(root.resolve("unknown"),false,false)) {
            Path journal=Files.createDirectory(root.resolve("journal"));
            try(var controller=f.controller(journal)) { var handle=controller.prepare(f.definition); f.loseCatalogue.add("DEFINITION"); assertThrows(IOException.class,()->controller.advance(handle,1)); }
            try(var controller=f.controller(journal)) { var handle=controller.reopen(f.definition.intentId()); until(controller,handle,journal,RatisTopologyController.Phase.FREEZE); assertArrayEquals(f.catalogueInputs.get("DEFINITION").getFirst(),f.catalogueInputs.get("DEFINITION").getLast()); f.loseFreeze=true; assertThrows(IOException.class,()->controller.advance(handle,1)); }
            try(var controller=f.controller(journal)) { var handle=controller.reopen(f.definition.intentId()); f.loseSeed=true; until(controller,handle,journal,RatisTopologyController.Phase.PUBLISH); assertArrayEquals(f.freezeInputs.getFirst(),f.freezeInputs.getLast()); assertArrayEquals(f.seedInputs.getFirst(),f.seedInputs.getLast()); f.loseCatalogue.add("PUBLISH"); assertThrows(IOException.class,()->controller.advance(handle,1)); }
            byte[] original;
            try(var controller=f.controller(journal)) { var handle=controller.reopen(f.definition.intentId()); until(controller,handle,journal,RatisTopologyController.Phase.ACTIVATE); assertArrayEquals(f.catalogueInputs.get("PUBLISH").getFirst(),f.catalogueInputs.get("PUBLISH").getLast()); f.loseActivation=true; assertThrows(IOException.class,()->controller.advance(handle,1)); original=f.activationInputs.getLast(); }
            try(var controller=f.controller(journal)) { var handle=controller.reopen(f.definition.intentId()); until(controller,handle,journal,RatisTopologyController.Phase.ACTIVE); assertArrayEquals(original,f.activationInputs.get(1)); assertEquals(TopologyChange.Phase.RETIRED,controller.advance(handle,2).state().phase()); }
        }
    }
    @Test void lostReceiptAckReopensExactIdentityAndDefiniteConflictNeverRebases() throws Exception {
        try(var f=new Fixture(root.resolve("conflict"),false,false)) {
            Path journal=Files.createDirectory(root.resolve("journal"));
            try(var controller=f.controller(journal)) { var handle=controller.prepare(f.definition); controller.advance(handle,1); f.loseCatalogue.add("ACK"); assertThrows(IOException.class,()->controller.advance(handle,1)); }
            try(var controller=f.controller(journal)) {
                var handle=controller.reopen(f.definition.intentId()); controller.advance(handle,1); assertArrayEquals(f.catalogueInputs.get("ACK").getFirst(),f.catalogueInputs.get("ACK").getLast());
                var directory=f.definition.targetDirectory(); byte[] key=ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array();
                f.env.catalogue.store.apply(new ApplyPosition(1,f.env.catalogue.next.getAndIncrement()),new DistributedCommand(f.plan.catalogue().groupId(),1,new byte[32],new OperationId(id(100),0,0),List.of(new Mutation(Mutation.Kind.PUT,f.plan.catalogue().directoryColumn(),key,RangeDirectoryCodec.encode(directory)))));
                assertEquals(RatisTopologyController.Phase.CONFLICT,controller.advance(handle,1).phase()); controller.advance(handle,1);
                int calls=f.catalogueInputs.get("BEGIN").size(); assertThrows(RatisTopologyController.Conflict.class,()->controller.advance(handle,1)); assertEquals(calls,f.catalogueInputs.get("BEGIN").size());
            }
            try(var controller=f.controller(journal)) { assertThrows(RatisTopologyController.Conflict.class,()->controller.advance(controller.reopen(f.definition.intentId()),1)); }
        }
    }
    @Test void blockedActualFreezeRetainsRootOwnershipAndDoesNotHoldControllerMonitor() throws Exception {
        try(var f=new Fixture(root.resolve("ownership"),false,false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); try(var controller=f.controller(journal)) {
                var handle=controller.prepare(f.definition); until(controller,handle,journal,RatisTopologyController.Phase.FREEZE); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var executor=Executors.newSingleThreadExecutor();
                f.beforeFreeze=()->{entered.countDown();try { if(!release.await(10,TimeUnit.SECONDS)) throw new IOException("Controlled native freeze wait timed out"); } catch(InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException(error); }};
                try { var work=executor.submit(()->controller.advance(handle,1)); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),()->{assertThrows(IOException.class,controller::close);assertThrows(IOException.class,handle::close);assertThrows(IOException.class,()->controller.reopen(handle.intentId()));}); assertThrows(IOException.class,()->f.controller(journal)); release.countDown(); work.get(5,TimeUnit.SECONDS); }
                finally { release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
            }
        }
    }
}
