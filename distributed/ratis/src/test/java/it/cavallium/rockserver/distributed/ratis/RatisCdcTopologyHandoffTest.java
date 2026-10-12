package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled transports with actual native terminal/child artifacts and actual catalogue CAS, no sockets. */
class RatisCdcTopologyHandoffTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisTopologyControllerTest helper=new RatisTopologyControllerTest();
        final RatisTopologyControllerTest.Fixture topology;
        final RatisRoutedClient routed;
        final RaftClient client;
        final Map<String,List<byte[]>> commands=new HashMap<>();
        final Set<String> lose=new HashSet<>();
        boolean ownersUnavailable;
        RatisNodeRuntime lateNode; RatisRoutedClient lateRouted;
        final RatisNodeRuntimeTest.FakeServer lateServer=new RatisNodeRuntimeTest.FakeServer();
        @FunctionalInterface interface Race { void run() throws Exception; }
        Race beforeHandoffPrepare=()->{};
        Fixture(boolean merge) throws Exception { this(merge,false); }
        Fixture(boolean merge,boolean listener) throws Exception {
            helper.root=root; topology=helper.new Fixture(root.resolve("native"),merge,listener);
            var source=topology.definition.sourceDirectory();
            routed=new RatisRoutedClient(source,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor range,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor range,byte[] wire,boolean fresh) throws IOException {
                    assertTrue(fresh); var request=RatisCdcPollRpc.decodeRequest(wire);
                    try {
                        var sourceStore=topology.store(range.groupId()); var page=sourceStore.topologyRetired()
                                ? sourceStore.pollRetiredCdc(request.binding().lineage(),request.from(),request.maxRecords(),request.maxBytes()-RatisCdcPollRpc.REPLY_BASE_BYTES)
                                : sourceStore.pollCdc(request.binding().lineage(),request.from(),request.maxRecords(),request.maxBytes()-RatisCdcPollRpc.REPLY_BASE_BYTES);
                        return RatisCdcPollRpc.reply(new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,page.cut(),page.floor(),page.next(),page.fromVerified(),0,page.records()));
                    } catch(ReplicatedRangeStore.CdcGap gap) { var p=gap.page(); return RatisCdcPollRpc.reply(new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.GAP,p.cut(),p.floor(),request.from(),false,0,List.of())); }
                    catch(ReplicatedRangeStore.CdcTooLarge large) { var p=large.page(); return RatisCdcPollRpc.reply(new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.TOO_LARGE,p.cut(),p.floor(),request.from(),p.fromVerified(),RatisCdcPollRpc.REPLY_BASE_BYTES+large.requiredPayloadBytes(),List.of())); }
                    catch(Exception error) { throw new IOException(error); }
                }
            });
            var authority=new RatisCdcDeliveryAuthority(routed,domain->source,(definition,lineage)->topology.definition.targetDirectory().ranges().stream().anyMatch(r->r.groupId().equals(lineage.groupId())) ? topology.definition.targetDirectory() : source,(definition,expected)->{
                if(ownersUnavailable) throw new IOException("Original native owners unavailable");
                var range=(expected.stage()==TopologyChange.Stage.FREEZE ? definition.parents() : definition.targets()).stream().filter(r->r.groupId().equals(expected.groupId())).findFirst().orElseThrow(); var evidence=topology.proof(definition,expected.stage(),range,topology.local); return new RatisTopologyCatalogue.QualifiedOwner(evidence.proof(),evidence.completedCut());
            },header->{header.requireDefinition(topology.definition);return topology.definition;});
            byte[][] prepared={null}; var store=topology.env.catalogue.store; var binding=topology.plan.catalogue();
            var validator=RatisCdcSubscriptionCatalogue.validator(source.clusterId(),source.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                byte[] bytes=((Message)args[0]).getContent().toByteArray();
                if(method.getName().equals("sendReadOnly")) {
                    var packet=ScopedTransactionRpc.parse(bytes,RatisCdcDeliveryRpc.MAX_BYTES);
                    if(packet.kind()==RatisCdcDeliveryRpc.PREPARE) { var command=RatisCdcDeliveryRpc.decode(bytes); validator.accept(command); if(RatisCdcSubscriptionCatalogue.isLineageTransition(command)) beforeHandoffPrepare.run(); if(store.operationResult(command.operationId(),command.payloadDigest())==null) authority.validate(command); prepared[0]=DistributedCommandCodec.encode(command); return topology.reply(binding.groupId(),bytes); }
                    assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var in=ByteBuffer.wrap(packet.body()); assertEquals(binding.directoryColumn(),ScopedTransactionRpc.uuid(in)); byte[] key=ScopedTransactionRpc.blob(in,65536,false); assertFalse(in.hasRemaining()); byte[] value=store.readCurrentPoint(binding.directoryColumn(),key); var out=ByteBuffer.allocate(4+(value==null ? 0 : value.length)).putInt(value==null ? -1 : value.length); if(value!=null) out.put(value); return topology.reply(binding.groupId(),ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,out.array()));
                }
                assertEquals("send",method.getName()); var command=DistributedCommandCodec.decode(bytes); String action="RECEIPT";
                if(!command.mutations().isEmpty()) { validator.accept(command); var next=DistributedCdcSubscription.decode(command.mutations().getFirst().value()); action=command.mutations().size()==2 ? "CREATE" : command.predicates().size()==3 ? "HANDOFF" : RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command) ? "DELIVER" : next.phase()==DistributedCdcSubscription.Phase.CLOSED ? "CLOSE" : "ACK"; if(RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command)) { assertArrayEquals(bytes,prepared[0]); prepared[0]=null; } }
                commands.computeIfAbsent(action,k->new ArrayList<>()).add(bytes.clone()); byte[] result=store.apply(new ApplyPosition(1,topology.env.catalogue.next.getAndIncrement()),command); if(lose.remove(action)) throw new IOException("Lost committed "+action+" response"); return topology.reply(binding.groupId(),result);
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;default->throw new AssertionError(method);});
        }
        DistributedCdcSubscription.Definition definition() {
            var directory=topology.definition.sourceDirectory(); var starts=new TreeMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>(); for(var range:directory.ranges()) starts.put(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.beginning());
            return DistributedCdcSubscription.Definition.create(new UUID(176,1),new UUID(176,2),directory,directory.columns(),new DistributedCdcCheckpoint(starts));
        }
        RatisCdcSubscriptionController controller(Path journal) throws IOException { var d=topology.definition.sourceDirectory(); return new RatisCdcSubscriptionController(journal,d.clusterId(),d.databaseId(),topology.plan.catalogue(),client,routed); }
        void activate() throws Exception { var journal=Files.createDirectory(root.resolve("topology-journal")); try(var controller=topology.controller(journal)) { var handle=controller.prepare(topology.definition); helper.until(controller,handle,journal,RatisTopologyController.Phase.ACTIVE); } }
        void put(RangeDescriptor range,int key,int value,int sequence) throws Exception { var store=topology.store(range.groupId()); var command=new DistributedCommand(range.groupId(),range.rangeEpoch(),topology.definition.header().schemaDigest(),new OperationId(new UUID(176,3),0,sequence),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{(byte)key},new byte[]{(byte)value}))); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status()); topology.server.machines.get(RaftGroupId.valueOf(range.groupId())).notifyTermIndexUpdated(1,store.applied().index()); }
        RatisTopologyOwnerRpc.Evidence actualLateListenerSeed() throws Exception {
            var definition=topology.definition; var source=definition.sourceDirectory(); var parent=definition.parents().getFirst(); var listener=parent.replicas().stream().filter(r->r.role()==ReplicaDescriptor.Role.LISTENER).findFirst().orElseThrow();
            var original=topology.plan.catalogue(); var binding=new RatisNodePlan.Catalogue(original.groupId(),original.epoch(),original.schemaDigest(),original.directoryColumn(),original.sessionsColumn(),List.of(topology.local));
            var plan=new RatisNodePlan(listener.nodeId(),listener.nodeIncarnation(),source,binding,topology.plan.columns(),Set.of(),16);
            lateRouted=new RatisRoutedClient(source,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError();}public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError();}});
            lateNode=RatisNodeRuntime.open(plan,Files.createDirectory(root.resolve("listener")),ConfigParser.parseDefault(),topology.env.catalogue.epochs,lateRouted,topology.env.scheduler,topology.env.timer,()->20L,topology.env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,lateServer::create,RatisTopologyControllerTest.logicalSchema());
            RatisTopologySeedRuntimeTest.field(lateNode,"seedAuthority",(RatisNodeRuntime.SeedAuthorityReader)topology::authority); lateNode.start();
            var machine=lateServer.machines.get(RaftGroupId.valueOf(parent.groupId())); var store=RatisTopologySeedRuntimeTest.field(machine,"store",ReplicatedRangeStore.class);
            var user=new DistributedCommand(parent.groupId(),parent.rangeEpoch(),definition.header().schemaDigest(),new OperationId(new UUID(176,40),0,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologyControllerTest.COLUMN,new byte[]{1},new byte[]{42})));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,1),user)).status()); var originalFreeze=topology.store(parent.groupId()).topologyFreeze();
            try(var gate=store.prepareTopologyFreeze(definition,originalFreeze.admissionDigest())) { assertTrue(gate.state().ready()); assertEquals(originalFreeze,store.applyTopologyFreeze(originalFreeze.position(),definition,originalFreeze.admissionDigest())); } machine.notifyTermIndexUpdated(1,originalFreeze.position().index());
            var seed=lateNode.seedTopology(new RatisTopologySeedRpc.Request(definition,binding.groupId(),parent.groupId(),listener.replicaId())); var child=definition.targets().getFirst(); var target=new RatisTopologyOwnerRpc.Target(definition,TopologyChange.Stage.SEED,binding.groupId(),child.groupId(),listener.replicaId());
            var evidence=RatisTopologyOwnerRpc.decodeReply(lateServer.machines.get(RaftGroupId.valueOf(child.groupId())).queryStale(RatisTopologyControllerTest.message(RatisTopologyOwnerRpc.request(target)),0).get(5,TimeUnit.SECONDS).getContent().toByteArray(),target); assertEquals(seed.proof(child.groupId()),evidence.proof()); return evidence;
        }
        public void close() throws Exception { if(lateNode!=null) lateNode.close(); if(lateRouted!=null) lateRouted.close(); routed.close(); topology.close(); }
    }
    @Test void actualSplitAndMergeDeliverTerminalThenExplicitAckHandoffAndEachChildUserOnce() throws Exception {
        for(boolean merge:List.of(false,true)) {
            Path caseRoot=Files.createDirectory(root.resolve("case-"+merge)); var owner=new RatisCdcTopologyHandoffTest(); owner.root=caseRoot;
            try(var f=owner.new Fixture(merge)) {
                Path journal=Files.createDirectory(caseRoot.resolve("cdc")); try(var controller=f.controller(journal)) {
                    var handle=controller.create(f.definition(),f.topology.definition.sourceDirectory()); f.activate();
                    for(int i=0;i<f.topology.definition.parents().size();i++) {
                        var poll=controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES); assertTrue(poll.batch().result().records().stream().anyMatch(r->r.kind()==DistributedCdcRecord.Kind.TERMINAL_BARRIER)); assertTrue(poll.batch().projected().envelopes().stream().anyMatch(e->e.terminalBarrier()!=null && e.events().isEmpty())); assertEquals(poll.batch().id(),controller.poll(handle,1,RatisCdcPollRpc.REPLY_BASE_BYTES+1).batch().id());
                        controller.acknowledge(handle,poll.batch().id()); if(i+1<f.topology.definition.parents().size()) assertEquals(f.topology.definition.parents().size(),handle.state().checkpoint().cursors().size());
                    }
                    assertEquals(f.topology.definition.targets().size(),handle.state().currentStarts().cursors().size()); assertNotNull(handle.state().latestHandoff());
                    int sequence=0; for(var range:f.topology.definition.targets()) f.put(range,range.lowerInclusive()==null ? 1 : 200,70+sequence,sequence++);
                    for(int i=0;i<f.topology.definition.targets().size();i++) { var poll=controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES); assertEquals(1,poll.batch().result().records().size()); assertEquals(DistributedCdcRecord.Kind.MUTATIONS,poll.batch().result().records().getFirst().kind()); controller.acknowledge(handle,poll.batch().id()); }
                    assertTrue(controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES).batch().result().records().isEmpty());
                }
            }
        }
    }
    @Test void lostAckAndHandoffReopenOriginalBatchAndCommandWithoutRequalifyingSavedReceipt() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("cdc")); UUID batch;
            try(var controller=f.controller(journal)) { var handle=controller.create(f.definition(),f.topology.definition.sourceDirectory()); f.activate(); batch=controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES).batch().id(); f.lose.add("ACK"); assertThrows(IOException.class,()->controller.acknowledge(handle,batch)); }
            try(var controller=f.controller(journal)) { var handle=controller.reopen(f.definition().subscriptionId()); f.lose.add("HANDOFF"); assertThrows(IOException.class,()->controller.acknowledge(handle,batch)); assertArrayEquals(f.commands.get("ACK").getFirst(),f.commands.get("ACK").getLast()); }
            f.ownersUnavailable=true;
            try(var controller=f.controller(journal)) { var handle=controller.reopen(f.definition().subscriptionId()); var state=controller.acknowledge(handle,batch); assertEquals(2,state.currentStarts().cursors().size()); assertArrayEquals(f.commands.get("HANDOFF").getFirst(),f.commands.get("HANDOFF").getLast()); long generation=state.generation(); assertEquals(generation,controller.acknowledge(handle,batch).generation()); }
        }
    }    @Test void childRetentionExpiryBeforeParentAckStillHandoffsAtOriginalActivationThenReportsGap() throws Exception {
        try(var f=new Fixture(true)) {
            Path journal=Files.createDirectory(root.resolve("cdc")); try(var controller=f.controller(journal)) {
                var handle=controller.create(f.definition(),f.topology.definition.sourceDirectory()); f.activate(); var child=f.topology.definition.targets().getFirst(); var nativeStore=f.topology.store(child.groupId()); int sequence=0;
                for(int i=0;i<4100;i++) {
                    f.put(child,1,i,sequence++);
                    if(i%256==255) { var ack=new DistributedCommand(child.groupId(),child.rangeEpoch(),f.topology.definition.header().schemaDigest(),new OperationId(new UUID(176,3),0,sequence),List.of(),List.of(),sequence-1); sequence++; assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(nativeStore.apply(new ApplyPosition(1,nativeStore.applied().index()+1),ack)).status()); f.topology.server.machines.get(RaftGroupId.valueOf(child.groupId())).notifyTermIndexUpdated(1,nativeStore.applied().index()); }
                }
                for(int i=0;i<2;i++) { var poll=controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES); controller.acknowledge(handle,poll.batch().id()); }
                var start=handle.state().currentStarts().cursors().values().iterator().next(); assertEquals(new ApplyPosition(1,1),start.position());
                var gap=controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES); assertEquals(RatisCdcPollRpc.Outcome.GAP,gap.outcome()); assertNull(gap.batch()); assertFalse(start.covers(gap.floor())); assertEquals(start,handle.state().checkpoint().cursors().values().iterator().next());
            }
        }
    }
    @Test void actualLateListenerProofConflictsOnlyOriginalHandoffThenNewOperationKeepsCutsAndPollsChild() throws Exception {
        try(var f=new Fixture(false,true)) {
            Path journal=Files.createDirectory(root.resolve("cdc")); try(var controller=f.controller(journal)) {
                var handle=controller.create(f.definition(),f.topology.definition.sourceDirectory()); f.activate(); var original=f.topology.catalogue.readIntent(f.topology.definition.intentId()); var late=f.actualLateListenerSeed();
                f.beforeHandoffPrepare=()->{
                    f.beforeHandoffPrepare=()->{}; var next=original.seeded(late.proof());
                    var command=f.topology.catalogue.prepareProof(new OperationId(new UUID(176,41),0,0),f.topology.definition,original,late.proof(),null,null);
                    RatisTopologyCatalogue.admitExecution(command,f.topology.definition.header().clusterId(),f.topology.definition.header().databaseId(),f.topology.plan.catalogue().groupId(),1,new byte[32],f.topology.plan.catalogue().directoryColumn(),key->{try{return f.topology.env.catalogue.store.readCurrentPoint(f.topology.plan.catalogue().directoryColumn(),key);}catch(Exception failure){throw new IllegalStateException(failure);}},(definition,expected)->new RatisTopologyCatalogue.QualifiedOwner(late.proof(),late.completedCut()));
                    assertEquals(CommandResult.Status.SUCCESS,f.topology.catalogue.execute(command).status()); assertEquals(next,f.topology.catalogue.readIntent(f.topology.definition.intentId())); f.lose.add("RECEIPT");
                };
                var poll=controller.poll(handle,256,RatisCdcPollRpc.MAX_REPLY_BYTES); assertThrows(IOException.class,()->controller.acknowledge(handle,poll.batch().id())); assertEquals(1,handle.state().currentStarts().cursors().size(),"Known stale-guard CAS had no effect"); controller.close();
                // Fresh optional proof metadata may change; original child frontier must not.
                var child=f.topology.definition.targets().getLast(); f.put(child,200,77,0); try(var resumed=f.controller(journal)) { var recovered=resumed.reopen(handle.subscriptionId()); var following=resumed.poll(recovered,256,RatisCdcPollRpc.MAX_REPLY_BYTES); assertEquals(2,recovered.state().currentStarts().cursors().size()); assertEquals(1,following.batch().result().records().size()); }
                var inputs=f.commands.get("HANDOFF"); assertEquals(2,inputs.size()); var first=DistributedCommandCodec.decode(inputs.getFirst()); var second=DistributedCommandCodec.decode(inputs.getLast()); assertNotEquals(first.operationId(),second.operationId()); assertArrayEquals(first.mutations().getFirst().value(),second.mutations().getFirst().value()); assertFalse(Arrays.equals(first.predicates().get(2).expectedValue(),second.predicates().get(2).expectedValue()));
                assertTrue(RatisCdcSubscriptionCatalogue.requalifiesTopologyGuard(first,2,second.predicates().get(2).expectedValue())); assertFalse(RatisCdcSubscriptionCatalogue.requalifiesTopologyGuard(first,0,second.predicates().get(2).expectedValue())); assertFalse(RatisCdcSubscriptionCatalogue.requalifiesTopologyGuard(first,2,TopologyChange.State.begin(original.header()).encode()));
            }
        }
    }

}
