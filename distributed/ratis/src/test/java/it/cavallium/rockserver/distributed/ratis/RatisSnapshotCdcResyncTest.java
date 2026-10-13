package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Native snapshot/poll/catalogue/destination composition. Controlled dispatch, no sockets or external consumer. */
class RatisSnapshotCdcResyncTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(20,TimeUnit.SECONDS);}
    static final byte[] CHECKPOINT={0,1},COMPLETE={0,2},INVALID={0,3};
    final class Source implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f;
        final DistributedSchema schema;final UUID column=new UUID(960,1);
        final RaftClient catalogue;final RatisRoutedClient poller;final RatisNodePlan.Catalogue binding;
        final Set<String> lose=new HashSet<>();final Map<String,List<byte[]>> commands=new HashMap<>();
        final List<byte[]> polls=new ArrayList<>();boolean losePoll;long sequence=1500;byte[] prepared;
        Source(Path path)throws Exception {
            var identity=new RatisSessionEpochsTest();
            f=new RatisAtomicClientTest.Fixture(path,true,Map.of(),null,(route,machine)->machine.cdcBinding(identity.cluster,identity.database,route.rangeId()));
            byte[] jar=RatisMergeFrontendTest.jar();var operator=new DistributedSchema.Operator("resync-append",1,RatisMergeFrontendTest.AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
            DistributedStoragePool.stageOperator(path.resolve("catalogue/catalogue"),operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
            var shape=ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true,operator.name(),operator.version(),operator.className());
            var columns=new ArrayList<>(f.schema.columns());columns.add(new DistributedSchema.Column(75,column,"resync-merge",shape,operator));schema=new DistributedSchema(f.schema.clusterId(),f.schema.databaseId(),f.schema.domainId(),2,1,columns);
            var bootstrap=new RatisSchemaAdministrationTest();bootstrap.sequence.set(100);
            bootstrap.publishSchema(f.catalogue,f.schema);bootstrap.publishSchema(f.catalogue,schema);
            var directories=f.catalogue.epochs.catalogue(f.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(50),null,f.directory)).status());
            var roster=new RatisDatabaseDomainRoster(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertEquals(Set.of(schema.domainId()),Set.copyOf(roster.read().domainIds()));
            var definition=new SchemaChange.Definition(new UUID(960,2),new OperationId(f.admin,1,75),f.directory,f.schema,schema);
            var admin=bootstrap.admin(bootstrap.client(f.catalogue));var head=bootstrap.begin(admin,definition,null);
            var bindings=new ArrayList<SchemaAdmissionManifest.Binding>();var owners=new ArrayList<SchemaAdmissionManifest.ReplicaAdmission>();var reservations=new HashMap<UUID,NativeSchemaCapacityReservation>();
            for(var range:f.directory.ranges()){var store=f.stores.get(range.groupId());var names=new HashMap<>(f.catalogue.pool.schemaPhysicalNames(store));names.put(column,"resync-merge");var reservation=f.catalogue.pool.reserveSchemaCapacity(store,definition,names,Set.of(column));reservations.put(range.groupId(),reservation);bindings.add(new SchemaAdmissionManifest.Binding(range.groupId(),"resync-merge",true));for(var owner:range.replicas())owners.add(new SchemaAdmissionManifest.ReplicaAdmission(range.groupId(),owner.replicaId(),reservation));}
            var admissions=new SchemaAdmissionManifest(definition,bindings,owners);
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(admin.prepareAdmissions(bootstrap.operation(),head,definition,admissions)).status());
            assertEquals(CommandResult.Status.SUCCESS,admin.executeFencing(admin.prepareFencing(bootstrap.operation(),head,definition,admissions)).status());head=admin.readHead(schema.domainId());
            var fences=new LinkedHashMap<UUID,ApplyPosition>();var installs=new LinkedHashMap<UUID,ApplyPosition>();
            for(var group:f.groups){var store=f.stores.get(group);long index=store.applied().index();var fence=f.catalogue.pool.applySchemaFence(store,new ApplyPosition(1,++index),definition.header(),reservations.get(group),admissions.digest());fences.put(group,fence.position());var names=new HashMap<>(f.catalogue.pool.schemaPhysicalNames(store));names.put(column,"resync-merge");var installed=f.catalogue.pool.installSchema(store,new ApplyPosition(1,++index),definition,names,Set.of(column));installs.put(group,installed.position());}
            for(var entry:fences.entrySet()){assertEquals(CommandResult.Status.SUCCESS,admin.execute(admin.prepareAdvance(bootstrap.operation(),head,head.status().fenced(entry.getKey(),entry.getValue()))).status());head=admin.readHead(schema.domainId());}
            for(var entry:installs.entrySet()){assertEquals(CommandResult.Status.SUCCESS,admin.execute(admin.prepareAdvance(bootstrap.operation(),head,head.status().installed(entry.getKey(),entry.getValue()))).status());head=admin.readHead(schema.domainId());}
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(admin.preparePublication(bootstrap.operation(),head,definition)).status());
            NativeSchemaActivationProof activation=null;
            for(var group:f.groups){var store=f.stores.get(group);var record=new RatisSchemaActivateRecord(definition.header(),group,installs.get(group),f.outer.catalogue,f.catalogue.store.applied());var context=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(f.machines.get(group)).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).setLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(store.applied().index()+1).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(record.encode()))).build()).build();activation=NativeSchemaActivationProof.decode(await(f.machines.get(group).applyTransaction(context)).getContent().toByteArray());}
            f.data.refreshSchema(definition,activation);assertArrayEquals(f.data.directory().payloadDigest(),roster.readComplete().directories().get(schema.domainId()).payloadDigest());
            binding=new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,f.directory.ranges().getFirst().replicas());
            poller=new RatisRoutedClient(f.data.directory(),new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return f.data.read(column,key);}
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{throw new IOException("Poll-only fixture route");}
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{assertTrue(fresh);try{byte[] reply=f.machines.get(route.groupId()).queryLinearizable(Message.valueOf(ByteString.copyFrom(request))).get(20,TimeUnit.SECONDS).getContent().toByteArray();var decoded=RatisCdcPollRpc.decodeRequest(request);if(decoded.maxBytes()>RatisCdcPollRpc.REPLY_BASE_BYTES+1){polls.add(request.clone());if(losePoll){losePoll=false;throw new IOException("Lost native poll reply");}}return reply;}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}}
            });
            var authority=new RatisCdcDeliveryAuthority(Map.of(schema.domainId(),poller),domain->f.data.directory());
            var actual=f.catalogue.client;var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                byte[] bytes=((Message)args[0]).getContent().toByteArray();
                if(method.getName().equals("sendReadOnly")){var packet=ScopedTransactionRpc.parse(bytes,RatisCdcDeliveryRpc.MAX_BYTES);if(packet.kind()==RatisCdcDeliveryRpc.PREPARE){var command=RatisCdcDeliveryRpc.decode(bytes);authority.validate(command);prepared=DistributedCommandCodec.encode(command);return reply(bytes);}if(packet.kind()==RatisCdcDeliveryRpc.CANCEL){RatisCdcDeliveryRpc.decode(bytes);prepared=null;return reply(bytes);}}
                String action=null;if(method.getName().equals("send")){var command=DistributedCommandCodec.decode(bytes);if(!command.mutations().isEmpty()){action=command.mutations().size()==2?"CREATE":RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command)?"DELIVER":"ACK";commands.computeIfAbsent(action,k->new ArrayList<>()).add(bytes.clone());if(action.equals("DELIVER")){assertArrayEquals(prepared,bytes);prepared=null;}}}
                Object result;try{result=method.invoke(actual.io(),args);}catch(InvocationTargetException failure){throw failure.getCause();}if(action!=null && lose.remove(action))throw new IOException("Lost applied "+action+" reply");return result;
            });
            catalogue=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->actual.getGroupId();case "io"->io;default->throw new AssertionError(method);});
        }
        RaftClientReply reply(byte[] bytes){return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("resync-fixture")).setGroupId(f.catalogue.client.getGroupId()).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();}
        DistributedCdcCheckpoint beginning(){var cursors=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();for(var range:f.data.directory().ranges())cursors.put(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.beginning());return new DistributedCdcCheckpoint(cursors);}
        DistributedCdcSubscription.Definition definition(int n,DistributedCdcCheckpoint start){return DistributedCdcSubscription.Definition.create(new UUID(961,n),new UUID(962,n),f.data.directory(),List.of(column),start);}
        RatisCdcSubscriptionController controller(Path path)throws IOException{return new RatisCdcSubscriptionController(path,schema.clusterId(),schema.databaseId(),binding,catalogue,poller);}
        void write(int prefix,Mutation.Kind kind,byte[] bytes)throws Exception{var route=f.data.directory().route(column,new byte[]{(byte)prefix});var command=new DistributedCommand(route.groupId(),route.rangeEpoch(),schema.digest(),new OperationId(f.session,1,sequence++),List.of(new Mutation(kind,column,new byte[]{(byte)prefix},bytes)));assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.restoreSessionCommand(f.data.directory(),command,route)).status());}
        void overtake()throws Exception{byte[] large=new byte[1024*1024];for(var route:f.data.directory().ranges())for(int n=0;n<70;n++){var command=new DistributedCommand(route.groupId(),route.rangeEpoch(),schema.digest(),new OperationId(f.session,1,sequence++),List.of(new Mutation(Mutation.Kind.PUT,schema.column(71).columnId(),new byte[]{(byte)(route.lowerInclusive()==null?3:201)},large)));assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.restoreSessionCommand(f.data.directory(),command,route)).status());}}
        RatisSnapshotClient snapshotClient(){return new RatisSnapshotClient(schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults());}
        RatisSnapshotClient.Lease snapshot(RatisSnapshotClient client,int block)throws Exception{var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,null,null)));var lease=client.open(f.root.resolve("resync-snapshot-"+block),RatisSnapshotClientTest.snapshotIdentity(f,block),scope,Duration.ofSeconds(60),RequestContext.batch());await(lease.completion());return lease;}
        DistributedCdcCheckpoint starts(RatisSnapshotClient.Lease lease)throws IOException{var cursors=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();for(var participant:lease.snapshot().participants()){var range=participant.route();cursors.put(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.after(participant.captureToken().position()));}return new DistributedCdcCheckpoint(cursors);}
        public void close()throws Exception{poller.close();f.close();}
    }
    static final class Destination implements AutoCloseable {
        final RatisMergeFrontendTest.AppendOperator operator=new RatisMergeFrontendTest.AppendOperator("resync-destination");final Options options=new Options().setCreateIfMissing(true).setMergeOperator(operator);final RocksDB db;final WriteOptions sync=new WriteOptions().setSync(true).setDisableWAL(false);
        Destination(Path path)throws Exception{db=RocksDB.open(options,path.toString());}
        void snapshot(Map<Integer,byte[]> values,DistributedCdcCheckpoint starts)throws Exception{try(var batch=new WriteBatch()){for(var row:values.entrySet())batch.put(new byte[]{row.getKey().byteValue()},row.getValue());batch.put(CHECKPOINT,starts.encode());batch.put(COMPLETE,starts.encode());db.write(sync,batch);}}
        DistributedCdcCheckpoint checkpoint()throws Exception{byte[] value=db.get(CHECKPOINT);return value==null?null:DistributedCdcCheckpoint.decode(value);}
        void apply(RatisCdcSubscriptionBatch delivered)throws Exception{assertNotNull(db.get(COMPLETE),"Snapshot marker must be forced before applying tail");var previous=checkpoint();var cursor=previous.cursors().get(delivered.lineage());try(var batch=new WriteBatch()){for(var envelope:delivered.envelopes())for(var event:envelope.events())if(!cursor.covers(event.cursor())){var mutation=event.mutation();switch(mutation.kind()){case PUT->batch.put(mutation.key(),mutation.value());case MERGE->batch.merge(mutation.key(),mutation.value());case DELETE->batch.delete(mutation.key());default->throw new AssertionError(mutation.kind());}}var next=cursor.covers(delivered.next())?previous:previous.advance(delivered.lineage(),delivered.next());batch.put(CHECKPOINT,next.encode());db.write(sync,batch);}}
        public void close(){db.close();sync.close();options.close();operator.close();}
    }
    @Test void gapResyncUsesNativeSnapshotCutsAndAtomicDestinationCheckpointAcrossLostReplies()throws Exception {
        try(var source=new Source(root.resolve("source"))){
            Path oldJournal=Files.createDirectory(root.resolve("old"));var old=source.definition(1,source.beginning());
            try(var controller=source.controller(oldJournal)){var handle=controller.create(old,source.f.data.directory());source.write(2,Mutation.Kind.PUT,new byte[]{1});source.write(200,Mutation.Kind.PUT,new byte[]{10});source.overtake();var gap=controller.poll(handle,256,8*1024*1024);assertEquals(RatisCdcPollRpc.Outcome.GAP,gap.outcome());assertEquals(old.start(),handle.state().checkpoint());assertNull(gap.batch());}
            source.write(2,Mutation.Kind.MERGE,new byte[]{2});source.write(200,Mutation.Kind.MERGE,new byte[]{20});Path journal=Files.createDirectory(root.resolve("resync")),destination=root.resolve("destination");DistributedCdcSubscription.Definition fresh;int captures;
            try(var snapshots=source.snapshotClient();var lease=source.snapshot(snapshots,0)){
                var starts=source.starts(lease);fresh=source.definition(2,starts);assertNotEquals(old.subscriptionId(),fresh.subscriptionId());assertNotEquals(old.incarnationId(),fresh.incarnationId());
                source.write(2,Mutation.Kind.MERGE,new byte[]{3});source.write(200,Mutation.Kind.MERGE,new byte[]{30});
                var values=Map.of(2,await(lease.get(75,key(2),RequestContext.batch())).toByteArray(),200,await(lease.get(75,key(200),RequestContext.batch())).toByteArray());assertArrayEquals(new byte[]{1,2},values.get(2));assertArrayEquals(new byte[]{10,20},values.get(200));
                try(var staged=new Destination(destination)){staged.snapshot(values,starts);assertEquals(starts,staged.checkpoint());assertArrayEquals(starts.encode(),staged.db.get(COMPLETE));}
                await(lease.release(RequestContext.batch()));captures=source.f.captureRequests.get();
            }
            UUID batchId;
            try(var controller=source.controller(journal)){var handle=controller.create(fresh,source.f.data.directory());source.losePoll=true;assertThrows(IOException.class,()->controller.poll(handle,256,8*1024*1024));}
            try(var controller=source.controller(journal)){var handle=controller.reopen(fresh.subscriptionId());source.lose.add("DELIVER");assertThrows(IOException.class,()->controller.poll(handle,256,8*1024*1024));}
            try(var controller=source.controller(journal);var target=new Destination(destination)){var handle=controller.reopen(fresh.subscriptionId());var delivered=controller.poll(handle,256,8*1024*1024).batch();batchId=delivered.id();target.apply(delivered);assertEquals(fresh.start(),handle.state().checkpoint());}
            try(var controller=source.controller(journal);var target=new Destination(destination)){var handle=controller.reopen(fresh.subscriptionId());var duplicate=controller.poll(handle,256,8*1024*1024).batch();assertEquals(batchId,duplicate.id());target.apply(duplicate);source.lose.add("ACK");assertThrows(IOException.class,()->controller.acknowledge(handle,batchId));}
            try(var controller=source.controller(journal);var target=new Destination(destination)){var handle=controller.reopen(fresh.subscriptionId());controller.acknowledge(handle,batchId);for(int n=0;n<4;n++){var delivered=controller.poll(handle,256,8*1024*1024).batch();target.apply(delivered);controller.acknowledge(handle,delivered.id());}assertArrayEquals(source.f.data.read(source.column,new byte[]{2}),target.db.get(new byte[]{2}));assertArrayEquals(source.f.data.read(source.column,new byte[]{(byte)200}),target.db.get(new byte[]{(byte)200}));assertArrayEquals(new byte[]{1,2,3},target.db.get(new byte[]{2}));assertArrayEquals(new byte[]{10,20,30},target.db.get(new byte[]{(byte)200}));assertEquals(captures,source.f.captureRequests.get());}
            var deliveries=source.commands.get("DELIVER");assertArrayEquals(deliveries.get(0),deliveries.get(1));var acks=source.commands.get("ACK");assertArrayEquals(acks.get(0),acks.get(1));assertArrayEquals(source.polls.get(1),source.polls.get(2));
        }
    }
    @Test void retentionOvertakesStagingAttemptWithoutPublishingSnapshotOrAdvancingAck()throws Exception {
        try(var source=new Source(root.resolve("overtaken"))){
            source.write(2,Mutation.Kind.PUT,new byte[]{1});
            try(var snapshots=source.snapshotClient();var lease=source.snapshot(snapshots,0)){
                var start=source.starts(lease);var definition=source.definition(3,start);Path journal=Files.createDirectory(root.resolve("overtaken-journal"));
                try(var controller=source.controller(journal);var staging=new Destination(root.resolve("invalid-staging"))){
                    var handle=controller.create(definition,source.f.data.directory());staging.db.put(staging.sync,new byte[]{2},await(lease.get(75,key(2),RequestContext.batch())).toByteArray());assertArrayEquals(new byte[]{1},staging.db.get(new byte[]{2}));
                    source.overtake();assertEquals(RatisCdcPollRpc.Outcome.GAP,controller.poll(handle,256,8*1024*1024).outcome());assertEquals(start,handle.state().checkpoint());
                    try(var invalidate=new WriteBatch()){invalidate.delete(new byte[]{2});invalidate.put(INVALID,start.encode());staging.db.write(staging.sync,invalidate);}
                    assertArrayEquals(start.encode(),staging.db.get(INVALID));assertNull(staging.db.get(new byte[]{2}));assertNull(staging.db.get(COMPLETE));assertNull(staging.checkpoint());assertFalse(source.commands.containsKey("DELIVER"));await(lease.release(RequestContext.batch()));
                }
            }
        }
    }
}
