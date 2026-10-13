package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Native HOME CAS and DATA gate adapters with real durable/cohort/export kernels; no Raft quorum or sockets. */
final class RatisDatabaseBackupFactoryFixture implements AutoCloseable,RatisDatabaseBackupRealCohortFixture.Control {
    boolean loseIssuance,loseHomeBinding,loseDataBinding;final List<ReplicatedRangeStore> bound=new ArrayList<>();final RatisDatabaseBackupRealCohortFixture real;final Catalogue catalogue;final SessionEpochLedger.Grant issued;
    final List<Home> homes=new ArrayList<>();final Map<UUID,RatisDatabaseBackupFactory.Domain> access=new HashMap<>();
    final Map<UUID,RatisDatabaseBackupFactory.Reservation> reservations=new HashMap<>();final DatabaseBackupDefinition definition;
    final class Catalogue implements AutoCloseable {
        final UUID group=UUID.randomUUID(),column=UUID.randomUUID();final DistributedStoragePool pool;final ReplicatedRangeStore store;final RatisSessionEpochs epochs;
        Catalogue()throws Exception{
            pool=DistributedStoragePool.open(real.base.root.resolve("native-catalogue"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(UUID.randomUUID(),Set.of(column),null,null),new byte[32],Map.of(column,"epochs"),Set.of());
            var io=(org.apache.ratis.client.api.BlockingApi)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args)->{
                byte[] bytes=((org.apache.ratis.protocol.Message)args[0]).getContent().toByteArray();byte[] reply;
                if(method.getName().equals("send")){var command=DistributedCommandCodec.decode(bytes);RatisSessionEpochs.validator(real.base.cluster,real.base.database,group,1,new byte[32],column).accept(command);reply=store.apply(store.applied()==null?new ApplyPosition(1,0):next(store),command);if(loseIssuance && !SessionEpochLedger.decode(command.mutations().getFirst().value()).grants().isEmpty()){loseIssuance=false;throw new IOException("Lost native issued-grant reply");}}
                else if(method.getName().equals("sendReadOnly")){var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);var in=ByteBuffer.wrap(packet.body());UUID requested=ScopedTransactionRpc.uuid(in);byte[] key=ScopedTransactionRpc.blob(in,1024,false);assertEquals(column,requested);try(var pin=store.capture()){byte[] value=pin.get(column,key);reply=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,ByteBuffer.allocate(4+(value==null?0:value.length)).putInt(value==null?-1:value.length).put(value==null?new byte[0]:value).array());}}
                else throw new AssertionError("Unexpected catalogue RPC");
                return new org.apache.ratis.protocol.RaftClientReply.Builder().setClientId(org.apache.ratis.protocol.ClientId.randomId()).setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf("native-cata")).setGroupId(org.apache.ratis.protocol.RaftGroupId.valueOf(group)).setCallId(1).setSuccess(true).setMessage(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(reply))).build();
            });
            var client=(org.apache.ratis.client.RaftClient)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->org.apache.ratis.protocol.RaftGroupId.valueOf(group);case "io"->io;default->throw new AssertionError("Unexpected borrowed client call");});
            epochs=new RatisSessionEpochs(client,real.base.cluster,real.base.database,group,1,new byte[32],column);
        }
        public void close() throws IOException {pool.close();}
    }
    final class Home implements AutoCloseable {
        final RatisDatabaseBackupDriverFixture.Pool data;final UUID column=UUID.randomUUID(),group=UUID.randomUUID();final DistributedStoragePool pool;
        final ReplicatedRangeStore store;final RatisRoutedClient routed;final RatisSnapshotFences fences;final SnapshotFence fence;
        Home(RatisDatabaseBackupRealCohortFixture.Endpoint endpoint,int index,SessionEpochLedger.Grant grant)throws Exception{
            data=endpoint.state;pool=DistributedStoragePool.open(real.base.root.resolve("factory-home-"+index),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(UUID.randomUUID(),Set.of(column),null,null),new byte[32],Map.of(column,"home"),Set.of());store.initializeConfiguration(new byte[]{1});store.applyNoop(new ApplyPosition(1,0));store.configureSnapshotHome(real.base.cluster,real.base.database,column);
            var range=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(data.owner));var directory=new RangeDirectory(real.base.cluster,real.base.database,store.ownership().interval().domainId(),0,new byte[32],0,Set.of(column),0,List.of(range));
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor route,UUID requested,byte[] key)throws IOException{try{assertEquals(column,requested);return value(key);}catch(Exception failure){throw new IOException(failure);}}
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{try{if(command.sessionControl()!=null)catalogue.epochs.sessionControlAdmission(directory,group).accept(command);else RatisSnapshotFences.validator(real.base.cluster,real.base.database,group,1,new byte[32],column).accept(command);var applied=CommandResult.decode(store.apply(next(store),command));if(command.sessionControl()!=null && loseHomeBinding){loseHomeBinding=false;throw new IOException("Lost native HOME binding reply");}return applied;}catch(Exception failure){throw new IOException(failure);}}
                public CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID requested,byte[] key)throws IOException{try{assertEquals(column,requested);return CompletableFuture.completedFuture(value(key));}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
                public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException{try{if(command.sessionControl()!=null)catalogue.epochs.sessionControlAdmission(directory,group).accept(command);else RatisSnapshotFences.validator(real.base.cluster,real.base.database,group,1,new byte[32],column).accept(command);var applied=CommandResult.decode(store.apply(next(store),command));if(command.sessionControl()!=null && loseHomeBinding){loseHomeBinding=false;throw new IOException("Lost native HOME binding reply");}return CompletableFuture.completedFuture(applied);}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
                public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{assertTrue(fresh);var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(packet.kind()!=ScopedTransactionRpc.SNAPSHOT_HOME_FENCE)throw new AssertionError("Unexpected HOME query");var identity=SnapshotHomeIdentity.decode(packet.body());return CompletableFuture.completedFuture(ScopedTransactionRpc.frame(packet.kind(),ByteBuffer.allocate(36).put(identity.digest()).putInt(-1).array()));}
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{assertTrue(fresh);var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(packet.kind()!=ScopedTransactionRpc.SNAPSHOT_HOME_FENCE)throw new AssertionError("Unexpected HOME query");var identity=SnapshotHomeIdentity.decode(packet.body());return ScopedTransactionRpc.frame(packet.kind(),ByteBuffer.allocate(36).put(identity.digest()).putInt(-1).array());}
            });fences=new RatisSnapshotFences(routed,column);
            long offset=1000L*index;var identity=new RatisSnapshotClient.Identity(UUID.randomUUID(),new OperationId(grant.sessionId(),grant.epoch(),offset),new OperationId(grant.sessionId(),grant.epoch(),offset+1));
            var plan=RatisSnapshotPlan.create(data.directory,new TransactionScope(List.of(TransactionScope.Span.range(data.column,null,null))),identity.fence(),identity.business(),group,1,identity.firstControl());fence=plan.fence();
            reservations.put(fence.fenceId(),new RatisDatabaseBackupFactory.Reservation(grant,identity));access.put(data.domain,new RatisDatabaseBackupFactory.Domain(fences,endpoint.routed,endpoint.scheduler));
        }
        byte[] value(byte[] key)throws Exception{try(var snapshot=store.capture(new TransactionScope(List.of(TransactionScope.Span.point(column,key))))){return snapshot.get(column,key);}}
        SnapshotFenceStatus status()throws Exception{byte[] key=ByteBuffer.allocate(16).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array();byte[] value=value(key);return value==null?null:SnapshotFenceStatus.decode(value);}
        @Override public void close()throws Exception{routed.close();pool.close();}
    }
    RatisDatabaseBackupFactoryFixture(Path root)throws Exception{this(root,false);}
    RatisDatabaseBackupFactoryFixture(Path root,boolean lostReplies)throws Exception{
        loseIssuance=loseHomeBinding=loseDataBinding=lostReplies;real=new RatisDatabaseBackupRealCohortFixture(root,1,1);real.control=this;
        catalogue=new Catalogue();
        catalogue.epochs.execute(catalogue.epochs.prepare(new OperationId(UUID.randomUUID(),0,0),null,SessionEpochLedger.initial(real.base.cluster,real.base.database,0)));
        Path issuance=root.resolve("issued-grant");byte[] pending=null;
        try(var issuer=RatisGrantIssuer.create(issuance,catalogue.epochs,600000)){if(lostReplies){assertThrows(IOException.class,issuer::step);pending=DistributedCommandCodec.encode(issuer.preparedCommand());}else assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step());}
        try(var issuer=RatisGrantIssuer.reopen(issuance,catalogue.epochs)){if(pending!=null)assertArrayEquals(pending,DistributedCommandCodec.encode(issuer.preparedCommand()));assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step());issued=issuer.issued().grant();assertEquals(issuer.issued().issuance(),catalogue.epochs.read());assertEquals(1,catalogue.epochs.read().grants().size());}
        for(var endpoint:real.endpoints){var terminal=endpoint.state.home.abort();for(var store:endpoint.state.stores.values())store.applyGateRelease(next(store),terminal);homes.add(new Home(endpoint,homes.size(),issued));}
        var domains=homes.stream().map(h->new DatabaseBackupDefinition.Domain(h.data.directory,h.data.schema,List.of(h.fence))).toList();definition=new DatabaseBackupDefinition(UUID.randomUUID(),real.base.definition.roster(),domains,real.base.definition.limits());
        int slot=0;for(var h:homes){bind(root.resolve("home-binding-"+slot),h.store,h.routed,h.group,lostReplies && slot==0);for(var store:h.data.stores.values())bind(root.resolve("data-binding-"+slot++),store,access.get(h.data.domain).routed(),store.ownership().owner().groupId(),lostReplies && slot==1);}


    }
    private void bind(Path path,ReplicatedRangeStore store,RatisRoutedClient routed,UUID group,boolean expectLost)throws Exception{
        store.enableSessionControls();byte[] pending=null;
        try(var maintenance=new RatisSessionMaintenance(path,catalogue.epochs,routed,group)){maintenance.prepareBinding(issued,4096);if(expectLost){assertThrows(IOException.class,maintenance::execute);pending=DistributedCommandCodec.encode(maintenance.pendingCommand());}else assertEquals(CommandResult.Status.SUCCESS,maintenance.execute().status());}
        try(var maintenance=new RatisSessionMaintenance(path,catalogue.epochs,routed,group)){if(pending!=null){assertArrayEquals(pending,DistributedCommandCodec.encode(maintenance.pendingCommand()));assertEquals(CommandResult.Status.SUCCESS,maintenance.execute().status());}assertEquals(SessionAuthority.from(catalogue.epochs.read()),maintenance.propagated());}
        assertEquals(SessionAuthority.from(catalogue.epochs.read()),store.metadataOutcomeState().sessionAuthority());bound.add(store);
    }
    static ApplyPosition next(ReplicatedRangeStore store){return new ApplyPosition(1,store.applied().index()+1);}
    Home home(UUID group){return homes.stream().filter(h->h.data.stores.containsKey(group)).findFirst().orElseThrow();}
    @Override public byte[] transaction(UUID group,int kind,byte[] body)throws Exception{
        var home=home(group);var store=home.data.stores.get(group);
        if(kind==ScopedTransactionRpc.SNAPSHOT_GATE_STATE){var gate=store.snapshotGate(ScopedTransactionRpc.uuid(ByteBuffer.wrap(body)));byte[] bytes=gate==null?null:gate.encode();return ByteBuffer.allocate(4+(bytes==null?0:bytes.length)).putInt(bytes==null?-1:bytes.length).put(bytes==null?new byte[0]:bytes).array();}
        if(kind==ScopedTransactionRpc.SNAPSHOT_READY){var fence=SnapshotFence.decode(body);var gate=store.snapshotGate(fence.fenceId());return new RatisSnapshotReadiness(ScopedTransactionRpc.SnapshotBinding.of(fence),group,gate==null?null:gate.installed(),gate!=null && gate.active() && store.activeTrackedSnapshotPins()==0).encode();}
        var request=RatisSnapshotRecord.decode(body);if(kind==ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL)return request.digest();if(kind!=ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE)throw new AssertionError("Unexpected DATA control");
        requireHome(home,request);var receipt=request.receipt(store.snapshotGateResult(request.fence()));byte[] encoded=receipt==null?null:receipt.encode();return ByteBuffer.allocate(41+(encoded==null?0:encoded.length)).putInt(32).put(request.digest()).put((byte)(encoded==null?1:2)).putInt(encoded==null?-1:encoded.length).put(encoded==null?new byte[0]:encoded).array();
    }
    static void requireHome(Home home,RatisSnapshotRecord request)throws Exception{var status=home.status();if(status==null || !status.fence().equals(request.fence()) || request.isRelease() && !Arrays.equals(status.terminalDecision().encode(),request.terminal().encode()) || !request.isRelease() && status.terminal())throw new IOException("Actual native HOME authority differs");}
    @Override public byte[] participant(UUID group,byte[] body)throws Exception{var home=home(group);var store=home.data.stores.get(group);var request=RatisSnapshotRecord.decode(body);requireHome(home,request);var saved=request.receipt(store.snapshotGateResult(request.fence()));if(saved!=null)return saved.encode();var result=request.isRelease()?store.applyGateRelease(next(store),request.terminal()):store.applyGateInstall(next(store),request.fence());return request.receipt(result).encode();}
    @Override public CommandResult session(UUID group,DistributedCommand command)throws Exception{var h=home(group);catalogue.epochs.sessionControlAdmission(h.data.directory,group).accept(command);var applied=CommandResult.decode(h.data.stores.get(group).apply(next(h.data.stores.get(group)),command));if(loseDataBinding){loseDataBinding=false;throw new IOException("Lost native DATA binding reply");}return applied;}
    @Override public void close()throws Exception{for(var home:homes)home.close();catalogue.close();real.close();}
}
