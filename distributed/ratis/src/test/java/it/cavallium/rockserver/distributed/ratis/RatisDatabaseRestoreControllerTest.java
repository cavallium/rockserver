package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual qualified fresh native node and catalogue/data apply paths; transport boundaries are controlled, never sockets. */
class RatisDatabaseRestoreControllerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
    final class Target implements RatisDatabaseRestoreController.Provisioner,RatisRangeImportController.DataPlane,AutoCloseable {
        final RatisNodeRuntimeTest.Env resources;
        RatisNodeRuntimeTest.FakeServer fake=new RatisNodeRuntimeTest.FakeServer();
        final Map<UUID,RatisRoutedClient> routes=new HashMap<>();
        RatisNodeRuntime node;RatisNodePlan plan;RaftClient client;RatisDatabaseRestorePlanner.Plan frozen;
        boolean loseGrant,loseInstall,recovering;
        Target()throws Exception{resources=new RatisNodeRuntimeTest.Env(root.resolve("resources"),false);}
        public RatisDatabaseRestoreController.Execution provision(RatisDatabaseRestorePlanner.Plan restored,Map<UUID,Path> operators)throws IOException{
            try {
                if(node!=null){assertArrayEquals(frozen.encode(),restored.encode());return new RatisDatabaseRestoreController.Execution(plan,client,routes,this);} frozen=restored;var owner=restored.catalogue().replicas().getFirst();
                var base=resources.settings();int columns=Math.max(2,restored.domains().stream().mapToInt(d->d.schema().columnIds().size()).max().orElse(0));var limits=new NativeRangeImage.Limits(base.images().maxRowBytes(),base.images().maxImageBytes(),columns);
                UUID poolId=UUID.nameUUIDFromBytes((restored.archiveId()+":"+owner.nodeIncarnation()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                var pool=new RatisNativePoolSpec(poolId,("database="+ConfigPrinter.stringify(ConfigParser.parseDefault())).getBytes(java.nio.charset.StandardCharsets.UTF_8),RatisNodePlan.MAX_DATA_GROUPS+1,limits,base.retention(),base.exports().pins(),RatisSchemaReplicaService.Limits.defaults(),base.resources().pool(),256L<<30);
                var placements=new HashMap<UUID,UUID>();for(var domain:restored.domains())placements.put(domain.schema().domainId(),poolId);
                plan=restored.nodePlan(owner,Map.of(),List.of(pool),placements,Map.of(restored.catalogue().groupId(),poolId));
                var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                    var wire=(Message)args[0];var machine=machine(plan.catalogue().groupId());
                    if(method.getName().equals("sendReadOnly"))return reply(plan.catalogue().groupId(),machine.queryLinearizable(wire).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                    if(!method.getName().equals("send"))throw new AssertionError(method);var result=append(plan.catalogue().groupId(),wire);var command=DistributedCommandCodec.decode(wire.getContent().toByteArray());if(loseGrant && command.mutations().stream().anyMatch(m->m.columnId().equals(plan.catalogue().sessionsColumn()) && m.value()!=null && SessionEpochLedger.decode(m.value()).grants().size()==1)){loseGrant=false;throw new IOException("Lost actual fresh grant reply");}return reply(plan.catalogue().groupId(),result);
                });
                client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(plan.catalogue().groupId());case "io"->io;default->throw new AssertionError(method);});
                for(var domain:restored.domains())routes.put(domain.schema().domainId(),new RatisRoutedClient(domain.initialDirectory(),new RatisRoutedClient.Transport(){
                    public byte[] read(RangeDescriptor range,UUID column,byte[] key)throws IOException{return ScopedTransactionRpc.pointValue(transaction(range,ScopedTransactionRpc.pointRequest(column,key),true),DistributedCommandCodec.MAX_ENCODED_BYTES);}
                    public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{try{return CommandResult.decode(append(range.groupId(),message(DistributedCommandCodec.encode(command))));}catch(Exception failure){throw new IOException(failure);}}
                    public byte[] transaction(RangeDescriptor range,byte[] bytes,boolean fresh)throws IOException{try{return (fresh ? machine(range.groupId()).queryLinearizable(message(bytes)) : machine(range.groupId()).query(message(bytes))).get(10,TimeUnit.SECONDS).getContent().toByteArray();}catch(Exception failure){throw new IOException(failure);}}
                    public RangeStateView stateView(RangeDescriptor range)throws IOException{return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(range,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body());}
                }));
                var c=plan.catalogue();var epochs=new RatisSessionEpochs(client,plan.clusterId(),plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
                Path targetRoot=Files.createDirectories(root.resolve("fresh-node"));SessionJournalFiles.force(targetRoot);SessionJournalFiles.force(root);
                var descriptors=restored.domains().stream().flatMap(d->d.schema().columns().stream()).map(DistributedSchema.Column::operator).filter(Objects::nonNull).distinct().toList();
                for(var descriptor:descriptors){Path asset=null;for(var candidate:operators.values())if(Arrays.equals(descriptor.artifactSha256(),RangeImportAdmission.sha256(Files.readAllBytes(candidate)))){asset=candidate;break;}if(asset==null)throw new IOException("Missing independently archived operator");if(!recovering)DistributedStoragePool.stageOperator(pool.nativePath(targetRoot),descriptor,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract(descriptor.deterministicContract(),descriptor.deterministicContractVersion(),true),Files.readAllBytes(asset));}
                var settings=new RatisNodeRuntime.Settings(limits,base.retention(),base.clockPeriod(),base.propagationVisits(),Math.max(base.maxPropagationRanges(),restored.domains().stream().mapToInt(d->d.initialDirectory().ranges().size()).sum()),base.shutdownTimeout(),base.exports(),RatisNodeRuntime.ResourceLimits.defaults());
                node=RatisNodeRuntime.open(plan,targetRoot,ConfigParser.parseDefault(),epochs,routes,resources.scheduler,resources.timer,()->restored.authority().logicalTimeMillis(),settings,recovering ? RatisNodeRuntime.Startup.RECOVER : RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,restored.domains().stream().map(RatisDatabaseRestorePlanner.Domain::schema).toList());
                // Replace only network transport lookup with actual local native owner replies at the same admission boundary.
                RatisTopologySeedRuntimeTest.field(machine(c.groupId()),"importPublicationAdmission",(RatisRangeStateMachine.ImportPublicationAdmission)command->{
                    var read=(RatisRangeImportCatalogue.ImmutableReader)key->{try{return store(c.groupId()).readCurrentPoint(c.directoryColumn(),key);}catch(Exception failure){throw new IllegalStateException(failure);}};
                    if(RatisRangeImportCatalogue.isPublication(command))RatisRangeImportCatalogue.admitPublication(command,plan.clusterId(),plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),read,this::receipt);
                    else RatisRangeImportCatalogue.admitCompletion(command,plan.clusterId(),plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),read,this::activation);
                });node.start();return new RatisDatabaseRestoreController.Execution(plan,client,routes,this);
            }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}
        }
        RatisRangeStateMachine machine(UUID group){return fake.machines.get(RaftGroupId.valueOf(group));}
        ReplicatedRangeStore store(UUID group)throws Exception{return RatisTopologySeedRuntimeTest.field(machine(group),"store",ReplicatedRangeStore.class);}
        RaftClientReply reply(UUID group,byte[] bytes){return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setSuccess(true).setMessage(message(bytes)).build();}
        byte[] append(UUID group,Message message)throws Exception{
            var machine=machine(group);var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message).build());if(transaction.getException()!=null)throw transaction.getException();machine.preAppendTransaction(transaction);transaction.initLogEntry(1,store(group).applied().index()+1);var reply=machine.applyTransaction(transaction).get(10,TimeUnit.SECONDS);return reply.getContent().toByteArray();
        }
        public RatisReferencedFile transfer(RangeImportDefinition definition,RatisReferencedFile.Request request,Path source)throws IOException{
            try{long index=store(request.groupId()).applied().index()+1;var files=(RatisReferencedFiles)RatisTopologySeedRuntimeTest.field(node,"importFiles",Map.class).get(request.groupId());byte[] bytes=Files.readAllBytes(source);var entry=RatisReferencedFilesTest.entry(request.groupId(),request.clientId(),request.callId(),index,bytes,true);files.write(entry).get(10,TimeUnit.SECONDS);files.flush(index).get(10,TimeUnit.SECONDS);var machine=machine(request.groupId());var transaction=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(entry).build();machine.applyTransaction(transaction).get(10,TimeUnit.SECONDS);return files.reference(1,index);}catch(Exception failure){throw new IOException(failure);}
        }
        public NativeSstImportManifest.Receipt install(RangeImportDefinition definition,RatisRangeImportRecord.Install record)throws IOException{
            try{var machine=machine(record.groupId());machine.queryLinearizable(message(RatisRangeImportRpc.prepare(record))).get(10,TimeUnit.SECONDS);var receipt=NativeSstImportManifest.Receipt.decode(append(record.groupId(),message(record.encode())));if(loseInstall){loseInstall=false;throw new IOException("Lost actual native INSTALL reply");}return receipt;}catch(Exception failure){throw new IOException(failure);}
        }
        public NativeRangeSstImport.Activation activate(RangeImportDefinition definition,RatisRangeImportRecord.Activate record)throws IOException{try{machine(record.receipt().groupId()).queryLinearizable(message(RatisRangeImportRpc.prepare(record))).get(10,TimeUnit.SECONDS);return NativeRangeSstImport.Activation.decode(append(record.receipt().groupId(),message(record.encode())));}catch(Exception failure){throw new IOException(failure);}}
        RatisRangeImportApply.Status status(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{assertEquals(frozen.catalogue().replicas().getFirst(),owner);var target=RatisRangeImportStatusRpc.Target.of(definition,range.groupId(),owner);try{return RatisRangeImportStatusRpc.decodeReply(machine(range.groupId()).queryStale(message(RatisRangeImportStatusRpc.request(target)),0).get(10,TimeUnit.SECONDS).getContent().toByteArray(),target);}catch(Exception failure){throw new IOException(failure);}}
        public RangeImportAdmission.OwnerReceipt receipt(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{var status=status(definition,range,owner);return new RangeImportAdmission.OwnerReceipt(range.groupId(),range.rangeId(),range.configurationVersion(),owner,status.receipt(),status.applied());}
        public RangeImportActivationAdmission.OwnerActivation activation(RangeImportDefinition definition,RatisRangeImportCatalogue.Head head,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{var status=status(definition,range,owner);return new RangeImportActivationAdmission.OwnerActivation(range.groupId(),range.rangeId(),range.configurationVersion(),owner,RangeImportActivationAdmission.Activation.decode(status.activation().encode()),status.applied());}
        public RatisRangeImportApply.PublicationView publication(RangeImportDefinition definition)throws IOException{var c=plan.catalogue();try{return RatisRangeImportAuthority.readNative(store(c.groupId()),c.directoryColumn(),plan.clusterId(),plan.databaseId(),RatisRangeImportAuthority.Request.of(definition));}catch(Exception failure){throw new IOException(failure);}}
        void cold()throws Exception{if(node!=null){node.close();node=null;}for(var route:routes.values())route.close();routes.clear();fake=new RatisNodeRuntimeTest.FakeServer();recovering=true;}
        public void close()throws Exception{cold();resources.close();}
    }
    private static boolean causedBy(Throwable failure,String text){for(var current=failure;current!=null;current=current.getCause())if(current.getMessage()!=null && current.getMessage().contains(text))return true;return false;}
    @Test void independentArchiveWithOriginalDirectoriesAbsentRestoresThroughNativeImportGrantAndQualifiedReads()throws Exception{
        var source=new RatisDatabaseBackupFixture(1,2*1024*1024);Path original=Files.createDirectory(root.resolve("original"));Path archiveRoot=root.resolve("archive");
        try(var nativeSource=new RatisDatabaseBackupArchiveTest.Source(original.resolve("db"),source);var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination)){var exported=nativeSource.export(original.resolve("data.sst"),source);var chunk=archive.copy(source.domain,0,UUID.randomUUID(),exported);archive.complete(source.manifest(List.of(chunk)));}
        try(var paths=Files.walk(original)){for(var path:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}assertFalse(Files.exists(original));Path journal=Files.createDirectory(root.resolve("restore"));
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination);var target=new Target()){
            byte[] frozen;try(var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(source.fresh()),UUID.randomUUID(),10,target)){frozen=restore.plan().encode();target.loseGrant=true;for(int i=0;i<100 && restore.phase()!=RatisDatabaseRestoreController.Phase.GRANT;i++)restore.step(1);assertThrows(IOException.class,()->restore.step(1));}
            try(var restore=RatisDatabaseRestoreController.reopen(journal,archive,target)){assertArrayEquals(frozen,restore.plan().encode());target.loseInstall=true;boolean lost=false;for(int i=0;i<500 && restore.phase()!=RatisDatabaseRestoreController.Phase.READY;i++){try{restore.step(1);}catch(IOException failure){if(!lost && causedBy(failure,"Lost actual native INSTALL reply")){lost=true;}else throw failure;}}assertTrue(lost);assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());var issued=restore.issuedLedger();var domain=restore.plan().domains().getFirst();var grant=issued.grants().stream().filter(g->g.sessionId().equals(restore.plan().sessionId())).findFirst().orElseThrow();var logical=new RatisLogicalClient(domain.schema(),target.routes.get(domain.schema().domainId()),issued,grant,RatisSessionClient.Limits.defaults());assertArrayEquals(new byte[]{9},logical.get(17,new Keys(new it.cavallium.buffer.Buf[]{it.cavallium.buffer.Buf.wrap(new byte[]{7})})).toByteArray());assertEquals(1,issued.grants().size());}
        }
    }    record Copied(DatabaseBackupDefinition definition,DatabaseBackupManifest.Destination destination,UUID emptyColumn) {}
    Copied twoDomainArchive(Path sourceRoot,Path archiveRoot)throws Exception {return twoDomainArchive(sourceRoot,archiveRoot,false);}
    Copied twoDomainArchive(Path sourceRoot,Path archiveRoot,boolean withOperator)throws Exception {
        Files.createDirectories(sourceRoot);byte[] jar=withOperator ? appendArtifact() : null;var operator=withOperator ? new DistributedSchema.Operator("append",1,RatisNodeRuntimeTest.AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"pure-append",1) : null;var base=new RatisDatabaseBackupFixture(1,4*1024*1024);var shape=ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true);var domains=new ArrayList<DatabaseBackupDefinition.Domain>();var held=new ArrayList<SnapshotFenceStatus>();var groups=new HashSet<UUID>();UUID empty=new UUID(900,2);
        for(int n=1;n<=2;n++) {
            UUID domain=new UUID(901,n),group=new UUID(902,n),column=new UUID(900,n==1 ? 1 : 3);var columns=new ArrayList<DistributedSchema.Column>();columns.add(new DistributedSchema.Column(n==1 ? 17 : 18,column,"data-"+n,withOperator ? ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true,"append",1L,RatisNodeRuntimeTest.AppendOperator.class.getName()) : shape,operator));if(n==1)columns.add(new DistributedSchema.Column(99,empty,"empty",shape,null));var schema=new DistributedSchema(base.cluster,base.database,domain,0,0,columns);var range=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(base.owner));var directory=new RangeDirectory(base.cluster,base.database,domain,0,schema.digest(),0,schema.columnIds(),0,List.of(range));
            var scope=new TransactionScope(schema.columnIds().stream().map(c->TransactionScope.Span.range(c,null,null)).toList());var participant=new SnapshotFence.Participant(group,group,1,schema.digest(),new OperationId(base.session,1,n*3),new OperationId(base.session,1,n*3+1),scope);var fence=new SnapshotFence(base.cluster,base.database,domain,new UUID(903,n),new UUID(904,n),1,new OperationId(base.session,1,n*3+2),0,0,schema.digest(),List.of(participant));var status=SnapshotFenceStatus.begin(fence).ready(group,base.cut).capture(group,new SnapshotFenceStatus.Capture(base.owner.nodeIncarnation(),new UUID(905,n),base.cut));domains.add(new DatabaseBackupDefinition.Domain(directory,schema,List.of(fence)));held.add(status);groups.add(group);
        }
        var definition=new DatabaseBackupDefinition(UUID.randomUUID(),new DatabaseDomainRoster(base.cluster,base.database,0,domains.stream().map(d->d.schema().domainId()).toList()),domains,new DatabaseBackupDefinition.Limits(2,3,3,4*1024*1024,4*1024*1024));var chunks=new ArrayList<DatabaseBackupManifest.Chunk>();
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,definition,base.destination)) {
            for(int n=0;n<domains.size();n++){var domain=domains.get(n);var range=domain.directory().ranges().getFirst();for(var column:domain.schema().columns()){
                if(column.columnId().equals(empty)){chunks.add(new DatabaseBackupManifest.Chunk(domain.schema().domainId(),range.groupId(),column.columnId(),0,base.cut,null,null,null,0,new byte[0],0,null,null));continue;}
                Path file=sourceRoot.resolve(column.columnId()+".sst");try(var env=new org.rocksdb.EnvOptions();var options=new org.rocksdb.Options();var writer=new org.rocksdb.SstFileWriter(env,options)){writer.open(file.toString());writer.put(new byte[]{7},new byte[]{(byte)(11*(n+1))});writer.finish();}
                byte[] hash=RangeImportAdmission.sha256(Files.readAllBytes(file));var exported=new NativeRangeSstExport.Manifest(range.groupId(),range.rangeEpoch(),column.columnId(),domain.schema().digest(),base.cut,null,null,1,2,Files.size(file),hash,file);chunks.add(archive.copy(domain.schema().domainId(),0,UUID.randomUUID(),exported));
            }}
            var assets=new ArrayList<DatabaseBackupManifest.OperatorAsset>();if(withOperator){Path asset=sourceRoot.resolve("append.jar");Files.write(asset,jar);assets.add(archive.copyOperator(UUID.randomUUID(),asset,operator.artifactSha256()));}
            archive.complete(new DatabaseBackupManifest(definition,base.destination,held,List.of(new DatabaseBackupManifest.Cohort(base.owner.nodeIncarnation(),UUID.randomUUID(),UUID.randomUUID(),groups)),chunks,assets));
        }
        try(var paths=Files.walk(sourceRoot)){for(var path:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}return new Copied(definition,base.destination,empty);
    }
    @Test void actualTwoDomainRestorePreservesInterleavedSparseApiIdsAndEmptyColumnWithoutOriginalPaths()throws Exception {
        Path original=root.resolve("two-original"),archiveRoot=root.resolve("two-archive");var source=twoDomainArchive(original,archiveRoot);assertFalse(Files.exists(original));Path journal=Files.createDirectory(root.resolve("two-restore"));var membership=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"restored","localhost:19010",ReplicaDescriptor.Role.VOTER);
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var target=new Target();var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(membership),UUID.randomUUID(),10,target)){
            for(int i=0;i<1000 && restore.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)restore.step(1);assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());var ledger=restore.issuedLedger();assertEquals(1,ledger.grants().size());var grant=ledger.grants().getFirst();var roster=new RatisDatabaseDomainRoster(target.client,restore.plan().roster().clusterId(),restore.plan().roster().databaseId(),restore.plan().catalogue()).readComplete();assertEquals(2,roster.directories().size());
            for(int n=0;n<restore.plan().domains().size();n++){var domain=restore.plan().domains().get(n);var client=new RatisLogicalClient(domain.schema(),target.routes.get(domain.schema().domainId()),ledger,grant,RatisSessionClient.Limits.defaults());assertArrayEquals(new byte[]{(byte)(11*(n+1))},client.get(n==0 ? 17 : 18,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))).toByteArray());if(n==0)assertNull(client.get(99,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))));}
            assertEquals(99,RatisColumnAllocation.counter(new RatisColumnAllocation(target.client,restore.plan().catalogue().groupId(),0,new byte[32],restore.plan().catalogue().directoryColumn()).readCounter()));
        }
    }

    @Test void coldTargetReopensExactPlanGrantAndUserDataWithOriginalSourceAbsent()throws Exception {
        Path original=root.resolve("cold-original"),archiveRoot=root.resolve("cold-archive");var source=twoDomainArchive(original,archiveRoot);Path journal=Files.createDirectory(root.resolve("cold-restore"));var membership=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"restored","localhost:19010",ReplicaDescriptor.Role.VOTER);
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var target=new Target()) {
            byte[] planBytes,ledgerBytes;
            try(var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(membership),UUID.randomUUID(),10,target)) {
                for(int i=0;i<1000 && restore.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)restore.step(1);
                assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());planBytes=restore.plan().encode();ledgerBytes=restore.issuedLedger().encode();
            }
            target.cold();assertFalse(Files.exists(original));
            try(var restore=RatisDatabaseRestoreController.reopen(journal,archive,target)) {
                assertArrayEquals(planBytes,restore.plan().encode());var ledger=restore.issuedLedger();assertArrayEquals(ledgerBytes,ledger.encode());assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());
                for(int n=0;n<restore.plan().domains().size();n++) {var domain=restore.plan().domains().get(n);var routed=target.routes.get(domain.schema().domainId());assertArrayEquals(domain.finalDirectory().payloadDigest(),routed.directory().payloadDigest());var logical=new RatisLogicalClient(domain.schema(),routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());assertArrayEquals(new byte[]{(byte)(11*(n+1))},logical.get(n==0 ? 17 : 18,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))).toByteArray());if(n==0)assertNull(logical.get(99,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))));}
                assertEquals(2,new RatisDatabaseDomainRoster(target.client,restore.plan().roster().clusterId(),restore.plan().roster().databaseId(),restore.plan().catalogue()).readComplete().directories().size());
            }
        }
    }

    private static byte[] appendArtifact()throws Exception {
        var bytes=new java.io.ByteArrayOutputStream();String entry=RatisNodeRuntimeTest.AppendOperator.class.getName().replace('.','/')+".class";
        try(var jar=new java.util.jar.JarOutputStream(bytes)){jar.putNextEntry(new java.util.jar.JarEntry(entry));try(var input=RatisNodeRuntimeTest.AppendOperator.class.getClassLoader().getResourceAsStream(entry)){assertNotNull(input);input.transferTo(jar);}jar.closeEntry();}return bytes.toByteArray();
    }
    @Test void archivedOperatorIsStagedAndExecutesMergeAfterRestoreAndColdReopen()throws Exception {
        Path original=root.resolve("operator-original"),archiveRoot=root.resolve("operator-archive");var source=twoDomainArchive(original,archiveRoot,true);assertFalse(Files.exists(original));Path journal=Files.createDirectory(root.resolve("operator-restore"));var membership=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"restored","localhost:19010",ReplicaDescriptor.Role.VOTER);
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var target=new Target()) {
            try(var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(membership),UUID.randomUUID(),10,target)) {
                for(int i=0;i<1000 && restore.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)restore.step(1);assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());var ledger=restore.issuedLedger();var domain=restore.plan().domains().getFirst();var logical=new RatisLogicalClient(domain.schema(),target.routes.get(domain.schema().domainId()),ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
                assertEquals(CommandResult.Status.SUCCESS,logical.execute(logical.prepareMerge(17,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7})),it.cavallium.buffer.Buf.wrap(new byte[]{33}),Mutation.ReturnMode.NONE)).status());assertArrayEquals(new byte[]{11,33},logical.get(17,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))).toByteArray());
            }
            target.cold();assertFalse(Files.exists(original));try(var restore=RatisDatabaseRestoreController.reopen(journal,archive,target)){var ledger=restore.issuedLedger();var domain=restore.plan().domains().getFirst();var logical=new RatisLogicalClient(domain.schema(),target.routes.get(domain.schema().domainId()),ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());assertArrayEquals(new byte[]{11,33},logical.get(17,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))).toByteArray());}
        }
    }

    Copied manyGroupArchive(Path sourceRoot,Path archiveRoot)throws Exception {
        Files.createDirectories(sourceRoot);var base=new RatisDatabaseBackupFixture(1,4*1024*1024);UUID domain=new UUID(911,1),column=new UUID(912,1);var shape=ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true);var schema=new DistributedSchema(base.cluster,base.database,domain,0,1,List.of(new DistributedSchema.Column(701,column,"batched",shape,null)));var ranges=new ArrayList<RangeDescriptor>();
        for(int n=0;n<65;n++){UUID group=new UUID(913,n);ranges.add(new RangeDescriptor(group,group,1,0,n==0 ? null : new byte[]{(byte)n},n==64 ? null : new byte[]{(byte)(n+1)},RangeDescriptor.TopologyState.ACTIVE,List.of(base.owner)));}
        var directory=new RangeDirectory(base.cluster,base.database,domain,0,schema.digest(),0,schema.columnIds(),1,ranges);var fences=new ArrayList<SnapshotFence>();var held=new ArrayList<SnapshotFenceStatus>();
        for(int start=0;start<65;start+=64){var participants=new ArrayList<SnapshotFence.Participant>();for(int n=start;n<Math.min(65,start+64);n++){var range=ranges.get(n);var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,range.lowerInclusive(),range.upperExclusive())));participants.add(new SnapshotFence.Participant(range.groupId(),range.rangeId(),1,schema.digest(),new OperationId(base.session,1,3*n),new OperationId(base.session,1,3*n+1),scope));}var fence=new SnapshotFence(base.cluster,base.database,domain,new UUID(914,start),new UUID(915,start),1,new OperationId(base.session,1,3*65+start),0,0,schema.digest(),participants);var status=SnapshotFenceStatus.begin(fence);for(var participant:participants)status=status.ready(participant.groupId(),base.cut);for(var participant:participants)status=status.capture(participant.groupId(),new SnapshotFenceStatus.Capture(base.owner.nodeIncarnation(),new UUID(916,participant.groupId().getLeastSignificantBits()),base.cut));fences.add(fence);held.add(status);}
        var definition=new DatabaseBackupDefinition(UUID.randomUUID(),new DatabaseDomainRoster(base.cluster,base.database,0,List.of(domain)),List.of(new DatabaseBackupDefinition.Domain(directory,schema,fences)),new DatabaseBackupDefinition.Limits(65,65,65,4*1024*1024,4*1024*1024));var chunks=new ArrayList<DatabaseBackupManifest.Chunk>();
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,definition,base.destination)) {
            for(int n=0;n<65;n++){var range=ranges.get(n);if(n!=64){chunks.add(new DatabaseBackupManifest.Chunk(domain,range.groupId(),column,0,base.cut,range.lowerInclusive(),range.upperExclusive(),null,0,new byte[0],0,null,null));continue;}Path file=sourceRoot.resolve("last.sst");try(var env=new org.rocksdb.EnvOptions();var options=new org.rocksdb.Options();var writer=new org.rocksdb.SstFileWriter(env,options)){writer.open(file.toString());writer.put(new byte[]{64},new byte[]{91});writer.finish();}var exported=new NativeRangeSstExport.Manifest(range.groupId(),1,column,schema.digest(),base.cut,range.lowerInclusive(),range.upperExclusive(),1,2,Files.size(file),RangeImportAdmission.sha256(Files.readAllBytes(file)),file);chunks.add(archive.copy(domain,0,UUID.randomUUID(),exported));}
            archive.complete(new DatabaseBackupManifest(definition,base.destination,held,List.of(new DatabaseBackupManifest.Cohort(base.owner.nodeIncarnation(),UUID.randomUUID(),UUID.randomUUID(),ranges.stream().map(RangeDescriptor::groupId).collect(java.util.stream.Collectors.toSet()))),chunks,List.of()));
        }
        try(var paths=Files.walk(sourceRoot)){for(var path:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}return new Copied(definition,base.destination,null);
    }
    @Test void sixtyFiveNativeGroupsRestoreAcrossTwoImportsWithDataInSecondBatch()throws Exception {
        Path original=root.resolve("batched-original"),archiveRoot=root.resolve("batched-archive");var source=manyGroupArchive(original,archiveRoot);assertFalse(Files.exists(original));Path journal=Files.createDirectory(root.resolve("batched-restore"));var membership=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"restored","localhost:19010",ReplicaDescriptor.Role.VOTER);
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var target=new Target();var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(membership),UUID.randomUUID(),10,target)) {
            var domain=restore.plan().domains().getFirst();assertEquals(2,domain.imports().size());assertEquals(64,domain.imports().getFirst().manifests().size());assertEquals(1,domain.imports().getLast().manifests().size());assertEquals((byte)64,domain.imports().getLast().manifests().getFirst().lowerInclusive()[0]);
            for(int i=0;i<3000 && restore.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)restore.step(1);assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());var ledger=restore.issuedLedger();assertEquals(1,ledger.grants().size());var routed=target.routes.get(domain.schema().domainId());assertEquals(65,routed.directory().ranges().size());for(var range:routed.directory().ranges()){assertEquals(RangeDescriptor.TopologyState.ACTIVE,range.topologyState());routed.stateView(range.groupId()).requireServing(SessionAuthority.from(ledger));}
            var logical=new RatisLogicalClient(domain.schema(),routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());assertArrayEquals(new byte[]{91},logical.get(701,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{64}))).toByteArray());assertNull(logical.get(701,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{0}))));var c=restore.plan().catalogue();assertEquals(65,new RatisDatabaseDomainRoster(target.client,restore.plan().roster().clusterId(),restore.plan().roster().databaseId(),c).readComplete().directories().get(domain.schema().domainId()).ranges().size());var imports=new RatisRangeImportCatalogue(target.client,restore.plan().roster().clusterId(),restore.plan().roster().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());for(var imported:domain.imports())assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,imports.readImport(imported.importId()).phase());assertEquals(RatisRangeImportCatalogue.Phase.COMPLETE,imports.readHead(domain.schema().domainId()).phase());
        }
    }

}
