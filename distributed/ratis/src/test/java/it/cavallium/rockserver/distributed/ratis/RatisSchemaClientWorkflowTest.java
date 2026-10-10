package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Native metadata and DATA changes behind controlled peer transports; no server/quorum/network claim. */
class RatisSchemaClientWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final class Peer implements AutoCloseable {
        final ReplicaDescriptor replica; final DistributedStoragePool pool; final ReplicatedRangeStore store; final Path path; final RatisSchemaArtifactRegistry artifacts;
        final Map<UUID,String> names=new HashMap<>();
        Peer(Path path,ReplicaDescriptor replica,RangeDescriptor range,DistributedSchema schema) throws Exception {
            this.replica=replica; this.path=path; artifacts=new RatisSchemaArtifactRegistry(path.resolveSibling(path.getFileName()+"-staging"),new RatisSchemaReplicaService.Limits(16,64L*1024*1024,192L*1024*1024,16,16,0)); pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            for(var column:schema.columns()) names.put(column.columnId(),"data");
            store=pool.openRange(new RangeOwnership.Owner(range.groupId(),range.rangeEpoch()),new RangeOwnership.Interval(schema.domainId(),schema.columnIds(),range.lowerInclusive(),range.upperExclusive()),schema.digest(),names,Set.of()); store.enableLogicalSchema(schema); store.applyNoop(new ApplyPosition(1,0));
        }
        ApplyPosition next() { var completed=store.applied(); return new ApplyPosition(1,completed==null ? 0 : completed.index()+1); }
        Map<UUID,String> target(RatisSchemaReplicaRpc.Binding binding) { var result=new HashMap<>(names); if(binding.header().kind()==SchemaChange.Kind.CREATE_COLUMN) result.put(binding.header().changedColumnId(),binding.changedPhysicalName()); else result.remove(binding.header().changedColumnId()); return result; }
        NativeSchemaInstallProof install(SchemaChange.Definition definition,String physical) throws Exception {
            var target=new HashMap<>(names); if(definition.kind()==SchemaChange.Kind.CREATE_COLUMN) target.put(definition.header().changedColumnId(),physical); else target.remove(definition.header().changedColumnId());
            var installed=pool.installSchema(store,next(),definition,target,mergeColumns(definition.targetSchema())); names.clear(); names.putAll(target); return new NativeSchemaInstallProof(installed.fence(),definition.targetSchema().digest(),installed.position());
        }
        public void close() throws Exception { artifacts.close(); pool.close(); }
    }
    static final class Fixture implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest(); final RatisSessionEpochsTest.Fixture metadata;
        final RaftClient client; final RatisSchemaAdministration admin; final RatisNodePlan.Catalogue catalogue;
        final DistributedSchema initial; final RangeDirectory initialDirectory; final List<Peer> peers=new ArrayList<>();
        final RatisRoutedClient routed; final RatisSchemaReplicaClient replicas; final RatisOperatorCatalogue operators;
        boolean lagging,nonforced; String losePhase; boolean lost;
        final Set<DistributedSchema.Operator> admittedOperators=new HashSet<>();
        final List<byte[]> operatorWrites=new ArrayList<>();
        java.util.function.Consumer<DistributedCommand> beforeOperatorWrite=command -> {};
        boolean loseOperatorBeginReply;
        Fixture(Path root,boolean listener) throws Exception { this(root,listener,null); }
        Fixture(Path root,boolean listener,DistributedSchema.Operator admitted) throws Exception {
            metadata=outer.new Fixture(root.resolve("metadata"));
            var borrowed=new RatisSchemaAdministrationTest().client(metadata);
            if(admitted!=null) admittedOperators.add(admitted);
            var policy=(RatisOperatorCatalogue.AdmissionPolicy)upload -> { if(!admittedOperators.contains(upload.operator())) throw new IllegalArgumentException("Unconfigured operator tuple"); };
            var limits=new RatisOperatorCatalogue.Limits(16,64L*1024*1024);
            var operatorValidator=RatisOperatorCatalogue.validator(outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,limits,policy,key -> { try { return metadata.store.readCurrentPoint(outer.directoryColumn,key); } catch(Exception failure) { throw new IllegalStateException(failure); } });
            var validator=RatisCatalogue.validator(outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,operatorValidator,key -> { try { return metadata.store.readCurrentPoint(outer.directoryColumn,key); } catch(Exception failure) { throw new IllegalStateException(failure); } });
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if(!method.getName().equals("send")) return method.invoke(borrowed.io(),args);
                var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray()); validator.accept(command);
                if(RatisOperatorCatalogue.isCommand(command)) { operatorWrites.add(DistributedCommandCodec.encode(command)); beforeOperatorWrite.accept(command); }
                byte[] result=metadata.store.apply(new ApplyPosition(1,metadata.next.getAndIncrement()),command);
                if(RatisSchemaAdministration.isCommand(command) && command.mutations().getFirst().key().length==17) {
                    var phase=RatisSchemaAdministration.Head.decode(command.mutations().getFirst().value()).status().phase(); result=maybeLose(phase.name(),result);
                }
                if(loseOperatorBeginReply && RatisOperatorCatalogue.isCommand(command) && command.mutations().getFirst().key()[0]==0x70) { loseOperatorBeginReply=false; result=new byte[4]; }
                if(metadata.writeOverride!=null) result=metadata.writeOverride;
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("fixture")).setGroupId(RaftGroupId.valueOf(outer.catalogue)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(result))).build();
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) { case "getGroupId" -> borrowed.getGroupId(); case "io" -> io; default -> throw new AssertionError(method); });
            operators=admitted==null ? null : new RatisOperatorCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,limits,policy);
            admin=new RatisSchemaAdministration(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
            var original=outer.directory(outer.cluster); var source=original.ranges().getFirst(); var members=new ArrayList<>(source.replicas());
            if(listener) members.add(new ReplicaDescriptor(new UUID(80,1),new UUID(80,2),new UUID(80,3),"listener","127.0.0.1:9002",ReplicaDescriptor.Role.LISTENER));
            var range=new RangeDescriptor(source.rangeId(),source.groupId(),source.rangeEpoch(),source.configurationVersion(),null,null,RangeDescriptor.TopologyState.ACTIVE,members);
            initial=new DistributedSchema(outer.cluster,outer.database,original.domainId(),1,0,List.of(new DistributedSchema.Column(7,outer.data,"existing",RatisSchemaClientTest.shape(),null)));
            initialDirectory=new RangeDirectory(outer.cluster,outer.database,original.domainId(),1,initial.digest(),0,initial.columnIds(),0,List.of(range));
            var schemas=new RatisSchemaCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); schemas.execute(schemas.prepare(outer.operation(0),initial));
            var directories=new RatisCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); directories.execute(directories.prepare(outer.operation(1),null,initialDirectory));
            for(var replica:members) peers.add(new Peer(root.resolve("peer-"+replica.peerId()),replica,range,initial));
            catalogue=new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,members);
            routed=new RatisRoutedClient(initialDirectory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) throws IOException { try(var pin=peers.getFirst().store.capture()) { return pin.get(column,key); } catch(Exception failure) { throw new IOException(failure); } }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException { try { var result=CommandResult.decode(peers.getFirst().store.apply(peers.getFirst().next(),command)); if(!lagging) for(int i=1;i<peers.size();i++) peers.get(i).store.apply(peers.get(i).next(),command); return result; } catch(Exception failure) { throw new IOException(failure); } }
                public byte[] transaction(RangeDescriptor descriptor,byte[] bytes,boolean fresh) throws IOException {
                    try {
                        int kind=ScopedTransactionRpc.kind(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                        if(ScopedTransactionRpc.schemaFence(kind)) return RatisSchemaFenceRpc.preparedReply(kind,RatisSchemaFenceRpc.request(bytes));
                        if(ScopedTransactionRpc.schemaInstall(kind)) return RatisSchemaInstallRpc.preparedReply(kind,RatisSchemaInstallRpc.request(bytes));
                        var request=RatisSchemaActivateRpc.request(bytes); var record=request.frozenRecord()!=null ? request.frozenRecord() : new RatisSchemaActivateRecord(request.header(),request.groupId(),request.expectedInstallPosition(),outer.catalogue,metadata.store.applied());
                        return RatisSchemaActivateRpc.preparedReply(kind,request,record);
                    } catch(Exception failure) { throw new IOException(failure); }
                }
                public byte[] participant(RangeDescriptor descriptor,byte[] bytes) throws IOException {
                    try {
                        if(RatisSchemaFenceRecord.isFence(bytes)) {
                            var record=RatisSchemaFenceRecord.decode(bytes); var definition=admin.readDefinition(record.header().definitionDigest()); var head=admin.readHead(initial.domainId()); var manifest=admin.readAdmissions(definition,head.status().admissionDigest()); NativeSchemaFence first=null;
                            for(var peer:peers) { var reservation=manifest.replica(descriptor.groupId(),peer.replica.replicaId()).reservation(); var proof=peer.pool.applySchemaFence(peer.store,peer.next(),record.header(),reservation,manifest.digest()); if(first==null) first=proof; }
                            return first.encode();
                        }
                        if(RatisSchemaInstallRecord.isInstall(bytes)) {
                            var record=RatisSchemaInstallRecord.decode(bytes); var proof=peers.getFirst().install(record.definition(),record.changedPhysicalName());
                            if(!lagging) for(int i=1;i<peers.size();i++) peers.get(i).install(record.definition(),record.changedPhysicalName()); return maybeLose("INSTALL",proof.encode());
                        }
                        var record=RatisSchemaActivateRecord.decode(bytes); var first=peers.getFirst(); var install=first.store.lookupSchemaInstallation(record.header());
                        return maybeLose("ACTIVATE",first.store.applySchemaActivation(first.next(),install,record.catalogueGroup(),record.cataloguePosition()).encode());
                    } catch(Exception failure) { throw new IOException(failure); }
                }
            });
            replicas=new RatisSchemaReplicaClient(4,(descriptor,owner) -> ownerClient(descriptor,owner));
        }
        byte[] maybeLose(String phase,byte[] bytes) throws IOException { if(phase.equals(losePhase) && !lost) { lost=true; throw new IOException("Lost original "+phase+" reply"); } return bytes; }
        RaftClient ownerClient(RangeDescriptor descriptor,ReplicaDescriptor owner) {
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                assertEquals("sendStaleRead",method.getName()); assertEquals(RaftPeerId.valueOf(owner.peerId()),args[2]); var request=RatisSchemaReplicaRpc.request(((Message)args[0]).getContent().toByteArray());
                var peer=peers.stream().filter(p -> p.replica.equals(owner)).findFirst().orElseThrow(); var input=request.target().binding();
                if(request.kind()<=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE) {
                    RatisSchemaArtifactRegistry.Progress progress=switch(request.kind()) {
                        case ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN -> peer.artifacts.begin(request);
                        case ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK -> peer.artifacts.chunk(request);
                        case ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH -> { byte[] jar=peer.artifacts.artifact(request); var operator=request.upload().operator(); DistributedStoragePool.stageOperator(peer.path,operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract(operator.deterministicContract(),operator.deterministicContractVersion(),true),jar); yield peer.artifacts.ready(request); }
                        default -> peer.artifacts.progress(request);
                    };
                    byte[] reply=RatisSchemaReplicaRpc.reply(request,progress.chunks(),progress.ready(),null,RatisSchemaReplicaRpc.CapacityState.NONE,false);
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(owner.peerId())).setGroupId(RaftGroupId.valueOf(descriptor.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(reply))).build();
                }
                if(request.kind()==ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE) peer.pool.reserveSchemaCapacity(peer.store,input.definition(),peer.target(input),mergeColumns(input.definition().targetSchema()));
                if(request.kind()==ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE) {
                    var status=peer.pool.lookupSchemaCapacityStatus(peer.store,input.definition(),input.changedPhysicalName(),input.mergeAdmission()); var head=admin.readOutcome(initial.domainId(),input.definition().digest());
                    if(head.status().phase()==SchemaChange.Phase.RETIRED && peer.store.lookupSchemaInstallation(input.header())==null && !lagging) peer.install(input.definition(),input.changedPhysicalName());
                    if(status.reservation()!=null && (head.status().phase()==SchemaChange.Phase.ABORTED || peer.store.lookupSchemaInstallation(input.header())!=null)) peer.pool.releaseSchemaCapacity(peer.store,status.reservation(),head.status().phase()==SchemaChange.Phase.ABORTED ? SchemaCapacityRelease.aborted(head.status(),outer.catalogue,metadata.store.applied()) : SchemaCapacityRelease.retired(head.status(),outer.catalogue,metadata.store.applied()));
                }
                var status=peer.pool.lookupSchemaCapacityStatus(peer.store,input.definition(),input.changedPhysicalName(),input.mergeAdmission());
                byte[] reply=RatisSchemaReplicaRpc.reply(request,new BitSet(),false,status.reservation(),RatisSchemaReplicaRpc.CapacityState.valueOf(status.state().name()),status.forcedDurable() && !nonforced);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(owner.peerId())).setGroupId(RaftGroupId.valueOf(descriptor.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(reply))).build();
            });
            return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) { case "io" -> io; case "close" -> null; default -> throw new AssertionError(method); });
        }
        RatisSchemaClient client(Path journals) throws IOException { return new RatisSchemaClient(journals,outer.cluster,outer.database,initial.domainId(),catalogue,client,routed,replicas,operators); }
        void advance(RatisSchemaClient client,RatisSchemaClient.Handle handle) throws Exception { for(int n=0;n<200;n++) { var progress=client.advance(handle,1); if(progress.result()!=null && progress.phase()!=RatisSchemaClient.Phase.RETIRE) return; if(progress.phase()==RatisSchemaClient.Phase.CONFLICT) fail("Unexpected metadata conflict"); } fail("Schema request did not complete"); }
        public void close() throws Exception { replicas.close(); routed.close(); for(var peer:peers) peer.close(); metadata.close(); }
    }
    static Set<UUID> mergeColumns(DistributedSchema schema) { var result=new HashSet<UUID>(); for(var column:schema.columns()) if(column.operator()!=null) result.add(column.columnId()); return result; }
    public static class AppendOperator extends it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator {
        public AppendOperator(String identity) { super(identity); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) { var out=new java.io.ByteArrayOutputStream(); if(existing!=null) out.writeBytes(existing); operands.forEach(out::writeBytes); return out.toByteArray(); }
    }
    static byte[] jar() throws Exception {
        var bytes=new java.io.ByteArrayOutputStream(); try(var out=new java.util.jar.JarOutputStream(bytes)) {
            String entry=AppendOperator.class.getName().replace('.','/')+".class"; out.putNextEntry(new java.util.jar.JarEntry(entry));
            try(var in=AppendOperator.class.getClassLoader().getResourceAsStream(entry)) { out.write(in.readAllBytes()); } out.closeEntry();
        } return bytes.toByteArray();
    }
    @Test void commonUploadIndexesEachNameAndDeduplicatesCompleteContentWithoutNewAllocation() throws Exception {
        byte[] jar=jar(),sha=OperatorUpload.sha256(jar); String type=AppendOperator.class.getName();
        var initial=new DistributedSchema.Operator("append",0,type,sha,1,1,"append-contract",1);
        try(var f=new Fixture(root,false,initial)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            RatisSchemaClient.TrustedOperatorResolver resolver=(name,className,hash,revision) -> {
                var operator=new DistributedSchema.Operator(name,revision,className,hash,1,1,"append-contract",1); f.admittedOperators.add(operator); return operator;
            };
            try(var client=f.client(journals)) {
                assertNull(client.checkOperator("append",sha));
                for(String name:List.of("append","other")) {
                    var handle=client.prepareUpload(name,type,jar,resolver); f.advance(client,handle);
                    assertEquals(0,client.uploaded(handle).operator().version()); assertEquals(0L,client.checkOperator(name,sha));
                }
                long writes=f.metadata.next.get(); var budget=f.operators.readBudget();
                var repeated=client.prepareUpload("other","another.Class",jar,(name,className,hash,revision) -> { throw new AssertionError("Dedup must precede descriptor allocation"); });
                assertEquals(RatisSchemaClient.Phase.DONE,repeated.phase()); assertEquals(0,client.uploaded(repeated).operator().version());
                f.advance(client,repeated); assertEquals(writes,f.metadata.next.get()); assertEquals(budget,f.operators.readBudget());
                assertEquals("append",f.operators.readBySha(sha).operator().name()); assertEquals("other",f.operators.readByNameSha("other",sha).operator().name());
            }
        }
    }
    @Test void concurrentOtherNamePublicationRetriesOnlyDefinitiveNoEffectsWitnessConflict() throws Exception {
        byte[] jar=jar(),sha=OperatorUpload.sha256(jar); String type=AppendOperator.class.getName();
        var initial=new DistributedSchema.Operator("append",0,type,sha,1,1,"append-contract",1);
        try(var f=new Fixture(root,false,initial)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            var other=new DistributedSchema.Operator("other",0,type,sha,1,1,"append-contract",1); f.admittedOperators.add(other);
            var inserted=new java.util.concurrent.atomic.AtomicBoolean();
            f.beforeOperatorWrite=command -> {
                if(command.mutations().getFirst().key()[0]!=0x73 || !inserted.compareAndSet(false,true)) return;
                try {
                    var upload=OperatorUpload.create(UUID.randomUUID(),f.outer.cluster,f.outer.database,other,jar);
                    assertEquals(CommandResult.Status.SUCCESS,f.operators.execute(f.operators.prepareBegin(f.outer.operation(500),upload,null,f.operators.readBudget())).status());
                    assertEquals(CommandResult.Status.SUCCESS,f.operators.execute(f.operators.prepareChunk(f.outer.operation(501),upload,0,jar)).status());
                    assertEquals(CommandResult.Status.SUCCESS,f.operators.execute(f.operators.preparePublication(f.outer.operation(502),upload,null,null)).status());
                } catch(IOException failure) { throw new java.io.UncheckedIOException(failure); }
            };
            try(var client=f.client(journals)) {
                var handle=client.prepareUpload("append",type,jar,(name,className,hash,revision) -> initial); f.advance(client,handle);
                assertEquals(0,client.uploaded(handle).operator().version()); assertEquals(0L,client.checkOperator("append",sha)); assertEquals(0L,client.checkOperator("other",sha));
                var publicationAttempts=f.operatorWrites.stream().filter(bytes -> DistributedCommandCodec.decode(bytes).mutations().getFirst().key()[0]==0x73)
                        .map(DistributedCommandCodec::decode).filter(command -> OperatorUpload.decode(command.mutations().getFirst().value()).operator().name().equals("append")).toList();
                assertEquals(2,publicationAttempts.size());
                assertArrayEquals(publicationAttempts.getFirst().mutations().getFirst().value(),publicationAttempts.getLast().mutations().getFirst().value());
                assertNull(publicationAttempts.getFirst().predicates().get(2).expectedValue()); assertNotNull(publicationAttempts.getLast().predicates().get(2).expectedValue());
            }
        }
    }
    @Test void definiteNameConflictReallocatesButUnknownBeginReopensExactOriginalInputs() throws Exception {
        byte[] jar=jar(),sha=OperatorUpload.sha256(jar); String type=AppendOperator.class.getName();
        var initial=new DistributedSchema.Operator("append",0,type,sha,1,1,"append-contract",1);
        try(var f=new Fixture(root,false,initial)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            var resolutions=new ArrayList<Long>();
            RatisSchemaClient.TrustedOperatorResolver resolver=(name,className,hash,revision) -> {
                resolutions.add(revision); var operator=new DistributedSchema.Operator(name,revision,className,hash,1,1,"append-contract",1); f.admittedOperators.add(operator); return operator;
            };
            UUID request=UUID.randomUUID(),uploadId;
            try(var client=f.client(journals)) {
                var handle=client.prepareUpload(request,"append",type,jar,resolver); assertEquals(request,handle.requestId());
                uploadId=OperatorUpload.decode(RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("upload")).uploadId();
                var inserted=new java.util.concurrent.atomic.AtomicBoolean();
                f.beforeOperatorWrite=command -> {
                    if(command.mutations().getFirst().key()[0]!=0x70 || !inserted.compareAndSet(false,true)) return;
                    try {
                        var competitor=OperatorUpload.create(UUID.randomUUID(),f.outer.cluster,f.outer.database,initial,jar);
                        var begin=f.operators.prepareBegin(f.outer.operation(500),competitor,null,f.operators.readBudget());
                        assertEquals(CommandResult.Status.SUCCESS,f.operators.execute(begin).status());
                    } catch(IOException failure) { throw new java.io.UncheckedIOException(failure); }
                };
                client.advance(handle,1); assertEquals(RatisSchemaClient.Phase.OPERATOR_BEGIN,handle.phase());
                var revised=OperatorUpload.decode(RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("upload"));
                assertEquals(uploadId,revised.uploadId()); assertEquals(1,revised.operator().version()); assertEquals(List.of(0L,1L),resolutions);
                f.beforeOperatorWrite=command -> {}; f.advance(client,handle); assertEquals(1,client.uploaded(handle).operator().version());
            }
        }
        try(var f=new Fixture(root.resolve("unknown"),false,initial)) {
            Path journals=Files.createDirectory(root.resolve("unknown-journals")); UUID request=UUID.randomUUID(); byte[] frozen;
            RatisSchemaClient.TrustedOperatorResolver resolver=(name,className,hash,revision) -> initial;
            try(var client=f.client(journals)) {
                var handle=client.prepareUpload(request,"append",type,jar,resolver); f.loseOperatorBeginReply=true;
                assertThrows(IOException.class,() -> client.advance(handle,1)); frozen=RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("pendingCommand");
            }
            try(var client=f.client(journals)) {
                var handle=client.reopen(request,(name,className,hash,revision) -> { throw new AssertionError("Unknown BEGIN must retain original descriptor"); });
                f.advance(client,handle); assertEquals(0,client.uploaded(handle).operator().version());
                assertArrayEquals(frozen,f.operatorWrites.get(1)); assertEquals(0L,client.checkOperator("append",sha));
            }
        }
    }
    @Test void uploadCheckOperatorCreateMergeAndReopenUseTheCompleteDurableFrontend() throws Exception {
        byte[] jar=jar(); var operator=new DistributedSchema.Operator("append",0,AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
        try(var f=new Fixture(root,false,operator)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID created; long api;
            try(var client=f.client(journals)) {
                var upload=client.prepareUpload(operator,jar); f.advance(client,upload); assertTrue(client.checkOperator("append",0)); var published=client.uploaded(upload); assertNotNull(published);
                var plain=RatisSchemaClientTest.shape(); var shape=new it.cavallium.rockserver.core.common.ColumnSchema(plain.keys(),plain.variableTailKeys(),plain.hasValue(),operator.name(),operator.version(),operator.className());
                var handle=client.prepareCreate("merged",shape,published); created=handle.requestId(); f.advance(client,handle); api=client.completed(handle).columnId();
                byte[] counter=f.admin.readAllocationCounter();
                var repeated=client.prepareCreate("merged",shape);
                assertEquals(api,client.completed(repeated).columnId()); assertFalse(client.completed(repeated).changed());
                assertArrayEquals(counter,f.admin.readAllocationCounter());
                var column=f.peers.getFirst().store.installedSchema().column(api).columnId();
                for(int n=0;n<2;n++) { var pending=f.routed.prepare(new OperationId(new UUID(90,1),0,n),List.of(new Mutation(Mutation.Kind.MERGE,column,new byte[8],new byte[]{7}))); assertEquals(CommandResult.Status.SUCCESS,f.routed.execute(pending).status()); }
                assertArrayEquals(new byte[]{7,7},f.routed.read(column,new byte[8]));
            }
            try(var client=f.client(journals)) { assertEquals(api,client.completed(client.reopen(created)).columnId()); assertTrue(client.checkOperator("append",0)); }
        }
    }
    @Test void nativeCreatePutGetReopenThenDeleteEveryColumnAndPreserveAbsentDeleteOutcome() throws Exception {
        try(var f=new Fixture(root,false)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID request; long added;
            try(var client=f.client(journals)) {
                var handle=client.prepareCreate("added",RatisSchemaClientTest.shape()); request=handle.requestId(); f.advance(client,handle); added=client.completed(handle).columnId(); assertEquals(8,added);
                var schema=new RatisSchemaCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn).read(f.routed.directory().schemaDigest()); var column=schema.column(added).columnId();
                var pending=f.routed.prepare(new OperationId(new UUID(89,1),0,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[8],new byte[]{9}))); assertEquals(CommandResult.Status.SUCCESS,f.routed.execute(pending).status()); assertArrayEquals(new byte[]{9},f.routed.read(column,new byte[8]));
            }
            try(var client=f.client(journals)) {
                assertEquals(added,client.completed(client.reopen(request)).columnId());
                var delete=client.prepareDeleteIfExists("added"); f.advance(client,delete); var last=client.prepareDelete(7); f.advance(client,last);
                assertTrue(f.routed.directory().columns().isEmpty()); var absent=client.prepareDeleteIfExists("added"); assertFalse(client.completed(absent).changed());
            }
        }
    }
    @Test void unknownInstallRetainsOriginalBytesAndLaggingListenerDoesNotDelayQuorumResult() throws Exception {
        try(var f=new Fixture(root,true)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID request; f.lagging=true; f.losePhase="INSTALL";
            try(var client=f.client(journals)) { var h=client.prepareCreate("added",RatisSchemaClientTest.shape()); request=h.requestId(); assertThrows(IOException.class,() -> f.advance(client,h)); }
            try(var client=f.client(journals)) {
                var h=client.reopen(request); f.advance(client,h); assertTrue(client.completed(h).cleanupPending());
                for(int n=0;n<10;n++) client.advance(h,1); assertTrue(client.completed(h).cleanupPending()); f.lagging=false;
                for(int n=0;n<10 && h.phase()!=RatisSchemaClient.Phase.DONE;n++) client.advance(h,1); assertEquals(RatisSchemaClient.Phase.DONE,h.phase());
            }
        }
    }
    @Test void unknownFencingPublicationActivationAndRetirementReopenOriginalPendingInputs() throws Exception {
        for(String lostPhase:List.of("FENCING","PUBLISHED","ACTIVATE","RETIRED")) {
            Path path=Files.createDirectory(root.resolve(lostPhase));
            try(var f=new Fixture(path,false)) {
                Path journals=Files.createDirectory(path.resolve("journals")); UUID request; Map<String,byte[]> frozen; f.losePhase=lostPhase;
                try(var client=f.client(journals)) { var h=client.prepareCreate("added",RatisSchemaClientTest.shape()); request=h.requestId(); assertThrows(IOException.class,() -> f.advance(client,h)); frozen=RatisSchemaClientJournal.read(journals.resolve(request+".request")); }
                var allocation=new RatisColumnAllocation(f.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(new OperationId(UUID.randomUUID(),0,0),UUID.randomUUID(),new UUID(91,1),UUID.randomUUID(),allocation.readCounter())).status());
                try(var client=f.client(journals)) {
                    var h=client.reopen(request); var recovered=RatisSchemaClientJournal.read(journals.resolve(request+".request"));
                    for(var entry:frozen.entrySet()) if(entry.getKey().equals("pendingCommand") || entry.getKey().startsWith("activationRecord/")) assertArrayEquals(entry.getValue(),recovered.get(entry.getKey()));
                    f.advance(client,h); assertEquals(8,client.completed(h).columnId());
                }
            }
        }
    }
    @Test void originalDeleteByNameCannotFollowDeletedAndRecreatedColumn() throws Exception {
        try(var f=new Fixture(root,false)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID oldRequest;
            try(var client=f.client(journals)) {
                var old=client.prepareDeleteIfExists("existing"); oldRequest=old.requestId();
                var delete=client.prepareDeleteIfExists("existing"); f.advance(client,delete);
                var recreated=client.prepareCreate("existing",RatisSchemaClientTest.shape()); f.advance(client,recreated); assertEquals(8,client.completed(recreated).columnId());
            }
            try(var client=f.client(journals)) {
                var old=client.reopen(oldRequest); assertEquals(RatisSchemaClient.Phase.CONFLICT,client.advance(old,1).phase());
                var current=f.peers.getFirst().store.installedSchema().column("existing"); assertEquals(8,current.apiId()); assertNotEquals(f.outer.data,current.columnId());
            }
        }
    }
    @Test void abortConsumesAllocationAndCannotCrossUnknownCommittedFencing() throws Exception {
        try(var f=new Fixture(root,false)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            try(var client=f.client(journals)) {
                var prepared=client.prepareCreate("aborted",RatisSchemaClientTest.shape());
                for(int n=0;n<30 && prepared.phase()!=RatisSchemaClient.Phase.ADMISSIONS;n++) client.advance(prepared,1);
                assertEquals(RatisSchemaClient.Phase.ADMISSIONS,prepared.phase()); client.abort(prepared);
                assertTrue(client.completed(prepared).aborted()); assertFalse(client.completed(prepared).changed());
                for(int n=0;n<10 && prepared.phase()!=RatisSchemaClient.Phase.ABORTED;n++) client.advance(prepared,1);
                var next=client.prepareCreate("next",RatisSchemaClientTest.shape()); f.losePhase="FENCING";
                assertThrows(IOException.class,() -> f.advance(client,next)); client.abort(next);
                assertEquals(SchemaChange.Phase.FENCING,f.admin.readHead(f.initial.domainId()).status().phase());
                for(int n=0;n<3;n++) { try { client.abort(next); } catch(IllegalStateException fenced) { break; } }
                assertEquals(SchemaChange.Phase.FENCING,f.admin.readHead(f.initial.domainId()).status().phase());
                var allocation=new RatisColumnAllocation(f.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); assertEquals(9,allocation.read(next.requestId()).apiId());
            }
        }
    }
    @Test void retirementIntentIsNotCompletionUntilRetirementResultIsForced() throws Exception {
        try(var f=new Fixture(root,false)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            try(var client=f.client(journals)) {
                var h=client.prepareCreate("added",RatisSchemaClientTest.shape());
                for(int n=0;n<100 && h.phase()!=RatisSchemaClient.Phase.RETIRE;n++) client.advance(h,1);
                assertEquals(RatisSchemaClient.Phase.RETIRE,h.phase()); assertNull(client.completed(h));
                assertEquals(SchemaChange.Phase.PUBLISHED,f.admin.readHead(f.initial.domainId()).status().phase());
                while(h.phase()==RatisSchemaClient.Phase.RETIRE) client.advance(h,1);
                assertEquals(RatisSchemaClient.Phase.CLEANUP,h.phase()); assertNotNull(client.completed(h));
                assertEquals(SchemaChange.Phase.RETIRED,f.admin.readHead(f.initial.domainId()).status().phase());
            }
        }
    }
    @Test void failedRetirementCheckpointCannotExposeUnforcedSuccessFromMemory() throws Exception {
        try(var f=new Fixture(root,false)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            var clientRef=new java.util.concurrent.atomic.AtomicReference<RatisSchemaClient>(); var handleRef=new java.util.concurrent.atomic.AtomicReference<RatisSchemaClient.Handle>();
            UUID request;
            try(var client=new RatisSchemaClient(journals,f.outer.cluster,f.outer.database,f.initial.domainId(),f.catalogue,f.client,f.routed,f.replicas,f.operators,(file,fields) -> {
                if(new String(fields.get("phase"),java.nio.charset.StandardCharsets.UTF_8).equals("CLEANUP")) {
                    assertNull(clientRef.get().completed(handleRef.get()),"Busy owner must not expose the prospective checkpoint"); throw new IOException("Controlled retirement checkpoint failure");
                }
                RatisSchemaClientJournal.write(file,fields);
            })) {
                clientRef.set(client); var handle=client.prepareCreate("added",RatisSchemaClientTest.shape()); handleRef.set(handle); request=handle.requestId();
                assertThrows(IOException.class,() -> f.advance(client,handle)); assertEquals(RatisSchemaClient.Phase.CLEANUP,handle.phase()); assertNull(client.completed(handle)); assertNull(client.uploaded(handle));
                assertEquals("RETIRE",new String(RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("phase"),java.nio.charset.StandardCharsets.UTF_8));
            }
            try(var client=f.client(journals)) { var handle=client.reopen(request); assertNull(client.completed(handle)); f.advance(client,handle); assertNotNull(client.completed(handle)); }
        }
    }
    @Test void failedUploadCompletionCheckpointSuppressesUploadedManifest() throws Exception {
        byte[] jar=jar(); var operator=new DistributedSchema.Operator("append",0,AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
        try(var f=new Fixture(root,false,operator)) {
            Path journals=Files.createDirectory(root.resolve("journals")); var clientRef=new java.util.concurrent.atomic.AtomicReference<RatisSchemaClient>(); var handleRef=new java.util.concurrent.atomic.AtomicReference<RatisSchemaClient.Handle>(); UUID request;
            try(var client=new RatisSchemaClient(journals,f.outer.cluster,f.outer.database,f.initial.domainId(),f.catalogue,f.client,f.routed,f.replicas,f.operators,(file,fields) -> {
                if(new String(fields.get("phase"),java.nio.charset.StandardCharsets.UTF_8).equals("DONE")) {
                    assertNull(clientRef.get().uploaded(handleRef.get())); throw new IOException("Controlled upload completion checkpoint failure");
                }
                RatisSchemaClientJournal.write(file,fields);
            })) {
                clientRef.set(client); var handle=client.prepareUpload(operator,jar); handleRef.set(handle); request=handle.requestId();
                assertThrows(IOException.class,() -> f.advance(client,handle)); assertEquals(RatisSchemaClient.Phase.DONE,handle.phase()); assertNull(client.uploaded(handle)); assertNull(client.completed(handle));
            }
            try(var client=f.client(journals)) { var handle=client.reopen(request); assertNull(client.uploaded(handle)); f.advance(client,handle); assertNotNull(client.uploaded(handle)); }
        }
    }
    @Test void partialPreparedAbortCleansOriginalOwnerWithoutReservation() throws Exception {
        try(var f=new Fixture(root,true)) {
            Path journals=Files.createDirectory(root.resolve("journals"));
            try(var client=f.client(journals)) {
                var h=client.prepareCreate("partial",RatisSchemaClientTest.shape());
                for(int n=0;n<30 && h.phase()!=RatisSchemaClient.Phase.CAPACITY;n++) client.advance(h,1);
                client.advance(h,1); // Drain the BEGIN receipt acknowledgement.
                client.advance(h,1); // Admit only the first original owner.
                assertEquals(RatisSchemaClient.Phase.CAPACITY,h.phase()); client.abort(h);
                for(int n=0;n<20 && h.phase()!=RatisSchemaClient.Phase.ABORTED;n++) client.advance(h,1);
                assertEquals(RatisSchemaClient.Phase.ABORTED,h.phase()); assertFalse(client.completed(h).cleanupPending());
            }
        }
    }
    @Test void nonforcedCapacityCannotReachFencing() throws Exception {
        try(var f=new Fixture(root,false)) {
            Path journals=Files.createDirectory(root.resolve("journals")); f.nonforced=true;
            try(var client=f.client(journals)) { var h=client.prepareCreate("added",RatisSchemaClientTest.shape()); for(int n=0;n<20;n++) client.advance(h,1); assertEquals(RatisSchemaClient.Phase.CAPACITY,h.phase()); assertEquals(SchemaChange.Phase.PREPARED,f.admin.readHead(f.initial.domainId()).status().phase()); }
        }
    }
}
