package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Real sealed source ABSENT and durable HOME reservation; no server/network or accepted-plan authority claim. */
class RatisHomeReservationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(long n){return new UUID(0,n);}
    static Message message(byte[] bytes){return RatisTopologyCdcPollTest.message(bytes);}
    final class Fixture implements AutoCloseable {
        final RatisTopologyCdcPollTest.Fixture source;
        final UUID groupId=id(80),column=id(81),domain=id(82),controlSession=id(88);
        final RaftPeer peer=RaftPeer.newBuilder().setId("home").setAddress("127.0.0.1:9997").build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final DistributedStoragePool pool;final ReplicatedRangeStore store;final RaftStorage storage;final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=RatisProofQueryService.operationRerouteLane();final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicInteger sourceReads=new AtomicInteger();volatile Runnable beforeSource=()->{};
        final ReplicatedRangeStore nativeCatalogue;final UUID catalogueColumn=id(21);
        final DistributedCommand original;
        final OperationRerouteOrigin origin;final TransactionStatus reserved;final DistributedCommand reservation;long index;
        Fixture(Path path,boolean configured)throws Exception{this(path,configured,false);}
        Fixture(Path path,boolean configured,boolean production)throws Exception{
            source=new RatisTopologyCdcPollTest.Fixture(path.resolve("source"));source.seal(true);
            source.ledger.set(source.ledger.get().issue(controlSession,SessionEpochLedger.MAX_LIFETIME_MILLIS+10));
            pool=DistributedStoragePool.open(path.resolve("home-native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(domain,Set.of(column),null,null),new byte[32],Map.of(column,"home"),Set.of());
            if(production){
                nativeCatalogue=source.pool.openRange(new RangeOwnership.Owner(source.catalogue,1),new RangeOwnership.Interval(id(91),Set.of(catalogueColumn),null,null),new byte[32],Map.of(catalogueColumn,"catalogue"),Set.of());
                // Deliberately populate committed catalogue authority; this does not claim real target activation.
                var state=TopologyChange.State.begin(source.definition.header()).beginFencing().frozen(source.freeze.proof(source.local.replicaId()));
                for(var target:source.target.ranges())state=state.seeded(source.seed.proof(target.groupId()));state=state.publish();
                for(var target:source.target.ranges())state=state.activated(TopologyChange.Proof.of(TopologyChange.Stage.ACTIVATE,state.header(),target.groupId(),source.local.replicaId(),new ApplyPosition(1,1),new byte[32],source.seed.proof(target.groupId()).digest()));
                nativeCatalogue.apply(new ApplyPosition(1,nativeCatalogue.applied()==null ? 0 : nativeCatalogue.applied().index()+1),new DistributedCommand(source.catalogue,1,new byte[32],new OperationId(id(92),1,0),List.of(new Mutation(Mutation.Kind.PUT,catalogueColumn,RatisTopologyCatalogue.stateKey(source.definition.intentId()),state.encode()),new Mutation(Mutation.Kind.PUT,catalogueColumn,RatisTopologyCatalogue.definitionKey(source.definition.digest()),source.definition.encode()),new Mutation(Mutation.Kind.PUT,catalogueColumn,domainKey(source.domain),RangeDirectoryCodec.encode(source.target)))));
            }else nativeCatalogue=null;
            var replica=new ReplicaDescriptor(id(84),id(85),id(86),peer.getId().toString(),peer.getAddress(),ReplicaDescriptor.Role.VOTER);
            var directory=new RangeDirectory(source.cluster,source.database,domain,1,new byte[32],1,Set.of(column),0,List.of(new RangeDescriptor(id(83),groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);byte[] bytes=source.ledger.get().encode();return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(RaftGroupId.valueOf(source.catalogue)).setCallId(1).setSuccess(true).setMessage(message(ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,ByteBuffer.allocate(4+bytes.length).putInt(bytes.length).put(bytes).array()))).build();
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->{if(method.getName().equals("getGroupId"))return RaftGroupId.valueOf(source.catalogue);if(method.getName().equals("io"))return io;throw new AssertionError(method);});
            var epochs=new RatisSessionEpochs(client,source.cluster,source.database,source.catalogue,1,new byte[32],id(21));
            machine=RatisRangeStateMachine.withSessions(store,group,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),RatisTransactionStatuses.validator(source.cluster,source.database,groupId,1,new byte[32],column),epochs,directory,RatisRangeStateMachine.RetentionLimits.defaults(),leader::get);
            machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value)->RatisTransactionStatuses.validateRecord(source.cluster,source.database,key,value));machine.homeRecoveryReservations(source.cluster,source.database,column);machine.proofQueries(queries);machine.operationRerouteQueries(queries);
            if(production)machine.operationRerouteProofAdmission(new RatisOperationRerouteAdmission(new RatisOperationRerouteAdmission.Authority(){
                public TopologyChange.State intent(UUID intent)throws IOException{return TopologyChange.State.decode(cataloguePoint(RatisTopologyCatalogue.stateKey(intent)));}
                public TopologyChange.Definition definition(byte[] digest)throws IOException{return TopologyChange.Definition.decode(cataloguePoint(RatisTopologyCatalogue.definitionKey(digest)));}
                public RangeDirectory directory(UUID domain)throws IOException{return RangeDirectoryCodec.decode(cataloguePoint(domainKey(domain)));}
                public RatisTopologyOperationOutcomeRpc.Reply outcome(RangeDirectory sourceDirectory,RangeDescriptor descriptor,DistributedCommand command)throws IOException{assertEquals(source.parent,descriptor);assertArrayEquals(source.directory.payloadDigest(),sourceDirectory.payloadDigest());return sourceOutcome(command);}
            },source.catalogue)::validate);
            else if(configured)machine.operationRerouteAdmission(command->RatisTransactionStatuses.admitReservation(command,entry->{sourceReads.incrementAndGet();beforeSource.run();var request=new RatisTopologyOperationOutcomeRpc.Request(entry.clusterId(),entry.databaseId(),entry.domainId(),entry.source(),entry.operation(),entry.originalReceiptDigest());try{return RatisTopologyOperationOutcomeRpc.decodeReply(source.machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException(failure);}}),null);
            storage=RaftStorage.newBuilder().setDirectory(path.resolve("home-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "home-reservation-fixture";throw new AssertionError(method);});
            machine.initialize(identity,group.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);
            for(var grant:source.ledger.get().grants())apply(DistributedCommand.sessionControl(groupId,1,new byte[32],new OperationId(controlSession,2,400+grant.epoch()),RangeSessionControl.bind(source.ledger.get(),grant,4096),-1));
            original=new DistributedCommand(source.groupId,1,source.schema.digest(),new OperationId(source.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,source.column,new byte[]{10},new byte[]{7}),new Mutation(Mutation.Kind.PUT,source.column,new byte[]{(byte)200},new byte[]{8})));
            origin=new OperationRerouteOrigin(source.cluster,source.database,source.domain,new DistributedCdcCheckpoint.Lineage(source.rangeId,source.groupId,1),original.operationId(),original.receiptDigest(),OperatorUpload.sha256(source.freeze.encode()),source.store.applied(),source.target.payloadDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(original,source.target),RatisOperationRerouteAdmissionRpc.targetParticipantCount(original,source.target),new OperationId(controlSession,2,0));
            reserved=TransactionStatus.reserve(origin,source.schema.schemaEpoch(),source.schema.digest());reservation=command(origin.firstControl(),null,reserved);
        }
        byte[] cataloguePoint(byte[] key)throws IOException{try{return nativeCatalogue.readCurrentPoint(catalogueColumn,key);}catch(org.rocksdb.RocksDBException failure){throw new IOException(failure);}}
        RatisTopologyOperationOutcomeRpc.Reply sourceOutcome(DistributedCommand command)throws IOException{sourceReads.incrementAndGet();var request=new RatisTopologyOperationOutcomeRpc.Request(source.cluster,source.database,source.domain,new DistributedCdcCheckpoint.Lineage(source.rangeId,source.groupId,1),command.operationId(),command.receiptDigest());try{return RatisTopologyOperationOutcomeRpc.decodeReply(source.machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException(failure);}}
        RatisOperationRerouteAdmissionRpc.Evidence evidence()throws IOException{return new RatisOperationRerouteAdmissionRpc.Evidence(original,source.directory,source.target,sourceOutcome(original),List.of());}
        DistributedCommand command(OperationId control,TransactionStatus old,TransactionStatus next){byte[] key=ByteBuffer.allocate(16).putLong(next.transactionId().getMostSignificantBits()).putLong(next.transactionId().getLeastSignificantBits()).array();return new DistributedCommand(groupId,1,new byte[32],control,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,old==null?null:old.encode())));}
        TransactionContext propose(DistributedCommand command)throws IOException{return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(DistributedCommandCodec.encode(command))).build());}
        byte[] apply(DistributedCommand command)throws Exception{var tx=propose(command);assertNull(tx.getException());machine.preAppendTransaction(tx);tx.initLogEntry(leader.get().term(),++index);return machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray();}
        CompletableFuture<Message> prepare(DistributedCommand command){return machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.prepareRequest(command)));}
        TransactionStatus read()throws Exception{return TransactionStatus.decode(store.readCurrentPoint(column,reservation.mutations().getFirst().key()));}
        @Override public void close()throws Exception{queries.stop().get(5,TimeUnit.SECONDS);machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();pool.close();source.close();}
    }
    static byte[] domainKey(UUID id){return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();}
    @Test void productionEvidenceUsesRetainedNativeCatalogueAndFreshSealedSource()throws Exception{
        try(var f=new Fixture(root.resolve("production"),true,true)){
            var evidence=f.evidence();var decodedState=TopologyChange.State.decode(f.cataloguePoint(RatisTopologyCatalogue.stateKey(f.source.definition.intentId())));assertNotSame(decodedState.header(),evidence.absence().freeze().header());assertArrayEquals(decodedState.header().encode(),evidence.absence().freeze().header().encode());byte[] encoded=evidence.encode();assertArrayEquals(encoded,RatisOperationRerouteAdmissionRpc.Evidence.decode(encoded).encode());
            var movedReplica=new ReplicaDescriptor(id(101),id(102),id(103),"prior-parent","127.0.0.1:9996",ReplicaDescriptor.Role.VOTER);
            var oldParent=new RangeDescriptor(f.source.rangeId,f.source.groupId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(movedReplica));
            var earlierDirectory=new RangeDirectory(f.source.cluster,f.source.database,f.source.domain,1,f.source.schema.digest(),0,Set.of(f.source.column),0,List.of(oldParent));
            // Earlier caller membership is never used to address95; committed definition supplies actual peers.
            evidence=new RatisOperationRerouteAdmissionRpc.Evidence(f.original,earlierDirectory,evidence.targetDirectory(),evidence.absence(),List.of());
            byte[] request=RatisOperationRerouteAdmissionRpc.prepareRequest(f.reservation,evidence);var decoded=RatisOperationRerouteAdmissionRpc.decodeRequest(request);assertArrayEquals(request,RatisOperationRerouteAdmissionRpc.prepareRequest(decoded.command(),decoded.evidence()));
            assertThrows(IllegalArgumentException.class,()->RatisOperationRerouteAdmissionRpc.Evidence.decode(Arrays.copyOf(encoded,encoded.length-1)));
            assertThrows(ExecutionException.class,()->f.prepare(f.reservation).get(5,TimeUnit.SECONDS));
            RatisOperationRerouteAdmissionRpc.requireEcho(f.machine.queryLinearizable(message(request)).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisOperationRerouteAdmissionRpc.PREPARE,f.reservation);assertEquals(2,f.sourceReads.get());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(f.reservation)).status());assertTrue(f.read().reserved());assertEquals(0,f.source.store.activeSnapshotPins());
        }
    }
    record ReservedEvidence(TransactionStatus status,RatisOperationRerouteAdmissionRpc.Evidence evidence) {}
    ReservedEvidence reserveOriginal(Fixture f,DistributedCommand original)throws Exception {
        var origin=new OperationRerouteOrigin(f.source.cluster,f.source.database,f.source.domain,new DistributedCdcCheckpoint.Lineage(f.source.rangeId,f.source.groupId,1),original.operationId(),original.receiptDigest(),OperatorUpload.sha256(f.source.freeze.encode()),f.source.store.applied(),f.source.target.payloadDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(original,f.source.target),RatisOperationRerouteAdmissionRpc.targetParticipantCount(original,f.source.target),f.origin.firstControl());
        var reserved=TransactionStatus.reserve(origin,f.source.schema.schemaEpoch(),f.source.schema.digest());var command=f.command(origin.firstControl(),null,reserved);
        var evidence=new RatisOperationRerouteAdmissionRpc.Evidence(original,f.source.directory,f.source.target,f.sourceOutcome(original),List.of());prepareEvidence(f,command,evidence);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(command)).status());assertEquals(reserved,f.read());return new ReservedEvidence(reserved,evidence);
    }
    void prepareEvidence(Fixture f,DistributedCommand command,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws Exception {
        byte[] reply=f.machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.prepareRequest(command,evidence))).get(5,TimeUnit.SECONDS).getContent().toByteArray();RatisOperationRerouteAdmissionRpc.requireEcho(reply,RatisOperationRerouteAdmissionRpc.PREPARE,command);
    }
    RatisTransactionPlan.Accepted controlledAccepted(Fixture f,ReservedEvidence base) {
        var plan=RatisTransactionPlan.create(f.source.target,base.evidence().original().operationId(),base.evidence().original().mutations(),base.evidence().original().predicates());var accepted=new HashMap<UUID,DistributedCommand>();var captures=new HashMap<UUID,UUID>();var prepares=new HashMap<UUID,OperationId>();var finalizes=new HashMap<UUID,OperationId>();int n=0;
        // These canonical accepted images exercise HOME normalization, not live child DATA certification.
        for(var part:plan.partitions()){
            var values=part.command().mutations().stream().filter(m->m.returnMode()!=Mutation.ReturnMode.NONE).map(Mutation::value).toList();
            var audit=new TransactionAcceptance(new ApplyPosition(1,50+n),2,id(200+n),part.scope(),new CommandResult(CommandResult.Status.SUCCESS,values).encode());UUID group=part.route().groupId();accepted.put(group,DistributedCommand.acceptedTransaction(part.command(),audit));captures.put(group,id(300+n));prepares.put(group,RatisOperationRerouteAdmissionRpc.control(base.status().origin(),n));finalizes.put(group,RatisOperationRerouteAdmissionRpc.control(base.status().origin(),64+n++));
        }
        return plan.accept(base.status().transactionId(),accepted,prepares,finalizes,captures);
    }
    TransactionStatus initialFromIntents(Fixture f,ReservedEvidence base,List<RatisDurableTransaction.Intent> intents){
        var participants=intents.stream().map(i->new TransactionStatus.Participant(i.route().rangeId(),i.route().groupId(),i.route().rangeEpoch(),f.source.schema.digest(),i.acceptedBusiness().payloadDigest(),null,false)).toList();return TransactionStatus.begin(f.source.cluster,f.source.database,f.source.domain,base.status().transactionId(),base.status().operation(),base.status().schemaEpoch(),base.status().schemaDigest(),RatisDurableTransaction.planDigest(intents),participants);
    }
    RatisOperationRerouteAdmissionRpc.Evidence promotionEvidence(ReservedEvidence base,List<RatisDurableTransaction.Intent> intents){var e=base.evidence();return new RatisOperationRerouteAdmissionRpc.Evidence(e.original(),e.sourceDirectory(),e.targetDirectory(),e.absence(),intents);}
    void rejectPromotion(Fixture f,ReservedEvidence base,List<RatisDurableTransaction.Intent> intents)throws Exception {
        var next=base.status().promote(initialFromIntents(f,base,intents));var command=f.command(RatisOperationRerouteAdmissionRpc.control(base.status().origin(),128),base.status(),next);var evidence=promotionEvidence(base,intents);
        try {byte[] request=RatisOperationRerouteAdmissionRpc.prepareRequest(command,evidence);assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message(request)).get(5,TimeUnit.SECONDS));}catch(IllegalArgumentException malformed){/* Pure wire/identity checks can reject before authority. */}
        assertNotNull(f.propose(command).getException());assertEquals(base.status(),f.read());
    }
    @Test void productionPromotionNormalizesExactAcceptedProjectionAndGlobalResults()throws Exception {
        try(var f=new Fixture(root.resolve("qualified-promotion"),true,true)){
            var original=new DistributedCommand(f.source.groupId,1,f.source.schema.digest(),f.original.operationId(),List.of(new Mutation(Mutation.Kind.PUT,f.source.column,new byte[]{(byte)200},new byte[]{20},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,f.source.column,new byte[]{10},new byte[]{10},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,f.source.column,new byte[]{(byte)201},new byte[]{21},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,f.source.column,new byte[]{11},new byte[]{11},Mutation.ReturnMode.CURRENT)));
            var base=reserveOriginal(f,original);var accepted=controlledAccepted(f,base);var intents=accepted.intents();assertEquals(List.of(1,3),intents.getFirst().resultPositions());assertEquals(List.of(0,2),intents.getLast().resultPositions());
            var first=intents.getFirst();var audit=first.acceptedBusiness().acceptance();
            var wide=new TransactionScope(List.of(TransactionScope.Span.range(f.source.column,null,new byte[]{(byte)128})));
            var changedScope=DistributedCommand.acceptedTransaction(new DistributedCommand(first.route().groupId(),first.route().rangeEpoch(),f.source.schema.digest(),original.operationId(),first.acceptedBusiness().mutations(),first.acceptedBusiness().predicates()),new TransactionAcceptance(audit.snapshot(),audit.leaderGeneration(),audit.ownerIncarnation(),wide,audit.result()));
            var changed=new ArrayList<>(intents);changed.set(0,new RatisDurableTransaction.Intent(first.route(),changedScope,first.prepareControl(),first.finalizeControl(),first.resultPositions(),first.captureId()));rejectPromotion(f,base,changed);
            var writes=new ArrayList<>(first.acceptedBusiness().mutations());var old=writes.getFirst();writes.set(0,new Mutation(old.kind(),old.columnId(),old.key(),new byte[]{99},old.returnMode()));
            var changedWrites=DistributedCommand.acceptedTransaction(new DistributedCommand(first.route().groupId(),first.route().rangeEpoch(),f.source.schema.digest(),original.operationId(),writes,first.acceptedBusiness().predicates()),audit);
            changed=new ArrayList<>(intents);changed.set(0,new RatisDurableTransaction.Intent(first.route(),changedWrites,first.prepareControl(),first.finalizeControl(),first.resultPositions(),first.captureId()));rejectPromotion(f,base,changed);
            var changedBusiness=DistributedCommand.acceptedTransaction(new DistributedCommand(first.route().groupId(),first.route().rangeEpoch(),f.source.schema.digest(),new OperationId(original.operationId().sessionId(),original.operationId().epoch(),5),first.acceptedBusiness().mutations(),first.acceptedBusiness().predicates()),audit);
            changed=new ArrayList<>(intents);changed.set(0,new RatisDurableTransaction.Intent(first.route(),changedBusiness,first.prepareControl(),first.finalizeControl(),first.resultPositions(),first.captureId()));rejectPromotion(f,base,changed);
            changed=new ArrayList<>(intents);changed.set(0,new RatisDurableTransaction.Intent(first.route(),first.acceptedBusiness(),RatisOperationRerouteAdmissionRpc.control(base.status().origin(),3),first.finalizeControl(),first.resultPositions(),first.captureId()));rejectPromotion(f,base,changed);
            changed=new ArrayList<>(intents);changed.set(0,new RatisDurableTransaction.Intent(first.route(),first.acceptedBusiness(),first.prepareControl(),first.finalizeControl(),List.of(0,3),first.captureId()));rejectPromotion(f,base,changed);
            var foreign=new RangeDescriptor(id(400),id(401),1,1,first.route().lowerInclusive(),first.route().upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,first.route().replicas());var foreignBusiness=DistributedCommand.acceptedTransaction(new DistributedCommand(foreign.groupId(),1,f.source.schema.digest(),original.operationId(),first.acceptedBusiness().mutations(),first.acceptedBusiness().predicates()),audit);
            changed=new ArrayList<>(intents);changed.set(0,new RatisDurableTransaction.Intent(foreign,foreignBusiness,first.prepareControl(),first.finalizeControl(),first.resultPositions(),first.captureId()));rejectPromotion(f,base,changed);
            var promoted=base.status().promote(accepted.initial());var command=f.command(RatisOperationRerouteAdmissionRpc.control(base.status().origin(),128),base.status(),promoted);var evidence=promotionEvidence(base,intents);
            var target=f.source.target;var changedTarget=new RangeDirectory(target.clusterId(),target.databaseId(),target.domainId(),target.schemaEpoch(),target.schemaDigest(),target.generation()+1,target.columns(),target.routingPrefixBytes(),target.ranges());var wrongTarget=new RatisOperationRerouteAdmissionRpc.Evidence(original,evidence.sourceDirectory(),changedTarget,evidence.absence(),intents);
            assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.prepareRequest(command,wrongTarget))).get(5,TimeUnit.SECONDS));assertEquals(base.status(),f.read());
            byte[] request=RatisOperationRerouteAdmissionRpc.prepareRequest(command,evidence);var decoded=RatisOperationRerouteAdmissionRpc.decodeRequest(request);assertArrayEquals(request,RatisOperationRerouteAdmissionRpc.prepareRequest(decoded.command(),decoded.evidence()));prepareEvidence(f,command,evidence);
            byte[] result=f.apply(command);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result).status());assertArrayEquals(promoted.encode(),f.read().encode());assertEquals(1,f.read().generation());assertEquals(base.status().origin(),f.read().origin());int reads=f.sourceReads.get();prepareEvidence(f,command,evidence);assertArrayEquals(result,f.apply(command));assertEquals(reads,f.sourceReads.get());assertEquals(0,f.source.child.activeSnapshotPins());assertTrue(f.source.child.topologyBirthPending());
        }
    }
    @Test void acceptedOriginalWriteAndReadCertificationReserveButNeverPromote()throws Exception {
        for(boolean readCertification:List.of(false,true))try(var f=new Fixture(root.resolve(readCertification?"accepted-read":"accepted-write"),true,true)){
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(f.source.column,new byte[]{10}),TransactionScope.Span.point(f.source.column,new byte[]{(byte)200})));var audit=new TransactionAcceptance(new ApplyPosition(1,1),1,id(500),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode());
            var original=readCertification?DistributedCommand.readCertification(f.source.groupId,1,f.source.schema.digest(),f.original.operationId(),audit):DistributedCommand.acceptedTransaction(f.original,audit);var base=reserveOriginal(f,original);
            assertTrue(base.evidence().intents().isEmpty());assertArrayEquals(DistributedCommandCodec.encode(original),DistributedCommandCodec.encode(RatisOperationRerouteAdmissionRpc.Evidence.decode(base.evidence().encode()).original()));
            var accepted=controlledAccepted(f,readCertification?new ReservedEvidence(base.status(),new RatisOperationRerouteAdmissionRpc.Evidence(f.original,base.evidence().sourceDirectory(),base.evidence().targetDirectory(),base.evidence().absence(),List.of())):base);
            var promoted=base.status().promote(accepted.initial());var command=f.command(RatisOperationRerouteAdmissionRpc.control(base.status().origin(),128),base.status(),promoted);
            // Accepted original input has no target capture authorization. Even hash-consistent next metadata stays closed.
            assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.prepareRequest(command,base.evidence()))).get(5,TimeUnit.SECONDS));assertNotNull(f.propose(command).getException());assertEquals(base.status(),f.read());assertEquals(0,f.source.child.activeSnapshotPins());assertTrue(f.source.child.topologyBirthPending());
        }
    }
    @Test void originalDeleteRangeCannotBeClippedIntoSourceOwnership()throws Exception{
        try(var f=new Fixture(root.resolve("bounds"),true)){
            var bounded=new RangeDescriptor(f.source.rangeId,f.source.groupId,1,1,new byte[]{10},new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(f.source.local));
            var crossing=new DistributedCommand(f.source.groupId,1,f.source.schema.digest(),f.origin.operation(),List.of(new Mutation(Mutation.Kind.DELETE_RANGE,f.source.column,new byte[]{10},new byte[]{(byte)200})));
            assertThrows(IOException.class,()->RatisOperationRerouteAdmission.requireOriginalOwnership(crossing,bounded,Set.of(f.source.column)));
            var exact=new DistributedCommand(f.source.groupId,1,f.source.schema.digest(),f.origin.operation(),List.of(new Mutation(Mutation.Kind.DELETE_RANGE,f.source.column,new byte[]{10},new byte[]{(byte)128})));
            RatisOperationRerouteAdmission.requireOriginalOwnership(exact,bounded,Set.of(f.source.column));
        }
    }
    @Test void reservationNeedsFreshAbsentTicketAndOriginalReceiptRetrySkipsSource()throws Exception{
        try(var f=new Fixture(root,true)){
            assertNotNull(f.propose(f.reservation).getException());assertThrows(ExecutionException.class,()->f.machine.query(message(RatisOperationRerouteAdmissionRpc.prepareRequest(f.reservation))).get(5,TimeUnit.SECONDS));
            RatisOperationRerouteAdmissionRpc.requireEcho(f.prepare(f.reservation).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisOperationRerouteAdmissionRpc.PREPARE,f.reservation);
            byte[] result=f.apply(f.reservation);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result).status());assertTrue(f.read().reserved());assertEquals(f.origin,f.read().origin());assertEquals(1,f.sourceReads.get());
            f.beforeSource=()->{throw new AssertionError("Saved reservation must not query source again");};f.prepare(f.reservation).get(5,TimeUnit.SECONDS);assertArrayEquals(result,f.apply(f.reservation));assertEquals(1,f.sourceReads.get());
            assertTrue(f.source.child.topologyBirthPending());assertEquals(0,f.source.store.activeSnapshotPins());
        }
    }
    @Test void unconfiguredReservationAndHashOnlyPromotionStayClosed()throws Exception{
        try(var f=new Fixture(root.resolve("closed"),false)){assertThrows(ExecutionException.class,()->f.prepare(f.reservation).get(5,TimeUnit.SECONDS));assertNotNull(f.propose(f.reservation).getException());}
        try(var f=new Fixture(root.resolve("promotion"),true)){
            f.prepare(f.reservation).get(5,TimeUnit.SECONDS);f.apply(f.reservation);var participants=new ArrayList<TransactionStatus.Participant>();
            for(var target:f.source.definition.targets())participants.add(new TransactionStatus.Participant(target.rangeId(),target.groupId(),target.rangeEpoch(),f.source.schema.digest(),new byte[32],null,false));
            var accepted=TransactionStatus.begin(f.origin.clusterId(),f.origin.databaseId(),f.origin.domainId(),f.origin.transactionId(),f.origin.operation(),f.reserved.schemaEpoch(),f.reserved.schemaDigest(),new byte[32],participants);
            var promotion=f.command(new OperationId(f.origin.firstControl().sessionId(),f.origin.firstControl().epoch(),128),f.reserved,f.reserved.promote(accepted));
            assertThrows(ExecutionException.class,()->f.prepare(promotion).get(5,TimeUnit.SECONDS));assertNotNull(f.propose(promotion).getException());assertEquals(f.reserved,f.read());
        }
    }
    @Test void queuedCancelledAdmissionAndConfigurationLossRetainActualOwnershipUntilReturn()throws Exception{
        try(var f=new Fixture(root,true)){
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);f.beforeSource=()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Fixture timeout");}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}};
            try{var future=f.prepare(f.reservation);assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(future.cancel(false));assertEquals(1,f.queries.stats().jobs());
                f.machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.cancelRequest(f.reservation))).get(5,TimeUnit.SECONDS);assertNotNull(f.propose(f.reservation).getException());assertEquals(1,f.queries.stats().jobs());release.countDown();f.queries.stop().get(5,TimeUnit.SECONDS);assertEquals(0,f.queries.stats().jobs());
            }finally{release.countDown();}
        }
        try(var f=new Fixture(root.resolve("configuration"),true)){
            f.prepare(f.reservation).get(5,TimeUnit.SECONDS);var tx=f.propose(f.reservation);assertNull(tx.getException());f.machine.notifyConfigurationChanged(1,f.index+1,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());
            assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,()->f.machine.preAppendTransaction(tx));assertNotNull(f.propose(f.reservation).getException());
        }
    }
}
