package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.distributed.ratis.RatisRoutedClient.*;
import java.io.*;
import java.nio.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/** Immutable database-wide route authority. All transports and generation lifetimes are borrowed. */
public final class RatisTransactionBindings {
    public record Domain(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient client) {
        public Domain {Objects.requireNonNull(directory);Objects.requireNonNull(client);if(schema!=null){schema.requireMatches(directory);client.requireKnownSchema(schema);}}
    }
    private final List<Domain> domains;
    private final Map<UUID,Domain> byDomain,byGroup,byColumn;
    private final Map<Long,Domain> byApiColumn;
    private final UUID cluster,database;
    public static RatisTransactionBindings of(RatisRoutedClient client){return new RatisTransactionBindings(List.of(new Domain(null,client.directory(),client)));}
    public static RatisTransactionBindings of(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient client){return new RatisTransactionBindings(List.of(new Domain(schema,directory,client)));}
    public RatisTransactionBindings(List<Domain> input) {
        if(input.isEmpty() || input.size()>DomainBinding.MAX_DOMAINS)throw new IllegalArgumentException("Invalid transaction domain count");
        domains=input.stream().sorted(Comparator.comparing(d->d.directory().domainId(),RangeDirectoryCodec.UUID_ORDER)).toList();
        cluster=domains.getFirst().directory().clusterId();database=domains.getFirst().directory().databaseId();
        var domainIndex=new LinkedHashMap<UUID,Domain>();var groups=new HashMap<UUID,Domain>();var columns=new HashMap<UUID,Domain>();var api=new HashMap<Long,Domain>();long bytes=0;
        for(var domain:domains){var directory=domain.directory();if(!cluster.equals(directory.clusterId()) || !database.equals(directory.databaseId()) || domainIndex.putIfAbsent(directory.domainId(),domain)!=null)throw new IllegalArgumentException("Foreign or repeated transaction domain");
            bytes+=RangeDirectoryCodec.encodedSize(directory)+(domain.schema()==null?0:DistributedSchemaCodec.encodedSize(domain.schema()));if(bytes>16L*1024*1024)throw new IllegalArgumentException("Transaction binding metadata exceeds bound");
            for(var group:directory.ranges())if(groups.putIfAbsent(group.groupId(),domain)!=null)throw new IllegalArgumentException("Ambiguous transaction participant group");
            for(var column:directory.columns())if(columns.putIfAbsent(column,domain)!=null)throw new IllegalArgumentException("Ambiguous transaction column UUID");
            if(domain.schema()!=null)for(var column:domain.schema().columns())if(api.putIfAbsent(column.apiId(),domain)!=null)throw new IllegalArgumentException("Ambiguous transaction API column");
        }
        byDomain=Map.copyOf(domainIndex);byGroup=Map.copyOf(groups);byColumn=Map.copyOf(columns);byApiColumn=Map.copyOf(api);
    }
    public List<Domain> domains(){return domains;}
    public UUID clusterId(){return cluster;}public UUID databaseId(){return database;}
    public List<DomainBinding> authority(){return domains.stream().map(d->DomainBinding.of(d.directory())).toList();}
    public Domain domain(UUID id){var value=byDomain.get(id);if(value==null)throw new IllegalArgumentException("Unknown transaction domain");return value;}
    public Domain group(UUID id){var value=byGroup.get(id);if(value==null)throw new IllegalArgumentException("Unknown frozen transaction group");return value;}
    public Domain column(UUID id){var value=byColumn.get(id);if(value==null)throw new IllegalArgumentException("Unknown frozen transaction column");return value;}
    public DistributedSchema schema(long id){var value=byApiColumn.get(id);if(value==null || value.schema()==null)throw new IllegalArgumentException("Unknown transaction API column");return value.schema();}
    public List<DistributedSchema> schemas(){return domains.stream().map(d->Objects.requireNonNull(d.schema(),"Logical schema unavailable")).toList();}
    public List<RangeDirectory> directories(){return domains.stream().map(Domain::directory).toList();}
    public DistributedSchema schema(UUID column){var domain=column(column);if(domain.schema()==null)throw new IllegalArgumentException("Logical schema unavailable");return domain.schema();}
    public DistributedSchema.Column column(String name){
        Objects.requireNonNull(name);var matches=columns().stream().filter(column->column.name().equals(name)).toList();
        if(matches.size()!=1)throw new IllegalArgumentException("Unknown or ambiguous transaction column name");return matches.getFirst();
    }
    public DistributedSchema.Column column(long column){return schema(column).column(column);}
    public DistributedSchema.EncodedKey encodeLogicalKey(UUID column,it.cavallium.rockserver.core.common.Keys keys){return schema(column).encodeLogicalKey(column,keys);}
    public List<DistributedSchema.Column> columns(){return domains.stream().filter(d->d.schema()!=null).flatMap(d->d.schema().columns().stream()).toList();}
    public List<RatisBackendSessions.Target> targets(TransactionScope scope){return participants(scope).stream().map(route->new RatisBackendSessions.Target(client(route.groupId()),route.groupId(),directoryForGroup(route.groupId()))).toList();}
    public List<RangeDescriptor> participants(TransactionScope scope){return RatisSnapshotPlan.participants(directories(),scope);}
    public void preflight(TransactionScope scope){RatisSnapshotPlan.preflight(directories(),scope);}
    public TransactionScope scope(it.cavallium.rockserver.core.common.TransactionOpenScope declaration){
        var spans=new ArrayList<TransactionScope.Span>();for(var span:declaration.spans()){
            var schema=schema(span.columnId());var column=schema.column(span.columnId()).columnId();
            spans.add(span.point()?TransactionScope.Span.point(column,schema.encodeLogicalKey(column,span.lowerInclusive()).physicalKey()):TransactionScope.Span.range(column,RatisLogicalRangePages.bound(schema,column,span.lowerInclusive()),RatisLogicalRangePages.upperBound(schema,column,span.upperExclusive())));
        }var physical=new TransactionScope(spans);preflight(physical);return physical;
    }
    public RangeDirectory directoryForGroup(UUID id){return group(id).directory();}
    public RatisRoutedClient client(UUID group){return group(group).client();}
    public void require(List<DomainBinding> expected){if(!authority().equals(expected))throw new IllegalArgumentException("Frozen transaction binding vector changed");}
    public byte[] sessionIdentityDigest(){return digest(false);}public byte[] snapshotNamespaceDigest(){return digest(true);}
    private byte[] digest(boolean readOnly){try{var digest=MessageDigest.getInstance("SHA-256");for(var domain:domains){var binding=domain.directory();digest.update(RangeDirectoryCodec.encode(binding));digest.update(readOnly?domain.client().snapshotNamespaceDigest():domain.client().sessionIdentityDigest());}return digest.digest();}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    public CompletableFuture<RatisSnapshotReadiness> snapshotReadinessAsync(SnapshotFence fence,UUID group)  {return client(group).snapshotReadinessAsyncAt(directoryForGroup(group),fence,group);}
    public RatisSnapshotReadiness snapshotReadiness(SnapshotFence fence,UUID group) throws IOException  {return client(group).snapshotReadinessAt(directoryForGroup(group),fence,group);}
    public SnapshotTarget snapshotTarget(SnapshotFence fence,UUID group)throws IOException{return client(group).snapshotTargetAt(directoryForGroup(group),fence,group);}
    public CompletableFuture<SnapshotTarget> snapshotTargetAsync(SnapshotFence fence,UUID group)  {return client(group).snapshotTargetAsyncAt(directoryForGroup(group),fence,group);}
    public SnapshotTarget restoreSnapshotTarget(SnapshotFence fence,RangeDescriptor descriptor,ReplicaDescriptor replica,UUID incarnation)  {return client(descriptor.groupId()).restoreSnapshotTargetAt(directoryForGroup(descriptor.groupId()),fence,descriptor,replica,incarnation);}
    public byte[] snapshotGet(SnapshotTarget target,SnapshotFenceStatus.Capture token,UUID column,byte[] key) throws IOException  {return client(target.descriptor().groupId()).snapshotGet(target,token,column,key);}
    public PhysicalRangePage snapshotPage(SnapshotTarget target,SnapshotFenceStatus.Capture token,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException  {return client(target.descriptor().groupId()).snapshotPage(target,token,column,lower,upper,reverse,resume,items,bytes);}
    public CompletableFuture<Void> releaseSnapshotAsync(SnapshotTarget target,SnapshotFenceStatus.Capture token) throws IOException {return client(target.descriptor().groupId()).releaseSnapshotAsync(target,token);}
    public void releaseSnapshot(SnapshotTarget target,SnapshotFenceStatus.Capture token) throws IOException  {client(target.descriptor().groupId()).releaseSnapshot(target,token);}
    public PreparedSnapshotCapture prepareSnapshotCapture(SnapshotTarget target,UUID snapshotId,java.time.Duration ttl)  {return client(target.descriptor().groupId()).prepareSnapshotCapture(target,snapshotId,ttl);}
    public PreparedSnapshotCapture prepareWritableSnapshotCapture(SnapshotTarget target,UUID snapshotId,java.time.Duration ttl)  {return client(target.descriptor().groupId()).prepareWritableSnapshotCapture(target,snapshotId,ttl);}
    public PreparedSnapshotCapture restoreSnapshotCapture(SnapshotTarget target,RatisSnapshotCaptureRequest request)  {return client(target.descriptor().groupId()).restoreSnapshotCapture(target,request);}
    public SnapshotFenceStatus.Capture executeSnapshotCapture(PreparedSnapshotCapture operation) throws IOException  {return client(operation.target().descriptor().groupId()).executeSnapshotCapture(operation);}
    public RatisSnapshotCaptureRequest.Result executeWritableSnapshotCapture(PreparedSnapshotCapture operation) throws IOException  {return client(operation.target().descriptor().groupId()).executeWritableSnapshotCapture(operation);}
    public ScopedTransaction capturedTransaction(PreparedSnapshotCapture operation,RatisSnapshotCaptureRequest.Result result) throws IOException  {return client(operation.target().descriptor().groupId()).capturedTransaction(operation,result);}
    public PreparedSnapshotOperation restoreSnapshotGate(RatisSnapshotRecord request,RangeDescriptor descriptor)  {return client(descriptor.groupId()).restoreSnapshotGateAt(directoryForGroup(descriptor.groupId()),request,descriptor);}
    public CompletableFuture<RatisSnapshotRecord.Result> executeSnapshotGateAsync(PreparedSnapshotOperation operation)  {return client(operation.descriptor().groupId()).executeSnapshotGateAsync(operation);}
    public RatisSnapshotRecord.Result executeSnapshotGate(PreparedSnapshotOperation operation) throws IOException  {return client(operation.descriptor().groupId()).executeSnapshotGate(operation);}
    public void cancelSnapshotGate(PreparedSnapshotOperation operation) throws IOException  {client(operation.descriptor().groupId()).cancelSnapshotGate(operation);}
    public CompletableFuture<Boolean> helpSnapshotFenceAsync(SnapshotFence fence,UUID group)  {return client(group).helpSnapshotFenceAsyncAt(directoryForGroup(group),fence,group);}
    public boolean helpSnapshotFence(SnapshotFence fence,UUID group) throws IOException  {return client(group).helpSnapshotFenceAt(directoryForGroup(group),fence,group);}
    public CompletableFuture<SnapshotGate> snapshotGateAsync(SnapshotFence fence,UUID group)  {return client(group).snapshotGateAsyncAt(directoryForGroup(group),fence,group);}
    public SnapshotGate snapshotGate(SnapshotFence fence,UUID group) throws IOException  {return client(group).snapshotGateAt(directoryForGroup(group),fence,group);}
    public PreparedFinalizationOperation restoreFinalization(ParticipantFinalization finalization,RangeDescriptor descriptor)  {return client(descriptor.groupId()).restoreFinalizationAt(directoryForGroup(descriptor.groupId()),finalization,descriptor);}
    public ApplyPosition executeFinalization(PreparedFinalizationOperation operation) throws IOException  {return client(operation.descriptor().groupId()).executeFinalization(operation);}
    public PreparedParticipantOperation restoreParticipant(PreparedParticipant.Preparation preparation,RangeDescriptor descriptor)  {return client(descriptor.groupId()).restoreParticipantAt(directoryForGroup(descriptor.groupId()),preparation,descriptor,false);}
    public PreparedParticipantOperation restoreParticipant(PreparedParticipant.Preparation preparation,RangeDescriptor descriptor,boolean negative)  {return client(descriptor.groupId()).restoreParticipantAt(directoryForGroup(descriptor.groupId()),preparation,descriptor,negative);}
    public ParticipantVote executeParticipant(PreparedParticipantOperation operation) throws IOException  {return client(operation.descriptor().groupId()).executeParticipant(operation);}
    public PreparedParticipant preparedParticipant(UUID group,UUID transactionId) throws IOException  {return client(group).preparedParticipantAt(directoryForGroup(group),group,transactionId);}
    public ParticipantOutcome finalizedParticipant(UUID group,UUID transactionId) throws IOException  {return client(group).finalizedParticipant(directoryForGroup(group),directoryForGroup(group).ranges().stream().filter(route->route.groupId().equals(group)).findFirst().orElseThrow(),transactionId);}
    public CompletableFuture<SnapshotFenceProof> snapshotFenceAsync(SnapshotFence fence,UUID group)  {return client(group).snapshotFenceAsyncAt(directoryForGroup(group),fence,group);}
    public SnapshotFenceProof snapshotFence(SnapshotFence fence,UUID group) throws IOException  {return client(group).snapshotFenceAt(directoryForGroup(group),fence,group);}
    public ParticipantFenceProof participantFence(ParticipantIdentity identity) throws IOException  {return client(identity.participantGroup()).participantFence(directoryForGroup(identity.participantGroup()),directoryForGroup(identity.participantGroup()).ranges().stream().filter(route->route.groupId().equals(identity.participantGroup())).findFirst().orElseThrow(),identity);}
    public ScopedTransaction restoreTransaction(RatisDurableTransaction.Intent intent) throws IOException  {return client(intent.route().groupId()).restoreTransactionAt(directoryForGroup(intent.route().groupId()),intent);}
    CompletableFuture<RatisSnapshotCaptureRequest.Result> executeCaptureAsync(PreparedSnapshotCapture operation){return client(operation.target().descriptor().groupId()).executeCaptureAsync(operation);}
}
