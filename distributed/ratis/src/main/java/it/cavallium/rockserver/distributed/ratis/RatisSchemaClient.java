package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;

/** Caller-driven durable schema administration. All transports remain borrowed. */
public final class RatisSchemaClient implements AutoCloseable {
    public static final int MAX_HANDLES=8, MAX_REQUESTS=1024;
    public enum Phase { ALLOCATE, SCHEMA, DEFINITION, BEGIN, ARTIFACTS, CAPACITY, ADMISSIONS, FENCING, FENCE, INSTALL, PUBLISH, ACTIVATE, RETIRE, CLEANUP, DONE, ABORTED, CONFLICT, OPERATOR_BEGIN, OPERATOR_CHUNKS, OPERATOR_PUBLISH }
    public record Result(long columnId,boolean changed,boolean aborted,boolean cleanupPending) {}
    public record Progress(UUID requestId,Phase phase,Result result) {}
    /** Trusted local policy supplies codecs and deterministic contract for the exact requested allocation. */
    @FunctionalInterface public interface TrustedOperatorResolver {
        DistributedSchema.Operator resolve(String name,String className,byte[] sha256,long requestedRevision) throws IOException;
    }
    private final Path root;
    private final UUID cluster,database,domain;
    private final RaftClient client;
    private final RatisNodePlan.Catalogue binding;
    private final RatisCatalogue catalogue;
    private final RatisSchemaCatalogue schemas;
    private final RatisSchemaAdministration administration;
    private final RatisColumnAllocation allocation;
    private final RatisRoutedClient routed;
    private final RatisSchemaReplicaClient replicas;
    private final RatisOperatorCatalogue operators;
    @FunctionalInterface interface Checkpoint { void write(Path path,Map<String,byte[]> fields) throws IOException; }
    private final Checkpoint checkpoint;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Map<UUID,Handle> handles=new HashMap<>();
    private boolean closed,busy,fenced;
    public final class Handle implements AutoCloseable {
        private final UUID id; private final Map<String,byte[]> fields; private boolean released;
        private TrustedOperatorResolver operatorResolver;
        private Handle(UUID id,Map<String,byte[]> fields) { this.id=id; this.fields=fields; }
        public UUID requestId() { return id; }
        public Phase phase() { return Phase.valueOf(text("phase")); }
        private String text(String key) { return new String(fields.get(key),StandardCharsets.UTF_8); }
        private long number(String key) { return ByteBuffer.wrap(fields.get(key)).getLong(); }
        private void text(String key,String value) { fields.put(key,value.getBytes(StandardCharsets.UTF_8)); }
        private void number(String key,long value) { fields.put(key,ByteBuffer.allocate(8).putLong(value).array()); }
        @Override public void close() throws IOException { synchronized(RatisSchemaClient.this) { if (busy) throw new IOException("Schema client I/O still active"); released=true; handles.remove(id,this); } }
    }
    public RatisSchemaClient(Path root,UUID cluster,UUID database,UUID domain,RatisNodePlan.Catalogue binding,RaftClient client,RatisRoutedClient routed,RatisSchemaReplicaClient replicas,RatisOperatorCatalogue operators) throws IOException {
        this(root,cluster,database,domain,binding,client,routed,replicas,operators,RatisSchemaClientJournal::write);
    }
    RatisSchemaClient(Path root,UUID cluster,UUID database,UUID domain,RatisNodePlan.Catalogue binding,RaftClient client,RatisRoutedClient routed,RatisSchemaReplicaClient replicas,RatisOperatorCatalogue operators,Checkpoint checkpoint) throws IOException {
        this.checkpoint=Objects.requireNonNull(checkpoint);
        this.root=root.toAbsolutePath(); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.domain=Objects.requireNonNull(domain);
        this.binding=Objects.requireNonNull(binding); this.client=Objects.requireNonNull(client); this.routed=Objects.requireNonNull(routed); this.replicas=Objects.requireNonNull(replicas); this.operators=operators;
        var directory=routed.directory(); if (!directory.clusterId().equals(cluster) || !directory.databaseId().equals(database) || !directory.domainId().equals(domain)) throw new IllegalArgumentException("Schema client namespace mismatch");
        if (!Files.isDirectory(this.root)) throw new IOException("Schema journal root absent");
        catalogue=new RatisCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        schemas=new RatisSchemaCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        administration=new RatisSchemaAdministration(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        allocation=new RatisColumnAllocation(client,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        lockChannel=FileChannel.open(this.root.resolve("schema-client.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        try { FileLock acquired; try { acquired=lockChannel.tryLock(); } catch(OverlappingFileLockException owned) { throw new IOException("Schema client root already owned",owned); } if (acquired==null) throw new IOException("Schema client root already owned"); lock=acquired; }
        catch (IOException|RuntimeException failure) { lockChannel.close(); throw failure; }
    }
    public Handle prepareCreate(String name,ColumnSchema schema) throws IOException { return prepareCreate(name,schema,null); }
    public Handle prepareCreate(String name,ColumnSchema schema,OperatorUpload upload) throws IOException { return prepareCreate(UUID.randomUUID(),name,schema,upload); }
    public Handle prepareCreate(UUID request,String name,ColumnSchema schema) throws IOException { return prepareCreate(request,name,schema,null); }
    public synchronized Handle prepareCreate(UUID request,String name,ColumnSchema schema,OperatorUpload upload) throws IOException {
        requireNewRequest(request); var source=source(); var old=source.schema.columns().stream().filter(column -> column.name().equals(name)).findFirst().orElse(null);
        if (old!=null) {
            if (!old.schema().equals(schema) || upload!=null && !Objects.equals(old.operator(),upload.operator())) throw new IllegalArgumentException("Existing column definition differs");
            return initial(request,source,"CREATE",old.columnId(),old.apiId(),null,null,null,false);
        }
        if (source.schema.columns().size()>=RangeDirectoryCodec.MAX_COLUMNS) throw new IllegalArgumentException("Schema column capacity exhausted");
        UUID column=UUID.randomUUID(); var desired=new DistributedSchema(cluster,database,domain,source.schema.schemaEpoch()+1,source.schema.routingPrefixBytes(),List.of(new DistributedSchema.Column(1,column,name,schema,upload==null ? null : upload.operator())));
        if (upload!=null) {
            if (operators==null) throw new IOException("Operator catalogue unavailable"); var published=operators.readPublished(upload.operator().name(),upload.operator().version());
            if (published==null || !Arrays.equals(published.encode(),upload.encode())) throw new IOException("Operator upload is not published");
        }
        return initial(request,source,"CREATE",column,0,DistributedSchemaCodec.encode(desired),upload,null,true);
    }
    public Handle prepareUpload(DistributedSchema.Operator operator,byte[] artifact) throws IOException { return prepareUpload(UUID.randomUUID(),operator,artifact); }
    public synchronized Handle prepareUpload(UUID request,DistributedSchema.Operator operator,byte[] artifact) throws IOException {
        requireNewRequest(request); if (operators==null) throw new IOException("Operator catalogue unavailable");
        var upload=OperatorUpload.create(UUID.randomUUID(),cluster,database,operator,artifact); var source=source();
        return initial(request,source,"UPLOAD",UUID.randomUUID(),0,null,upload,artifact,true);
    }
    public Handle prepareUpload(String name,String className,byte[] artifact,TrustedOperatorResolver resolver) throws IOException {
        return prepareUpload(UUID.randomUUID(),name,className,artifact,resolver);
    }
    public synchronized Handle prepareUpload(UUID request,String name,String className,byte[] artifact,TrustedOperatorResolver resolver) throws IOException {
        requireNewRequest(request); Objects.requireNonNull(resolver);
        if (operators==null) throw new IOException("Operator catalogue unavailable");
        if (name==null || name.isBlank() || className==null || className.isBlank() || artifact==null || artifact.length==0 || artifact.length>OperatorUpload.MAX_ARTIFACT_BYTES) throw new IllegalArgumentException("Invalid common operator upload");
        byte[] sha=OperatorUpload.sha256(artifact); var published=operators.readByNameSha(name,sha); var source=source();
        if (published!=null) return initial(request,source,"UPLOAD",UUID.randomUUID(),0,null,published,null,false);
        var previous=operators.readNameHead(name); long revision=previous==null ? 0 : Math.addExact(previous.revision(),1);
        var operator=resolveOperator(resolver,name,className,sha,revision);
        var upload=OperatorUpload.create(UUID.randomUUID(),cluster,database,operator,artifact);
        var fields=new HashMap<String,byte[]>(); fields.put("commonUpload",new byte[]{1});
        if (previous!=null) fields.put("operatorNameWitness",previous.encode());
        return initial(request,source,"UPLOAD",UUID.randomUUID(),0,null,upload,artifact,true,fields,resolver);
    }
    private static DistributedSchema.Operator resolveOperator(TrustedOperatorResolver resolver,String name,String className,byte[] sha,long revision) throws IOException {
        var operator=Objects.requireNonNull(resolver.resolve(name,className,sha.clone(),revision),"Trusted operator resolver returned no descriptor");
        if (!operator.name().equals(name) || !operator.className().equals(className) || operator.version()!=revision || !Arrays.equals(operator.artifactSha256(),sha)) throw new IllegalArgumentException("Trusted operator descriptor differs from exact name/class/SHA/allocation");
        return operator;
    }
    public synchronized Long checkOperator(String name,byte[] sha) throws IOException {
        requireIdle(); if (operators==null) return null; var upload=operators.readByNameSha(name,sha); return upload==null ? null : upload.operator().version();
    }
    public synchronized boolean checkOperator(String name,long revision) throws IOException {
        requireIdle(); if (operators==null) return false; var upload=operators.readPublished(name,revision); if(upload==null) return false;
        var chunks=new ArrayList<byte[]>(); for(int i=0;i<upload.chunks().size();i++) chunks.add(operators.readChunk(upload,i)); upload.requireArtifact(chunks); return true;
    }
    public Handle prepareDelete(long apiId) throws IOException { return prepareDelete(UUID.randomUUID(),apiId); }
    public synchronized Handle prepareDelete(UUID request,long apiId) throws IOException {
        requireNewRequest(request); var source=source(); var column=source.schema.column(apiId); return initial(request,source,"DELETE",column.columnId(),column.apiId(),null,null,null,true);
    }
    public Handle prepareDeleteIfExists(String name) throws IOException { return prepareDeleteIfExists(UUID.randomUUID(),name); }
    public synchronized Handle prepareDeleteIfExists(UUID request,String name) throws IOException {
        requireNewRequest(request); var source=source(); var column=source.schema.columns().stream().filter(c -> c.name().equals(name)).findFirst().orElse(null);
        return initial(request,source,"DELETE",column==null ? UUID.randomUUID() : column.columnId(),column==null ? 0 : column.apiId(),null,null,null,column!=null);
    }
    private void requireNewRequest(UUID request) throws IOException {
        requireIdle(); Objects.requireNonNull(request);
        if (handles.containsKey(request) || Files.exists(path(request))) throw new IOException("Schema request already exists; reopen original request");
    }
    private record Source(RangeDirectory directory,DistributedSchema schema) {}
    private Source source() throws IOException {
        var directory=catalogue.read(domain); if (directory==null) throw new IOException("Domain directory absent"); var schema=schemas.read(directory.schemaDigest());
        if (schema==null) throw new IOException("Published schema absent"); schema.requireMatches(directory); return new Source(directory,schema);
    }
    private Handle initial(UUID request,Source source,String kind,UUID column,long api,byte[] desired,OperatorUpload upload,byte[] artifact,boolean changed) throws IOException {
        return initial(request,source,kind,column,api,desired,upload,artifact,changed,Map.of(),null);
    }
    private Handle initial(UUID request,Source source,String kind,UUID column,long api,byte[] desired,OperatorUpload upload,byte[] artifact,boolean changed,Map<String,byte[]> extra,TrustedOperatorResolver resolver) throws IOException {
        if (handles.size()>=MAX_HANDLES) throw new IOException("Schema handles exhausted");
        try (var files=Files.list(root)) { if (files.filter(p -> p.getFileName().toString().endsWith(".request")).count()>=MAX_REQUESTS) throw new IOException("Schema durable request capacity exhausted"); }
        var h=new Handle(request,new HashMap<>()); h.operatorResolver=resolver; extra.forEach((key,value) -> h.fields.put(key,value.clone())); var identity=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(identity,request); h.fields.put("requestId",identity.array()); h.text("namespace",cluster+"/"+database+"/"+domain); h.fields.put("catalogueBinding",catalogueBinding()); h.text("kind",kind); h.text("column",column.toString());
        h.number("api",api); h.number("sequence",0); h.number("cursor",0); h.number("uploadCursor",0); h.number("changed",changed ? 1 : 0); h.number("aborted",0); h.text("phase",!changed ? Phase.DONE.name() : kind.equals("CREATE") ? Phase.ALLOCATE.name() : kind.equals("UPLOAD") ? Phase.OPERATOR_BEGIN.name() : Phase.SCHEMA.name());
        h.fields.put("sourceDirectory",RangeDirectoryCodec.encode(source.directory)); h.fields.put("sourceSchema",DistributedSchemaCodec.encode(source.schema));
        if (desired!=null) h.fields.put("desired",desired); if (upload!=null) h.fields.put("upload",upload.encode()); if (artifact!=null) h.fields.put("artifact",artifact.clone()); save(h); handles.put(request,h); return h;
    }
    public synchronized Handle reopen(UUID request) throws IOException {
        requireIdle(); if (handles.containsKey(request)) return handles.get(request); if (handles.size()>=MAX_HANDLES) throw new IOException("Schema handles exhausted");
        var h=new Handle(request,RatisSchemaClientJournal.read(path(request)));
        try { validateRecovered(h); } catch(RuntimeException malformed) { throw new IOException("Invalid owned schema request journal",malformed); }
        handles.put(request,h); return h;
    }
    public synchronized Handle reopen(UUID request,TrustedOperatorResolver resolver) throws IOException {
        var handle=reopen(request); handle.operatorResolver=Objects.requireNonNull(resolver); return handle;
    }
    public synchronized Result completed(Handle handle) { requireHandle(handle); return busy ? null : result(handle); }
    private Result result(Handle handle) {
        if(closed || fenced || !lock.isValid() || handle.phase()!=Phase.DONE && handle.phase()!=Phase.CLEANUP && handle.phase()!=Phase.ABORTED) return null;
        refreshCompleted(handle); return new Result(handle.number("api"),handle.number("changed")!=0 && handle.number("aborted")==0,handle.number("aborted")!=0,handle.phase()==Phase.CLEANUP);
    }
    public synchronized OperatorUpload uploaded(Handle handle) { requireHandle(handle); return !busy && !closed && !fenced && lock.isValid() && handle.text("kind").equals("UPLOAD") && handle.phase()==Phase.DONE ? OperatorUpload.decode(handle.fields.get(handle.fields.containsKey("publishedUpload") ? "publishedUpload" : "upload")) : null; }
    private void refreshCompleted(Handle h) {
        if(h.number("aborted")!=0 || !h.fields.containsKey("definition") || h.phase()!=Phase.CLEANUP && h.phase()!=Phase.DONE) return;
        var first=definition(h).sourceDirectory().ranges().getFirst().groupId(); var bytes=h.fields.get("activation/"+first); if(bytes!=null) routed.refreshSchema(definition(h),NativeSchemaActivationProof.decode(bytes));
    }
    public Progress advance(Handle handle,int maxVisits) throws IOException {
        if (maxVisits<1 || maxVisits>32) throw new IllegalArgumentException("Invalid schema visit budget");
        synchronized(this) { requireIdle(); requireHandle(handle); busy=true; }
        try { for (int visit=0;visit<maxVisits;visit++) { Phase before=handle.phase(); if ((before==Phase.DONE || before==Phase.ABORTED || before==Phase.CONFLICT) && !handle.fields.containsKey("pendingAck")) break; step(handle); } return new Progress(handle.id,handle.phase(),result(handle)); }
        finally { synchronized(this) { busy=false; notifyAll(); } }
    }
    private SchemaChange.Definition definition(Handle h) { return SchemaChange.Definition.decode(h.fields.get("definition")); }
    private RangeDirectory sourceDirectory(Handle h) { return RangeDirectoryCodec.decode(h.fields.get("sourceDirectory")); }
    private DistributedSchema sourceSchema(Handle h) { return DistributedSchemaCodec.decode(h.fields.get("sourceSchema")); }
    private UUID column(Handle h) { return UUID.fromString(h.text("column")); }
    private OperationId next(Handle h) { long n=h.number("sequence"); if (n>=32 && h.phase().ordinal()<Phase.BEGIN.ordinal()) throw new IllegalStateException("Schema preparation attempt budget exhausted"); h.number("sequence",n+1); return new OperationId(h.id,0,n); }
    private SchemaAdmissionManifest manifest(Handle h) { return SchemaAdmissionManifest.decode(h.fields.get("manifest"),definition(h)); }
    private List<RatisSchemaReplicaRpc.Target> owners(Handle h) {
        var definition=definition(h); var result=new ArrayList<RatisSchemaReplicaRpc.Target>();
        for (var range:definition.sourceDirectory().ranges()) result.addAll(RatisSchemaReplicaClient.targets(new RatisSchemaReplicaRpc.Binding(definition,range.groupId(),physical(h),merge(h)))); return result;
    }
    private String physical(Handle h) { return h.text("kind").equals("CREATE") ? "schema_"+column(h).toString().replace("-","") : null; }
    private boolean merge(Handle h) { return h.fields.containsKey("upload"); }
    private RatisSchemaAdministration.Head head(Handle h) throws IOException {
        var definition=definition(h); var head=administration.readHead(domain);
        if (head!=null && Arrays.equals(head.status().header().encode(),definition.header().encode())) return head;
        var outcome=administration.readOutcome(domain,definition.digest()); if (outcome!=null) return outcome; throw new IOException("Schema administration no longer owns original head");
    }
    @FunctionalInterface private interface Prepare { DistributedCommand get() throws IOException; }
    @FunctionalInterface private interface Execute { CommandResult run(DistributedCommand command) throws IOException; }
    private CommandResult command(Handle h,Prepare prepare,Execute execute) throws IOException {
        if (!h.fields.containsKey("pendingCommand")) { h.fields.put("pendingCommand",DistributedCommandCodec.encode(prepare.get())); save(h); }
        if (!h.fields.containsKey("pendingResult")) {
            var frozen=DistributedCommandCodec.decode(h.fields.get("pendingCommand")); byte[] result=execute.run(frozen).encode();
            h.fields.put("pendingResult",result); h.fields.put("command/"+frozen.operationId().sequence(),h.fields.get("pendingCommand").clone()); h.fields.put("result/"+frozen.operationId().sequence(),result.clone()); save(h);
        }
        return CommandResult.decode(h.fields.get("pendingResult"));
    }
    private boolean success(Handle h,CommandResult result,Phase next) throws IOException {
        if (result.status()==CommandResult.Status.SUCCESS) { phase(h,next); return true; }
        phase(h,Phase.CONFLICT); return false;
    }
    private void phase(Handle h,Phase phase) throws IOException {
        if (!h.fields.containsKey("pendingAck")) {
            long consumed=-1;
            if(h.phase().ordinal()>=Phase.BEGIN.ordinal() && h.fields.containsKey("pendingCommand")) consumed=DistributedCommandCodec.decode(h.fields.get("pendingCommand")).operationId().sequence();
            if(phase==Phase.CONFLICT) for(var key:h.fields.keySet()) if(key.startsWith("result/")) consumed=Math.max(consumed,Long.parseLong(key.substring(7)));
            if(consumed>=0) {
                // A terminal preparation conflict consumes no reserved BEGIN identity; normal DEFINITION→BEGIN inserts no ACK.
                long sequence=h.number("sequence"); h.number("sequence",sequence+1);
                var ack=new DistributedCommand(binding.groupId(),binding.epoch(),binding.schemaDigest(),new OperationId(h.id,0,sequence),List.of(),List.of(),consumed); h.fields.put("pendingAck",DistributedCommandCodec.encode(ack));
            }
        }
        h.text("phase",phase.name()); h.fields.remove("pendingCommand"); h.fields.remove("pendingResult"); h.fields.remove("pendingReplica"); h.number("cursor",0); save(h); }
    private RatisSchemaReplicaRpc.Reply replica(Handle h,RatisSchemaReplicaRpc.Request request) throws IOException {
        if (!h.fields.containsKey("pendingReplica")) { h.fields.put("pendingReplica",request.encode()); save(h); }
        var frozen=RatisSchemaReplicaRpc.request(h.fields.get("pendingReplica")); if (!frozen.equals(request)) throw new IOException("Original replica operation is still unresolved");
        return replicas.execute(frozen);
    }
    private void replicaDone(Handle h) throws IOException { h.fields.remove("pendingReplica"); save(h); }
    private void step(Handle h) throws IOException {
        if (h.fields.containsKey("pendingAck")) { acknowledge(h); return; }
        if (h.fields.containsKey("abortRequested")) { finishAbort(h); return; }
        switch(h.phase()) {
            case ALLOCATE -> {
                var result=command(h,() -> allocation.prepare(next(h),h.id,domain,column(h),allocation.readCounter()),allocation::execute);
                if (result.status()==CommandResult.Status.CONFLICT) { h.fields.remove("pendingCommand"); h.fields.remove("pendingResult"); save(h); return; }
                if (result.status()!=CommandResult.Status.SUCCESS) { phase(h,Phase.CONFLICT); return; }
                var reservation=allocation.read(h.id); if (reservation==null) throw new IOException("Successful allocation lacks original reservation"); reservation.requireOwner(domain,column(h),reservation.apiId()); h.number("api",reservation.apiId()); h.fields.put("reservation",reservation.encode()); phase(h,Phase.SCHEMA);
            }
            case SCHEMA -> {
                if(!h.fields.containsKey("pendingCommand")) { var current=catalogue.read(domain); if(current==null || !Arrays.equals(current.payloadDigest(),sourceDirectory(h).payloadDigest())) { phase(h,Phase.CONFLICT); return; } }
                if (!h.fields.containsKey("targetSchema")) {
                    var source=sourceSchema(h); var columns=new ArrayList<>(source.columns());
                    if (h.text("kind").equals("CREATE")) { var desired=DistributedSchemaCodec.decode(h.fields.get("desired")).columns().getFirst(); columns.add(new DistributedSchema.Column(h.number("api"),column(h),desired.name(),desired.schema(),desired.operator())); }
                    else columns.removeIf(c -> c.columnId().equals(column(h)));
                    var target=new DistributedSchema(cluster,database,domain,source.schemaEpoch()+1,source.routingPrefixBytes(),columns); h.fields.put("targetSchema",DistributedSchemaCodec.encode(target)); save(h);
                }
                var targetSchema=DistributedSchemaCodec.decode(h.fields.get("targetSchema")); var result=command(h,() -> {
                    var uuids=new HashMap<UUID,byte[]>(); var ids=new HashMap<Long,byte[]>(); for (var c:targetSchema.columns()) { byte[] u=administration.readColumnIdentity(c.columnId()),a=administration.readApiIdentity(c.apiId()); if (u!=null) uuids.put(c.columnId(),u); if (a!=null) ids.put(c.apiId(),a); }
                    var reservations=new HashMap<UUID,RatisColumnAllocation.Reservation>(); if (h.fields.containsKey("reservation") && !uuids.containsKey(column(h))) reservations.put(column(h),RatisColumnAllocation.Reservation.decode(h.id,h.fields.get("reservation")));
                    return schemas.prepare(next(h),targetSchema,allocation.readCounter(),uuids,ids,reservations);
                },schemas::execute);
                if (result.status()==CommandResult.Status.CONFLICT) {
                    var published=schemas.read(targetSchema.digest());
                    if (published!=null && Arrays.equals(DistributedSchemaCodec.encode(published),DistributedSchemaCodec.encode(targetSchema))) phase(h,Phase.DEFINITION);
                    else { h.fields.remove("pendingCommand"); h.fields.remove("pendingResult"); save(h); }
                } else success(h,result,Phase.DEFINITION);
            }
            case DEFINITION -> {
                if(!h.fields.containsKey("pendingCommand")) { var current=catalogue.read(domain); if(current==null || !Arrays.equals(current.payloadDigest(),sourceDirectory(h).payloadDigest())) { phase(h,Phase.CONFLICT); return; } }
                if (!h.fields.containsKey("definition")) { h.fields.put("definition",new SchemaChange.Definition(h.id,new OperationId(h.id,0,h.number("sequence")+1),sourceDirectory(h),sourceSchema(h),DistributedSchemaCodec.decode(h.fields.get("targetSchema"))).encode()); save(h); }
                success(h,command(h,() -> { var d=definition(h); return administration.prepareDefinition(next(h),d,administration.readHead(domain),administration.readColumnIdentity(column(h)),administration.readApiIdentity(h.number("api"))); },administration::execute),Phase.BEGIN);
            }
            case BEGIN -> {
                var result=command(h,() -> administration.prepareBegin(definition(h),administration.readHead(domain),administration.readColumnIdentity(column(h)),administration.readApiIdentity(h.number("api"))),administration::execute);
                h.number("sequence",definition(h).beginOperation().sequence()+1); success(h,result,merge(h) ? Phase.ARTIFACTS : Phase.CAPACITY);
            }
            case ARTIFACTS -> artifacts(h);
            case CAPACITY -> capacities(h);
            case ADMISSIONS -> success(h,command(h,() -> administration.prepareAdmissions(next(h),head(h),definition(h),manifest(h)),administration::execute),Phase.FENCING);
            case FENCING -> {
                if (!h.fields.containsKey("pendingCommand")) {
                    var all=owners(h); int i=(int)h.number("cursor"); if (i<all.size()) { var owner=all.get(i); var observed=replica(h,RatisSchemaReplicaClient.capacity(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE,owner));
                    if (!observed.capacityActive() || !observed.capacity().equals(manifest(h).replica(owner.binding().groupId(),owner.replica().replicaId()).reservation())) { replicaDone(h); h.number("cursor",0); save(h); return; }
                    replicaDone(h); h.number("cursor",i+1); save(h); if (i+1<all.size()) return; }
                }
                success(h,command(h,() -> administration.prepareFencing(next(h),head(h),definition(h),manifest(h)),administration::executeFencing),Phase.FENCE);
            }
            case FENCE -> groupProof(h,false);
            case INSTALL -> groupProof(h,true);
            case PUBLISH -> success(h,command(h,() -> administration.preparePublication(next(h),head(h),definition(h)),administration::execute),Phase.ACTIVATE);
            case ACTIVATE -> activate(h);
            case RETIRE -> success(h,command(h,() -> administration.prepareAdvance(next(h),head(h),head(h).status().retire()),administration::execute),Phase.CLEANUP);
            case CLEANUP -> cleanup(h);
            case OPERATOR_BEGIN -> operatorBegin(h);
            case OPERATOR_CHUNKS -> {
                var upload=OperatorUpload.decode(h.fields.get("upload")); int index=(int)h.number("uploadCursor");
                if (index==upload.chunks().size()) { phase(h,Phase.OPERATOR_PUBLISH); return; }
                byte[] artifact=h.fields.get("artifact"),chunk=Arrays.copyOfRange(artifact,index*OperatorUpload.CHUNK_BYTES,Math.min(artifact.length,(index+1)*OperatorUpload.CHUNK_BYTES));
                var result=command(h,() -> operators.prepareChunk(next(h),upload,index,chunk),operators::execute);
                if(result.status()==CommandResult.Status.SUCCESS) { h.number("uploadCursor",index+1); phase(h,Phase.OPERATOR_CHUNKS); } else phase(h,Phase.CONFLICT);
            }
            case OPERATOR_PUBLISH -> operatorPublish(h);
            default -> throw new IllegalStateException("Unexpected schema client phase");
        }
    }
    private void artifacts(Handle h) throws IOException {
        var all=owners(h); int index=(int)h.number("cursor"); var target=all.get(index); var upload=OperatorUpload.decode(h.fields.get("upload")); String key="artifact/"+target.binding().groupId()+"/"+target.replica().replicaId();
        int step=h.fields.containsKey(key) ? (int)ByteBuffer.wrap(h.fields.get(key)).getLong() : -1;
        RatisSchemaReplicaRpc.Request request;
        if (step==-1) request=RatisSchemaReplicaClient.artifact(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN,target,upload);
        else if (step<upload.chunks().size()) request=RatisSchemaReplicaClient.chunk(target,upload,step,operators.readChunk(upload,step));
        else request=RatisSchemaReplicaClient.artifact(step==upload.chunks().size() ? ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH : ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE,target,upload);
        var reply=replica(h,request);
        if (step>upload.chunks().size() && !reply.artifactReady()) { replicaDone(h); return; }
        h.fields.put(key,ByteBuffer.allocate(8).putLong(step+1).array()); h.fields.remove("pendingReplica");
        if (step>upload.chunks().size()) { h.number("cursor",index+1); if(index+1==all.size()) { phase(h,Phase.CAPACITY); return; } }
        save(h);
    }
    private void capacities(Handle h) throws IOException {
        var all=owners(h); int i=(int)h.number("cursor"); var owner=all.get(i); var reply=replica(h,RatisSchemaReplicaClient.capacity(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE,owner));
        h.fields.remove("pendingReplica"); if (!reply.capacityActive()) { save(h); return; }
        h.fields.put("capacity/"+owner.binding().groupId()+"/"+owner.replica().replicaId(),reply.capacity().encode()); h.number("cursor",i+1);
        if(i+1==all.size()) {
            var bindings=new ArrayList<SchemaAdmissionManifest.Binding>(); var admitted=new ArrayList<SchemaAdmissionManifest.ReplicaAdmission>();
            for (var range:definition(h).sourceDirectory().ranges()) bindings.add(new SchemaAdmissionManifest.Binding(range.groupId(),physical(h),merge(h)));
            for(var target:all) admitted.add(new SchemaAdmissionManifest.ReplicaAdmission(target.binding().groupId(),target.replica().replicaId(),NativeSchemaCapacityReservation.decode(h.fields.get("capacity/"+target.binding().groupId()+"/"+target.replica().replicaId()))));
            h.fields.put("manifest",new SchemaAdmissionManifest(definition(h),bindings,admitted).encode()); phase(h,Phase.ADMISSIONS);
        } else save(h);
    }
    private void groupProof(Handle h,boolean install) throws IOException {
        if(h.fields.containsKey("pendingCommand")) { success(h,command(h,() -> { throw new AssertionError("Frozen report already exists"); },administration::execute),install ? Phase.INSTALL : Phase.FENCE); return; }
        var d=definition(h); var current=head(h); var proofs=install ? current.status().installed() : current.status().fenced();
        var range=d.sourceDirectory().ranges().stream().filter(r -> !proofs.containsKey(r.groupId())).findFirst().orElse(null);
        if(range==null) { phase(h,install ? Phase.PUBLISH : Phase.INSTALL); return; }
        UUID group=range.groupId(); var target=routed.schemaTarget(d,group,manifest(h)); String key=(install ? "install/" : "fence/")+group;
        ApplyPosition position;
        if (!h.fields.containsKey(key)) {
            h.text("pendingGroup",group.toString()); save(h);
            if(install) {
                var fence=NativeSchemaFence.decode(h.fields.get("fence/"+group)); var installTarget=routed.schemaInstallTarget(target,fence,physical(h),merge(h));
                h.fields.put(key,routed.installSchema(installTarget).encode());
            } else h.fields.put(key,routed.fenceSchema(target).encode());
            h.fields.remove("pendingGroup"); save(h);
        }
        position=install ? NativeSchemaInstallProof.decode(h.fields.get(key)).position() : NativeSchemaFence.decode(h.fields.get(key)).position();
        var result=command(h,() -> { var before=head(h); return administration.prepareAdvance(next(h),before,install ? before.status().installed(group,position) : before.status().fenced(group,position)); },administration::execute);
        success(h,result,install ? Phase.INSTALL : Phase.FENCE);
    }
    private void activate(Handle h) throws IOException {
        if(h.fields.containsKey("pendingCommand")) { success(h,command(h,() -> { throw new AssertionError("Frozen activation report already exists"); },administration::execute),Phase.ACTIVATE); return; }
        var d=definition(h); var current=head(h); var range=d.sourceDirectory().ranges().stream().filter(r -> !current.status().activated().containsKey(r.groupId())).findFirst().orElse(null);
        if (range==null) { phase(h,Phase.RETIRE); return; }
        UUID group=range.groupId(); var target=routed.schemaTarget(d,group,manifest(h)); var installation=NativeSchemaInstallProof.decode(h.fields.get("install/"+group)); var activation=routed.schemaActivationTarget(target,installation);
        String recordKey="activationRecord/"+group,proofKey="activation/"+group;
        if (!h.fields.containsKey(recordKey)) { h.fields.put(recordKey,routed.prepareSchemaActivation(activation).encode()); save(h); }
        if (!h.fields.containsKey(proofKey)) { h.fields.put(proofKey,routed.appendSchemaActivation(activation,RatisSchemaActivateRecord.decode(h.fields.get(recordKey))).encode()); save(h); }
        var proof=NativeSchemaActivationProof.decode(h.fields.get(proofKey));
        success(h,command(h,() -> { var before=head(h); return administration.prepareAdvance(next(h),before,before.status().activated(group,proof.position())); },administration::execute),Phase.ACTIVATE);
    }
    private void operatorPublish(Handle h) throws IOException {
        var upload=OperatorUpload.decode(h.fields.get("upload"));
        var result=command(h,() -> operators.preparePublication(next(h),upload,operators.readBySha(upload.operator().artifactSha256()),
                operators.readByNameSha(upload.operator().name(),upload.operator().artifactSha256())),operators::execute);
        if (result.status()==CommandResult.Status.SUCCESS) { phase(h,Phase.DONE); return; }
        if (result.status()!=CommandResult.Status.CONFLICT || !h.fields.containsKey("commonUpload")) { phase(h,Phase.CONFLICT); return; }
        var published=operators.readByNameSha(upload.operator().name(),upload.operator().artifactSha256());
        if (published!=null) { h.fields.put("publishedUpload",published.encode()); h.number("changed",0); phase(h,Phase.DONE); }
        else phase(h,Phase.OPERATOR_PUBLISH); // Only a forced no-effects result permits new immutable lookup witnesses.
    }
    private void operatorBegin(Handle h) throws IOException {
        var upload=OperatorUpload.decode(h.fields.get("upload")); boolean common=h.fields.containsKey("commonUpload");
        var result=command(h,() -> operators.prepareBegin(next(h),upload,common
                ? (h.fields.containsKey("operatorNameWitness") ? RatisOperatorCatalogue.NameHead.decode(h.fields.get("operatorNameWitness")) : null)
                : operators.readNameHead(upload.operator().name()),operators.readBudget()),operators::execute);
        if (result.status()==CommandResult.Status.SUCCESS) { phase(h,Phase.OPERATOR_CHUNKS); return; }
        if (result.status()!=CommandResult.Status.CONFLICT || !common) { phase(h,Phase.CONFLICT); return; }
        // The forced CONFLICT receipt proves this original BEGIN had no effects. Unknown replies retain its exact manifest/witness/operation.
        var published=operators.readByNameSha(upload.operator().name(),upload.operator().artifactSha256());
        if (published!=null) {
            h.fields.put("publishedUpload",published.encode()); h.number("changed",0); phase(h,Phase.DONE); return;
        }
        if (h.operatorResolver==null) throw new IOException("Confirmed operator allocation conflict requires the original trusted resolver");
        var previous=operators.readNameHead(upload.operator().name()); long revision=previous==null ? 0 : Math.addExact(previous.revision(),1);
        var operator=resolveOperator(h.operatorResolver,upload.operator().name(),upload.operator().className(),upload.operator().artifactSha256(),revision);
        var revised=OperatorUpload.create(upload.uploadId(),cluster,database,operator,h.fields.get("artifact"));
        h.fields.put("upload",revised.encode());
        if (previous==null) h.fields.remove("operatorNameWitness"); else h.fields.put("operatorNameWitness",previous.encode());
        phase(h,Phase.OPERATOR_BEGIN);
    }
    private void cleanup(Handle h) throws IOException {
        var all=owners(h); int index=(int)(h.number("cursor")%all.size()); var owner=all.get(index); String key="released/"+owner.binding().groupId()+"/"+owner.replica().replicaId();
        if(!h.fields.containsKey(key)) {
            try {
                var reply=replicas.execute(RatisSchemaReplicaClient.capacity(ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE,owner));
                if(h.number("aborted")!=0 && reply.capacityState()==RatisSchemaReplicaRpc.CapacityState.NONE || reply.forcedDurable() && reply.capacity()!=null && reply.capacityState()!=RatisSchemaReplicaRpc.CapacityState.ACTIVE && reply.capacityState()!=RatisSchemaReplicaRpc.CapacityState.FENCED && reply.capacityState()!=RatisSchemaReplicaRpc.CapacityState.NONE) h.fields.put(key,new byte[]{1});
            } catch(IOException unavailable) { h.number("cursor",index+1); save(h); return; }
        }
        h.number("cursor",index+1); save(h);
        if(all.stream().allMatch(target -> h.fields.containsKey("released/"+target.binding().groupId()+"/"+target.replica().replicaId()))) phase(h,h.number("aborted")!=0 ? Phase.ABORTED : Phase.DONE);
    }
    public Progress abort(Handle h) throws IOException {
        synchronized(this) { requireIdle(); requireHandle(h); busy=true; }
        try {
            if (h.fields.containsKey("pendingAck")) { acknowledge(h); return new Progress(h.id,h.phase(),result(h)); }
            if (h.phase()==Phase.ABORTED || h.number("aborted")!=0) return new Progress(h.id,h.phase(),result(h));
            if (h.fields.containsKey("pendingCommand")) { step(h); return new Progress(h.id,h.phase(),result(h)); }
            if (!h.fields.containsKey("definition") || h.phase().ordinal()<Phase.BEGIN.ordinal()) throw new IllegalStateException("Abort requires committed PREPARED administration");
            var before=head(h); if(before.status().phase()!=SchemaChange.Phase.PREPARED) throw new IllegalStateException("Schema fencing already started");
            h.fields.remove("pendingReplica"); h.fields.put("abortRequested",new byte[]{1}); save(h); finishAbort(h);
            return new Progress(h.id,h.phase(),result(h));
        } finally { synchronized(this) { busy=false; notifyAll(); } }
    }
    private void validateRecovered(Handle h) {
        byte[] request=h.fields.get("requestId");
        if(request==null || request.length!=16 || !ScopedTransactionRpc.uuid(ByteBuffer.wrap(request)).equals(h.id)) throw new IllegalArgumentException("Schema request ID differs from journal file");
        if(!Arrays.equals(h.fields.get("catalogueBinding"),catalogueBinding()) || !h.text("namespace").equals(cluster+"/"+database+"/"+domain)) throw new IllegalArgumentException("Schema journal namespace changed");
        var source=sourceSchema(h); var directory=sourceDirectory(h); source.requireMatches(directory);
        if(!source.clusterId().equals(cluster) || !source.databaseId().equals(database) || !source.domainId().equals(domain) || h.number("sequence")<0) throw new IllegalArgumentException("Schema source namespace/sequence changed");
        h.phase();
        if(h.fields.containsKey("upload")) {
            var upload=OperatorUpload.decode(h.fields.get("upload"));
            if(!upload.clusterId().equals(cluster) || !upload.databaseId().equals(database)) throw new IllegalArgumentException("Operator upload namespace changed");
            if(h.fields.containsKey("artifact") && !Arrays.equals(upload.encode(),OperatorUpload.create(upload.uploadId(),cluster,database,upload.operator(),h.fields.get("artifact")).encode())) throw new IllegalArgumentException("Operator upload artifact differs from frozen manifest");
        }
        if(h.fields.containsKey("publishedUpload")) {
            var original=OperatorUpload.decode(h.fields.get("upload")); var published=OperatorUpload.decode(h.fields.get("publishedUpload"));
            if(!h.text("kind").equals("UPLOAD") || !published.clusterId().equals(cluster) || !published.databaseId().equals(database)
                    || !published.operator().name().equals(original.operator().name()) || !Arrays.equals(published.operator().artifactSha256(),original.operator().artifactSha256())) throw new IllegalArgumentException("Published upload result changed original name/SHA");
        }
        if(h.fields.containsKey("commonUpload")) {
            if(!h.text("kind").equals("UPLOAD") || !Arrays.equals(h.fields.get("commonUpload"),new byte[]{1})) throw new IllegalArgumentException("Invalid common upload request");
            if(h.fields.containsKey("operatorNameWitness")) RatisOperatorCatalogue.NameHead.decode(h.fields.get("operatorNameWitness"));
        }
        if(h.fields.containsKey("definition")) {
            var definition=definition(h);
            if(!definition.changeId().equals(h.id) || !definition.beginOperation().sessionId().equals(h.id) || definition.beginOperation().epoch()!=0
                    || !Arrays.equals(definition.sourceDirectory().payloadDigest(),directory.payloadDigest()) || !Arrays.equals(DistributedSchemaCodec.encode(definition.sourceSchema()),DistributedSchemaCodec.encode(source))) throw new IllegalArgumentException("Schema definition request/source identity changed");
            if(h.fields.containsKey("targetSchema") && !Arrays.equals(h.fields.get("targetSchema"),DistributedSchemaCodec.encode(definition.targetSchema()))) throw new IllegalArgumentException("Schema target content changed");
        }
        for(var entry:h.fields.entrySet()) if(entry.getKey().equals("pendingCommand") || entry.getKey().equals("pendingAck") || entry.getKey().startsWith("command/")) {
            var command=DistributedCommandCodec.decode(entry.getValue()); var operation=command.operationId();
            if(!operation.sessionId().equals(h.id) || operation.epoch()!=0 || operation.sequence()>h.number("sequence") || !command.groupId().equals(binding.groupId()) || command.rangeEpoch()!=binding.epoch() || !Arrays.equals(command.schemaDigest(),binding.schemaDigest())) throw new IllegalArgumentException("Schema command session/catalogue identity changed");
            if(entry.getKey().startsWith("command/") && !entry.getKey().equals("command/"+operation.sequence())) throw new IllegalArgumentException("Schema command history sequence changed");
            if(entry.getKey().equals("pendingAck") && (command.kind()!=DistributedCommand.Kind.DATA || !command.mutations().isEmpty() || !command.predicates().isEmpty() || command.acknowledgedThrough()<0 || !h.fields.containsKey("result/"+command.acknowledgedThrough()))) throw new IllegalArgumentException("Schema ACK has no durably consumed original result");
        }
        if(h.fields.containsKey("pendingResult") && !h.fields.containsKey("pendingCommand")) throw new IllegalArgumentException("Schema result lacks original command");
        for(var entry:h.fields.entrySet()) if(entry.getKey().startsWith("result/")) {
            if(!h.fields.containsKey("command/"+entry.getKey().substring(7))) throw new IllegalArgumentException("Schema result history lacks original command"); CommandResult.decode(entry.getValue());
        }
        if(h.fields.containsKey("reservation")) RatisColumnAllocation.Reservation.decode(h.id,h.fields.get("reservation")).requireOwner(domain,column(h),h.number("api"));
        if(h.fields.containsKey("pendingReplica") && !RatisSchemaReplicaRpc.request(h.fields.get("pendingReplica")).target().binding().definition().changeId().equals(h.id)) throw new IllegalArgumentException("Schema replica input belongs to another request");
    }
    private void acknowledge(Handle h) throws IOException {
        byte[] bytes=h.fields.get("pendingAck"); var reply=client.io().send(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes)));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>CommandResult.MAX_BYTES) throw new IOException("Schema receipt acknowledgement failed");
        final CommandResult result; try { result=CommandResult.decode(reply.getMessage().getContent().toByteArray()); } catch(IllegalArgumentException malformed) { throw new IOException("Invalid schema acknowledgement reply",malformed); }
        if(result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Schema acknowledgement lacks success");
        h.fields.remove("pendingAck"); save(h);
    }
    private void finishAbort(Handle h) throws IOException {
        var result=command(h,() -> { var before=head(h); return administration.prepareAdvance(next(h),before,before.status().abort()); },administration::execute);
        h.fields.remove("abortRequested");
        if(result.status()==CommandResult.Status.SUCCESS) { h.number("aborted",1); phase(h,Phase.CLEANUP); } else phase(h,Phase.CONFLICT);
    }
    private byte[] catalogueBinding() {
        var out=ByteBuffer.allocate(88); ScopedTransactionRpc.uuid(out,binding.groupId()); out.putLong(binding.epoch()).put(binding.schemaDigest()); ScopedTransactionRpc.uuid(out,binding.directoryColumn()); ScopedTransactionRpc.uuid(out,binding.sessionsColumn()); return out.array();
    }
    private Path path(UUID request) { return root.resolve(request+".request"); }
    private void save(Handle h) throws IOException { try { checkpoint.write(path(h.id),h.fields); } catch(IOException failure) { fenced=true; throw failure; } }
    private void requireHandle(Handle h) { if (h==null || h.released || handles.get(h.id)!=h) throw new IllegalArgumentException("Foreign/released schema request handle"); }
    private void requireIdle() throws IOException { if(closed || fenced || !lock.isValid()) throw new IOException("Schema client closed/fenced; reopen original request"); if(busy) throw new IOException("Schema client I/O active"); }
    @Override public synchronized void close() throws IOException {
        if(closed) return; if(busy) throw new IOException("Schema I/O still active"); closed=true; for(var h:handles.values()) h.released=true; handles.clear();
        try { lock.release(); } finally { lockChannel.close(); }
    }
}
