package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Durable caller-driven split/merge. Borrowed transports own remote work; local close retains all journals. */
public final class RatisTopologyController implements AutoCloseable {
    public static final int MAX_JOBS=8,MAX_STEPS=128;
    public static final long MAX_METADATA_BYTES=64L*1024*1024;
    public enum Phase { DEFINITION, BEGIN, FENCING, FREEZE, FREEZE_PROOFS, SEED, SEED_PROOFS, PUBLISH, ACTIVATE, ACTIVATE_PROOFS, ACTIVE, RETIRED, CONFLICT }
    /** Retirement completes catalogue administration; pending owners and physical parent retention remain independent. */
    public record Progress(UUID intentId,Phase phase,TopologyChange.State state,int pendingOwners,boolean retirementBlocked) {}
    public interface DataPlane {
        NativeTopologyFreeze freeze(RatisTopologyFreezeRecord record) throws IOException;
        NativeTopologySeed seed(RatisTopologySeedRpc.Request request) throws IOException;
        RatisTopologyOwnerRpc.Evidence proof(TopologyChange.Definition definition,TopologyChange.Stage stage,RangeDescriptor range,ReplicaDescriptor owner) throws IOException;
        RatisTopologyAuthority.View authority(TopologyChange.Header header,TopologyChange.Stage stage) throws IOException;
        NativeTopologyActivation activate(RatisTopologyActivationRecord record) throws IOException;
    }
    public static final class Conflict extends IOException { private Conflict() { super("Original topology command conflicted; its frozen input is not rebased"); } }
    private final Path root;
    private final UUID cluster,database;
    private final RatisNodePlan.Catalogue binding;
    private final RaftClient client;
    private final DataPlane data;
    private final RatisTopologyCatalogue catalogue;
    private final RatisSchemaAdministration schemas;
    private final RatisRangeImportCatalogue imports;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Map<UUID,Handle> handles=new HashMap<>();
    private boolean busy,closed,fenced;
    public final class Handle implements AutoCloseable {
        private final UUID id;
        private final Map<String,byte[]> fields;
        private final TopologyChange.Definition definition;
        private boolean released;
        private Handle(UUID id,Map<String,byte[]> fields) { this.id=id; this.fields=new ConcurrentHashMap<>(fields); definition=TopologyChange.Definition.decode(fields.get("definition")); }
        public UUID intentId() { return id; }
        public TopologyChange.Definition definition() { return definition; }
        public TopologyChange.State state() { return TopologyChange.State.decode(fields.get("state")); }
        public Phase phase() { return Phase.valueOf(text(this,"phase")); }
        @Override public void close() throws IOException { synchronized(RatisTopologyController.this) { if(busy) throw new IOException("Topology I/O still active"); released=true; handles.remove(id,this); } }
    }
    public RatisTopologyController(Path root,UUID cluster,UUID database,RatisNodePlan.Catalogue binding,RaftClient client,DataPlane data) throws IOException {
        this.root=root.toAbsolutePath().normalize(); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.binding=Objects.requireNonNull(binding); this.client=Objects.requireNonNull(client); this.data=Objects.requireNonNull(data);
        if(!Files.isDirectory(this.root,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Topology journal root absent");
        catalogue=new RatisTopologyCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        schemas=new RatisSchemaAdministration(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()); imports=new RatisRangeImportCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        lockChannel=FileChannel.open(this.root.resolve("topology-controller.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
        try { lock=lockChannel.tryLock(); if(lock==null) throw new IOException("Topology journal root already owned"); } catch(IOException|RuntimeException failure) { lockChannel.close(); throw new IOException("Topology journal ownership unavailable",failure); }
    }
    public Handle prepare(TopologyChange.Definition definition) throws IOException {
        begin(); try {
            requireDefinition(definition); UUID id=definition.intentId(); if(Files.exists(path(id),LinkOption.NOFOLLOW_LINKS) || handles.containsKey(id)) throw new IOException("Topology intent retained; reopen original journal");
            int count=0; try(var files=Files.newDirectoryStream(root,"*.topology")) { for(var ignored:files) if(++count>=MAX_JOBS) throw new IOException("Topology job capacity exhausted"); }
            var fields=new HashMap<String,byte[]>(); fields.put("definition",definition.encode()); fields.put("binding",bindingBytes()); fields.put("state",TopologyChange.State.begin(definition.header()).encode());
            fields.put("issuer",UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)); fields.put("sequence",number(0)); fields.put("cursor",number(0)); fields.put("seedCursor",number(0)); fields.put("seedProofCursor",number(0)); fields.put("actProofCursor",number(0)); fields.put("phase",Phase.DEFINITION.name().getBytes(StandardCharsets.UTF_8));
            var h=new Handle(id,fields); save(h); handles.put(id,h); return h;
        } finally { end(); }
    }
    public Handle reopen(UUID intentId) throws IOException {
        begin(); try {
            if(handles.containsKey(intentId)) return handles.get(intentId); if(handles.size()>=MAX_JOBS) throw new IOException("Topology handle capacity exhausted");
            final Handle h; try { h=new Handle(intentId,RatisSchemaClientJournal.read(path(intentId))); validateRecovered(h); } catch(RuntimeException invalid) { throw new IOException("Invalid retained topology journal",invalid); }
            handles.put(intentId,h); return h;
        } finally { end(); }
    }
    public Progress advance(Handle h,int maxSteps) throws IOException {
        begin(); try {
            requireHandle(h); if(maxSteps<1 || maxSteps>MAX_STEPS) throw new IllegalArgumentException("Invalid topology step bound");
            for(int i=0;i<maxSteps;i++) {
                if(h.fields.containsKey("pendingAck")) { acknowledge(h); continue; }
                if(h.phase()==Phase.CONFLICT) throw new Conflict(); if(h.phase()==Phase.RETIRED) break;
                if(h.fields.containsKey("pendingCommand")) { execute(h); continue; } step(h);
            }
            return new Progress(h.id,h.phase(),h.state(),pending(h),h.state().phase()==TopologyChange.Phase.ACTIVE);
        } finally { end(); }
    }
    private void step(Handle h) throws IOException {
        var d=h.definition; var state=h.state(); int cursor=Math.toIntExact(value(h,"cursor"));
        switch(h.phase()) {
            case DEFINITION -> { before(h,"capacity"); freeze(h,catalogue.prepareDefinition(next(h),d,catalogue.readCapacity()),Phase.BEGIN); execute(h); }
            case BEGIN -> {
                before(h,"previousTopologyHead"); var previous=catalogue.readHead(d.header().domainId()); before(h,"schemaHead"); var schema=schemas.readHead(d.header().domainId()); before(h,"importHead"); var imported=imports.readHead(d.header().domainId());
                if(schema!=null) h.fields.put("schemaHead",schema.encode()); if(imported!=null) h.fields.put("importHead",imported.encode());
                freeze(h,catalogue.prepareBegin(next(h),d,previous,schema,imported),Phase.FENCING); execute(h);
            }
            case FENCING -> { freeze(h,catalogue.prepareFencing(next(h),d,state,schema(h),imported(h)),Phase.FREEZE); execute(h); }
            case FREEZE -> {
                if(cursor==d.parents().size()) { move(h,Phase.FREEZE_PROOFS); return; } var parent=d.parents().get(cursor);
                if(!h.fields.containsKey("freezeRecord")) {
                    if(!h.fields.containsKey("freezeAdmission")) { var view=authority(h,TopologyChange.Stage.FREEZE); h.fields.put("freezeAdmission",view.admissionDigest()); }
                    h.fields.put("freezeRecord",new RatisTopologyFreezeRecord(d,parent.groupId(),h.fields.get("freezeAdmission")).encode()); save(h);
                }
                var record=RatisTopologyFreezeRecord.decode(h.fields.get("freezeRecord")); save(h); var result=data.freeze(record); record.requireFreeze(result); h.fields.put("freeze/"+parent.groupId(),result.encode()); h.fields.remove("freezeRecord"); value(h,"cursor",cursor+1); save(h);
            }
            case FREEZE_PROOFS -> {
                if(state.phase()==TopologyChange.Phase.FENCED) { move(h,Phase.SEED); return; }
                collect(h,TopologyChange.Stage.FREEZE,"cursor",Phase.FREEZE_PROOFS);
            }
            case SEED -> {
                if(state.phase()==TopologyChange.Phase.SEEDED) { seedNode(h); move(h,Phase.PUBLISH); return; } seedNode(h); move(h,Phase.SEED_PROOFS);
            }
            case SEED_PROOFS -> {
                if(state.phase()==TopologyChange.Phase.SEEDED) { move(h,Phase.PUBLISH); return; }
                collect(h,TopologyChange.Stage.SEED,"seedProofCursor",Phase.SEED);
            }
            case PUBLISH -> { freeze(h,catalogue.preparePublication(next(h),d,state,schema(h),imported(h)),Phase.ACTIVATE); execute(h); }
            case ACTIVATE -> {
                if(cursor==d.targets().size()) { move(h,Phase.ACTIVATE_PROOFS); return; } activate(h,d.targets().get(cursor)); value(h,"cursor",cursor+1); save(h);
            }
            case ACTIVATE_PROOFS -> {
                if(state.phase()==TopologyChange.Phase.ACTIVE) { move(h,Phase.ACTIVE); return; }
                if(cursor%3==0) { seedNode(h); value(h,"cursor",cursor+1); save(h); }
                else if(cursor%3==1) { value(h,"cursor",cursor+1); collect(h,TopologyChange.Stage.SEED,"seedProofCursor",Phase.ACTIVATE_PROOFS); }
                else { value(h,"cursor",cursor+1); collect(h,TopologyChange.Stage.ACTIVATE,"actProofCursor",Phase.ACTIVATE_PROOFS); }
            }
            case ACTIVE -> {
                if(state.phase()==TopologyChange.Phase.ACTIVE) { freeze(h,catalogue.prepareRetirement(next(h),d,state,schema(h),imported(h)),Phase.RETIRED); execute(h); }
                else if(cursor%3==0) { seedNode(h); value(h,"cursor",cursor+1); save(h); }
                else if(cursor%3==1) { value(h,"cursor",cursor+1); collect(h,TopologyChange.Stage.SEED,"seedProofCursor",Phase.ACTIVE); }
                else { value(h,"cursor",cursor+1); collect(h,TopologyChange.Stage.ACTIVATE,"actProofCursor",Phase.ACTIVE); }
            }
            default -> throw new IllegalStateException("Unexpected topology phase");
        }
    }
    private RatisTopologyAuthority.View authority(Handle h,TopologyChange.Stage stage) throws IOException {
        before(h,"authority/"+stage); var view=data.authority(h.definition.header(),stage); view.require(RatisTopologyAuthority.Request.of(h.definition.header(),stage));
        if(!view.catalogue().equals(binding.groupId()) || !Arrays.equals(view.definition().encode(),h.definition.encode())) throw new IOException("Topology authority crossed original catalogue/definition"); return view;
    }
    private List<ReplicaDescriptor> nodes(Handle h) { return h.definition.parents().getFirst().replicas().stream().sorted(Comparator.comparing(ReplicaDescriptor::role).thenComparing(ReplicaDescriptor::replicaId,RangeDirectoryCodec.UUID_ORDER)).toList(); }
    private void seedNode(Handle h) throws IOException {
        var nodes=nodes(h); int cursor=Math.toIntExact(value(h,"seedCursor")%nodes.size()); var owner=nodes.get(cursor); value(h,"seedCursor",Math.incrementExact(value(h,"seedCursor")));
        if(h.fields.containsKey("seedComplete/"+owner.replicaId())) { save(h); return; }
        String key="seedRequest/"+owner.replicaId();
        if(!h.fields.containsKey(key)) { var bytes=ByteBuffer.allocate(48); ScopedTransactionRpc.uuid(bytes,binding.groupId()); ScopedTransactionRpc.uuid(bytes,h.definition.parents().getFirst().groupId()); ScopedTransactionRpc.uuid(bytes,owner.replicaId()); h.fields.put(key,bytes.array()); h.fields.put("seedRequestHash/"+owner.replicaId(),RangeImportAdmission.sha256(RatisTopologySeedRpc.request(seedRequest(h,key)))); save(h); }
        var request=seedRequest(h,key); save(h);
        final NativeTopologySeed seed;
        try { seed=data.seed(request); requireSeed(h,seed,owner); } catch(IOException unavailable) { save(h); return; }
        h.fields.put("seededNode/"+owner.replicaId(),seed.digest()); h.fields.put("seedComplete/"+owner.replicaId(),new byte[]{1}); save(h); // Other actual nodes may establish the required voter quorums.
    }
    private RatisTopologySeedRpc.Request seedRequest(Handle h,String key) {
        byte[] bytes=h.fields.get(key); if(bytes.length!=48) throw new IllegalArgumentException("Invalid frozen seed identity"); var in=ByteBuffer.wrap(bytes); return new RatisTopologySeedRpc.Request(h.definition,ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in));
    }
    private void requireSeed(Handle h,NativeTopologySeed seed,ReplicaDescriptor owner) throws IOException {
        if(!Arrays.equals(seed.definition().encode(),h.definition.encode()) || !owner.equals(seed.localReplica()) || !Arrays.equals(seed.authority().freezeDigest(),h.state().freezeDigest())) throw new IOException("Seed changed original definition/owner/parent cuts");
        for(var parent:h.definition.parents()) if(!Arrays.equals(seed.parentFreezes().get(parent.groupId()).encode(),h.fields.get("freeze/"+parent.groupId()))) throw new IOException("Seed changed original native parent freeze");
        byte[] retained=h.fields.get("seededNode/"+owner.replicaId()); if(retained!=null && !Arrays.equals(retained,seed.digest())) throw new IOException("Owner changed its original retained seed artifact");
    }
    private void activate(Handle h,RangeDescriptor child) throws IOException {
        String key="activationRecord/"+child.groupId(); if(!h.fields.containsKey(key)) { var view=authority(h,TopologyChange.Stage.ACTIVATE); if(!Arrays.equals(view.state().freezeDigest(),h.state().freezeDigest())) throw new IOException("Publication changed original frozen parent cuts"); h.fields.put(key,RatisTopologyActivationRecord.of(view,child.groupId()).encode()); save(h); }
        var record=RatisTopologyActivationRecord.decode(h.fields.get(key)); save(h); var result=data.activate(record); result.requireRequest(h.definition,record.freezeDigest(),binding.groupId(),record.catalogueCut(),record.publicationCommitment());
        if(!result.birth().groupId().equals(child.groupId())) throw new IOException("Activation returned another child group"); h.fields.put("activation/"+child.groupId(),result.encode()); save(h);
    }
    private record Target(RangeDescriptor range,ReplicaDescriptor owner) {}
    private List<Target> targets(Handle h,TopologyChange.Stage stage) {
        var targets=new ArrayList<Target>(); for(var range:stage==TopologyChange.Stage.FREEZE ? h.definition.parents() : h.definition.targets()) for(var owner:range.replicas()) targets.add(new Target(range,owner));
        targets.sort(Comparator.comparing((Target t)->t.owner().role()).thenComparing(t->t.range().groupId(),RangeDirectoryCodec.UUID_ORDER).thenComparing(t->t.owner().replicaId(),RangeDirectoryCodec.UUID_ORDER)); return targets;
    }
    private Map<TopologyChange.Key,TopologyChange.Proof> proofs(TopologyChange.State state,TopologyChange.Stage stage) { return switch(stage) { case FREEZE -> state.frozen(); case SEED -> state.seeded(); case ACTIVATE -> state.activated(); }; }
    private void collect(Handle h,TopologyChange.Stage stage,String cursorKey,Phase nextPhase) throws IOException {
        var targets=targets(h,stage); long cursor=value(h,cursorKey); var target=targets.get(Math.toIntExact(cursor%targets.size())); value(h,cursorKey,Math.incrementExact(cursor));
        var key=new TopologyChange.Key(target.range().groupId(),target.owner().replicaId()); var state=h.state();
        if(proofs(state,stage).containsKey(key) || stage==TopologyChange.Stage.ACTIVATE && !state.seeded().containsKey(key)) { text(h,"phase",nextPhase.name()); save(h); return; }
        before(h,"proof/"+stage+"/"+target.range().groupId()+"/"+target.owner().replicaId()); final RatisTopologyOwnerRpc.Evidence evidence;
        try { evidence=data.proof(h.definition,stage,target.range(),target.owner()); } catch(IOException unavailable) { text(h,"phase",nextPhase.name()); save(h); return; }
        var expected=new RatisTopologyOwnerRpc.Target(h.definition,stage,binding.groupId(),target.range().groupId(),target.owner().replicaId());
        if(!Arrays.equals(RatisTopologyOwnerRpc.request(expected),RatisTopologyOwnerRpc.request(evidence.target())) || !target.owner().equals(evidence.owner()) || evidence.proof().stage()!=stage || !evidence.proof().groupId().equals(target.range().groupId()) || !evidence.proof().owner().equals(TopologyChange.Owner.of(target.owner()))) throw new IOException("Topology owner evidence crossed exact requested owner/stage");
        var cut=evidence.completedCut(); var position=evidence.proof().position(); if(cut.term()<1 || cut.index()<position.index() || cut.term()<position.term() || position.term()>0 && cut.index()==position.index() && cut.term()!=position.term()) throw new IOException("Owner lacks a genuine completed Raft prefix covering original stage");
        if(stage==TopologyChange.Stage.FREEZE) { var actual=NativeTopologyFreeze.decode(evidence.artifact()); if(!Arrays.equals(actual.encode(),h.fields.get("freeze/"+target.range().groupId()))) throw new IOException("Owner changed original group FREEZE artifact"); }
        if(stage==TopologyChange.Stage.SEED) { var actual=NativeTopologySeed.decode(evidence.artifact()); requireSeed(h,actual,target.owner()); h.fields.put("seededNode/"+target.owner().replicaId(),actual.digest()); }
        if(stage==TopologyChange.Stage.ACTIVATE) { var record=RatisTopologyActivationRecord.decode(h.fields.get("activationRecord/"+target.range().groupId())); NativeTopologyActivation.decode(evidence.artifact()).requireRequest(h.definition,record.freezeDigest(),binding.groupId(),record.catalogueCut(),record.publicationCommitment()); }
        before(h,"prepareProof"); freeze(h,catalogue.prepareProof(next(h),h.definition,state,evidence.proof(),schema(h),imported(h)),nextPhase); execute(h);
    }
    private void freeze(Handle h,DistributedCommand command,Phase next) throws IOException { h.fields.put("pendingCommand",DistributedCommandCodec.encode(command)); text(h,"commandPhase",h.phase().name()); text(h,"afterCommand",next.name()); save(h); }
    private void execute(Handle h) throws IOException {
        var command=DistributedCommandCodec.decode(h.fields.get("pendingCommand"));
        if(!h.fields.containsKey("pendingResult")) {
            if(RatisTopologyCatalogue.requiresOwnerAdmission(command)) {
                save(h); var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(message(RatisTopologyExecutionRpc.prepareRequest(command))));
                try { RatisTopologyExecutionRpc.requireEcho(content(reply,RatisTopologyExecutionRpc.MAX_BYTES),RatisTopologyExecutionRpc.PREPARE,command); } catch(IllegalArgumentException invalid) { throw new IOException("Topology execution preparation changed original command",invalid); }
            }
            save(h); var result=catalogue.execute(command); if(result.valueCount()!=0) throw new IOException("Topology command returned values"); h.fields.put("pendingResult",result.encode()); save(h);
        }
        var result=CommandResult.decode(h.fields.get("pendingResult"));
        if(result.status()==CommandResult.Status.SUCCESS) { if(Phase.valueOf(text(h,"commandPhase"))!=Phase.DEFINITION) h.fields.put("state",command.mutations().getFirst().value()); var after=Phase.valueOf(text(h,"afterCommand")); if(h.phase()!=after && (after==Phase.FREEZE || after==Phase.FREEZE_PROOFS || after==Phase.ACTIVATE || after==Phase.ACTIVATE_PROOFS || after==Phase.ACTIVE)) value(h,"cursor",0); text(h,"phase",after.name()); }
        else { text(h,"phase",Phase.CONFLICT.name()); h.fields.put("conflictResult",h.fields.get("pendingResult")); }
        long consumed=command.operationId().sequence(); h.fields.put("consumed",number(consumed)); h.fields.put("pendingAck",DistributedCommandCodec.encode(new DistributedCommand(binding.groupId(),binding.epoch(),binding.schemaDigest(),next(h),List.of(),List.of(),consumed)));
        for(var key:List.of("pendingCommand","pendingResult","commandPhase","afterCommand")) h.fields.remove(key); save(h);
    }
    private void acknowledge(Handle h) throws IOException { save(h); var reply=RatisRoutedClient.rpc(()->client.io().send(message(h.fields.get("pendingAck")))); final CommandResult result; try { result=CommandResult.decode(content(reply,CommandResult.MAX_BYTES)); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology receipt acknowledgement",invalid); } if(result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Topology receipt acknowledgement rejected"); h.fields.remove("pendingAck"); h.fields.remove("consumed"); save(h); }
    private int pending(Handle h) { var state=h.state(); int count=0; for(var node:nodes(h)) if(!h.fields.containsKey("seedComplete/"+node.replicaId())) count++; for(var target:targets(h,TopologyChange.Stage.SEED)) { var key=new TopologyChange.Key(target.range().groupId(),target.owner().replicaId()); if(!state.seeded().containsKey(key)) count++; if(!state.activated().containsKey(key)) count++; } return count; }
    private void validateRecovered(Handle h) {
        requireDefinition(h.definition); if(!h.id.equals(h.definition.intentId()) || !Arrays.equals(bindingBytes(),h.fields.get("binding"))) throw new IllegalArgumentException("Topology journal namespace/binding changed"); h.state().requireDefinition(h.definition); h.phase(); UUID issuer=UUID.fromString(text(h,"issuer"));
        for(var key:List.of("sequence","cursor","seedCursor","seedProofCursor","actProofCursor")) if(value(h,key)<0) throw new IllegalArgumentException("Negative topology journal cursor/sequence");
        for(var key:List.of("pendingCommand","pendingAck")) if(h.fields.containsKey(key)) { var command=DistributedCommandCodec.decode(h.fields.get(key)); var op=command.operationId(); if(!op.sessionId().equals(issuer) || op.epoch()!=0 || op.sequence()!=value(h,"sequence")-1 || !command.groupId().equals(binding.groupId()) || command.rangeEpoch()!=binding.epoch() || !Arrays.equals(command.schemaDigest(),binding.schemaDigest())) throw new IllegalArgumentException("Topology frozen command binding changed"); if(key.equals("pendingAck") && (!command.mutations().isEmpty() || !command.predicates().isEmpty() || command.acknowledgedThrough()!=value(h,"consumed"))) throw new IllegalArgumentException("Topology ACK lacks durable resolved original"); }
        if(h.fields.containsKey("pendingCommand")) {
            if(h.fields.containsKey("pendingAck") || !text(h,"commandPhase").equals(h.phase().name())) throw new IllegalArgumentException("Topology frozen command phase changed");
            Phase.valueOf(text(h,"afterCommand")); var command=DistributedCommandCodec.decode(h.fields.get("pendingCommand"));
            if(h.phase()==Phase.DEFINITION) { if(!Arrays.equals(command.mutations().getFirst().value(),h.definition.encode())) throw new IllegalArgumentException("Topology frozen definition changed"); }
            else { var next=TopologyChange.State.decode(command.mutations().getFirst().value()); next.requireDefinition(h.definition); if(h.phase()==Phase.BEGIN) { if(!next.equals(TopologyChange.State.begin(h.definition.header()))) throw new IllegalArgumentException("Topology frozen BEGIN changed"); } else { next.requireSuccessorOf(h.state()); if(!Arrays.equals(command.predicates().getFirst().expectedValue(),h.fields.get("state"))) throw new IllegalArgumentException("Topology frozen predecessor changed"); } }
        }
        if(h.fields.containsKey("pendingResult") && !h.fields.containsKey("pendingCommand")) throw new IllegalArgumentException("Topology result lost original command");
        if(h.fields.containsKey("freezeRecord")) { var record=RatisTopologyFreezeRecord.decode(h.fields.get("freezeRecord")); if(!Arrays.equals(record.definition().encode(),h.definition.encode()) || !Arrays.equals(record.admissionDigest(),h.fields.get("freezeAdmission")) || h.phase()!=Phase.FREEZE || !record.groupId().equals(h.definition.parents().get(Math.toIntExact(value(h,"cursor"))).groupId())) throw new IllegalArgumentException("Topology original freeze changed"); }
        for(var row:h.fields.entrySet()) {
            if(row.getKey().startsWith("seedRequest/")) { var request=seedRequest(h,row.getKey()); if(!request.catalogue().equals(binding.groupId()) || !request.anchorGroup().equals(h.definition.parents().getFirst().groupId()) || !row.getKey().equals("seedRequest/"+request.replica())) throw new IllegalArgumentException("Topology frozen seed binding changed"); if(!Arrays.equals(RangeImportAdmission.sha256(RatisTopologySeedRpc.request(request)),h.fields.get("seedRequestHash/"+row.getKey().substring(12)))) throw new IllegalArgumentException("Topology original seed request changed"); }
            if(row.getKey().startsWith("activationRecord/")) { var record=RatisTopologyActivationRecord.decode(row.getValue()); if(!Arrays.equals(record.definition().encode(),h.definition.encode()) || !record.catalogueGroup().equals(binding.groupId()) || !row.getKey().equals("activationRecord/"+record.groupId()) || !Arrays.equals(record.freezeDigest(),h.state().freezeDigest())) throw new IllegalArgumentException("Topology original activation changed"); }
        }
    }
    private void requireDefinition(TopologyChange.Definition d) { if(d.kind()==TopologyChange.Kind.RELOCATE || !d.header().clusterId().equals(cluster) || !d.header().databaseId().equals(database)) throw new IllegalArgumentException("Topology controller requires owned split/merge namespace"); }
    private OperationId next(Handle h) { long sequence=value(h,"sequence"); value(h,"sequence",Math.incrementExact(sequence)); return new OperationId(UUID.fromString(text(h,"issuer")),0,sequence); }
    private RatisSchemaAdministration.Head schema(Handle h) { return h.fields.containsKey("schemaHead") ? RatisSchemaAdministration.Head.decode(h.fields.get("schemaHead")) : null; }
    private RatisRangeImportCatalogue.Head imported(Handle h) { return h.fields.containsKey("importHead") ? RatisRangeImportCatalogue.Head.decode(h.fields.get("importHead")) : null; }
    private void before(Handle h,String action) throws IOException { text(h,"read",action); save(h); }
    private void move(Handle h,Phase phase) throws IOException { text(h,"phase",phase.name()); value(h,"cursor",0); save(h); }
    private static byte[] number(long number) { return ByteBuffer.allocate(8).putLong(number).array(); }
    private static long value(Handle h,String key) { return ByteBuffer.wrap(h.fields.get(key)).getLong(); }
    private static void value(Handle h,String key,long value) { h.fields.put(key,number(value)); }
    private static String text(Handle h,String key) { return new String(h.fields.get(key),StandardCharsets.UTF_8); }
    private static void text(Handle h,String key,String value) { h.fields.put(key,value.getBytes(StandardCharsets.UTF_8)); }
    private byte[] bindingBytes() { var out=ByteBuffer.allocate(88); ScopedTransactionRpc.uuid(out,binding.groupId()); out.putLong(binding.epoch()).put(binding.schemaDigest()); ScopedTransactionRpc.uuid(out,binding.directoryColumn()); ScopedTransactionRpc.uuid(out,binding.sessionsColumn()); return out.array(); }
    private Path path(UUID id) { return root.resolve(id+".topology"); }
    private void save(Handle h) throws IOException {
        try {
            long bytes=40; for(var row:h.fields.entrySet()) { String key=row.getKey(); if(key.length()>128 || !key.chars().allMatch(c->c>0 && c<128)) throw new IOException("Invalid topology journal field"); bytes=Math.addExact(bytes,6L+key.length()+row.getValue().length); }
            if(bytes>RatisSchemaClientJournal.MAX_BYTES) throw new IOException("Topology job metadata exceeds journal bound");
            long occupied=0; Path pending=path(h.id).resolveSibling(path(h.id).getFileName()+".pending");
            try(var files=Files.newDirectoryStream(root)) { for(var file:files) { String name=file.getFileName().toString(); if((name.endsWith(".topology") || name.endsWith(".topology.pending")) && !file.equals(pending)) { if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid owned topology journal path"); occupied=Math.addExact(occupied,Files.size(file)); } } }
            if(bytes>MAX_METADATA_BYTES-occupied) throw new IOException("Topology aggregate/atomic journal metadata capacity exhausted"); RatisSchemaClientJournal.write(path(h.id),h.fields);
        } catch(IOException failure) { fenced=true; throw failure; }
    }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(org.apache.ratis.protocol.RaftClientReply reply,int maximum) throws IOException { if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>maximum) throw new IOException("Topology transport response failed/exceeded bound",reply==null ? null : reply.getException()); return reply.getMessage().getContent().toByteArray(); }
    private synchronized void begin() throws IOException { if(closed || fenced || !lock.isValid()) throw new IOException("Topology controller closed/fenced; reopen original journal"); if(busy) throw new IOException("Topology I/O still active"); busy=true; }
    private synchronized void end() { busy=false; notifyAll(); }
    private void requireHandle(Handle h) { if(h==null || h.released || handles.get(h.id)!=h) throw new IllegalArgumentException("Foreign topology handle"); }
    @Override public synchronized void close() throws IOException { if(closed) return; if(busy) throw new IOException("Topology I/O still active"); closed=true; handles.values().forEach(h->h.released=true); handles.clear(); try { lock.release(); } finally { lockChannel.close(); } }
}
