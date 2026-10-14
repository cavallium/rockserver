package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Durable topology intent. PREPARED confers no native fencing, seeding or publication authority. */
public final class RatisTopologyCatalogue {
    public static final int MAX_INTENTS=128;
    public static final long MAX_BYTES=64L*1024*1024;
    private static final byte STATE=(byte)0x82,HEAD=(byte)0x83,DEFINITION=(byte)0x84,IDENTITY=(byte)0x85,CAPACITY=(byte)0x86;
    @FunctionalInterface public interface ImmutableReader { byte[] read(byte[] key); }
    /** Trusted transport validates the actual addressed owner, stable configuration and current Raft term. */
    @FunctionalInterface public interface OwnerProofReader { QualifiedOwner read(TopologyChange.Definition definition,TopologyChange.Proof expected) throws IOException; }
    @FunctionalInterface public interface RelocationOwnerReader { QualifiedRelocation read(TopologyChange.Definition definition,TopologyChange.RelocationProof expected)throws IOException; }
    public record QualifiedRelocation(TopologyChange.RelocationProof proof,ApplyPosition completedCut){public QualifiedRelocation{Objects.requireNonNull(proof);Objects.requireNonNull(completedCut);}}
    public record QualifiedOwner(TopologyChange.Proof proof,ApplyPosition completedCut) {
        public QualifiedOwner { Objects.requireNonNull(proof); Objects.requireNonNull(completedCut); }
    }
    public record Capacity(int count,long bytes) {
        public Capacity { if(count<0 || count>MAX_INTENTS || bytes<0 || bytes>MAX_BYTES || (count==0)!=(bytes==0)) throw new IllegalArgumentException("Invalid topology capacity"); }
        public byte[] encode() { return ByteBuffer.allocate(12).putInt(count).putLong(bytes).array(); }
        public static Capacity decode(byte[] bytes) { if(bytes==null) return new Capacity(0,0); if(bytes.length!=12) throw new IllegalArgumentException("Invalid topology capacity length"); var in=ByteBuffer.wrap(bytes); return new Capacity(in.getInt(),in.getLong()); }
        Capacity reserve(int definitionBytes) { return new Capacity(Math.incrementExact(count),Math.addExact(bytes,definitionBytes+33L+17+32+2L*(17+TopologyChange.MAX_STATUS_BYTES))); }
    }
    private final RaftClient client;
    private final UUID cluster,database,group,column;
    private final long epoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;
    public RatisTopologyCatalogue(RaftClient client,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column);
        if(!client.getGroupId().getUuid().equals(group) || epoch<0 || schema.length!=32) throw new IllegalArgumentException("Invalid topology catalogue binding");
        this.epoch=epoch; this.schema=schema.clone(); validator=validator(cluster,database,group,epoch,schema,column,this::uncheckedRead);
    }
    public Capacity readCapacity() throws IOException { byte[] bytes=read(capacityKey(),12); return bytes==null ? null : Capacity.decode(bytes); }
    public TopologyChange.Definition readDefinition(byte[] digest) throws IOException {
        byte[] bytes=read(definitionKey(digest),TopologyChange.MAX_DEFINITION_BYTES); if(bytes==null) return null;
        try { var definition=TopologyChange.Definition.decode(bytes); requireDefinition(definition,cluster,database,digest); return definition; } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology definition",invalid); }
    }
    public TopologyChange.State readIntent(UUID id) throws IOException { return readState(stateKey(id),id,null); }
    public TopologyChange.State readHead(UUID domain) throws IOException { return readState(headKey(domain),null,domain); }
    private TopologyChange.State readState(byte[] key,UUID id,UUID domain) throws IOException {
        byte[] bytes=read(key,TopologyChange.MAX_STATUS_BYTES); if(bytes==null) return null;
        try { var state=TopologyChange.State.decode(bytes); var header=state.header(); requireNamespace(header,cluster,database); if(id!=null && !id.equals(header.intentId()) || domain!=null && !domain.equals(header.domainId())) throw new IllegalArgumentException("Topology identity differs"); return state; } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology state",invalid); }
    }
    public DistributedCommand prepareDefinition(OperationId operation,TopologyChange.Definition definition,Capacity expected) {
        requireDefinition(definition,cluster,database,definition.digest()); byte[] bytes=definition.encode(); var next=(expected==null ? new Capacity(0,0) : expected).reserve(bytes.length);
        return checked(operation,List.of(put(definitionKey(definition.digest()),bytes),put(identityKey(definition.header().intentId()),definition.digest()),put(capacityKey(),next.encode())),
                List.of(predicate(definitionKey(definition.digest()),null),predicate(identityKey(definition.header().intentId()),null),predicate(capacityKey(),expected==null ? null : expected.encode())));
    }
    public DistributedCommand prepareBegin(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expectedHead,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead) {
        requireDefinition(definition,cluster,database,definition.digest()); if(expectedHead!=null && !expectedHead.terminal()) throw new IllegalArgumentException("Topology domain already owns an active intent");
        var state=TopologyChange.State.begin(definition.header()); byte[] bytes=state.encode(); UUID id=state.header().intentId(),domain=state.header().domainId();
        return checked(operation,List.of(put(stateKey(id),bytes),put(headKey(domain),bytes)),List.of(predicate(stateKey(id),null),predicate(headKey(domain),expectedHead==null ? null : expectedHead.encode()),
                predicate(domainKey(domain),RangeDirectoryCodec.encode(definition.sourceDirectory())),predicate(identityKey(id),definition.digest()),
                predicate(RatisSchemaAdministration.headKey(domain),schemaHead==null ? null : schemaHead.encode()),predicate(RatisRangeImportCatalogue.headKey(domain),importHead==null ? null : importHead.encode())));
    }
    public DistributedCommand prepareAbort(OperationId operation,TopologyChange.State expected) {
        var next=expected.abort(); byte[] bytes=next.encode();
        return checked(operation,List.of(put(stateKey(next.header().intentId()),bytes),put(headKey(next.header().domainId()),bytes)),
                List.of(predicate(stateKey(next.header().intentId()),expected.encode()),predicate(headKey(next.header().domainId()),expected.encode())));
    }
    /** Commits exclusive administration authority; parent FREEZE still needs fresh native admission. */
    public DistributedCommand prepareFencing(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead) {
        expected.requireDefinition(definition);
        if(expected.phase()!=TopologyChange.Phase.PREPARED) throw new IllegalArgumentException("Fencing requires PREPARED intent");
        var next=expected.beginFencing(); var header=next.header(); byte[] bytes=next.encode();
        return checked(operation,List.of(put(stateKey(header.intentId()),bytes),put(headKey(header.domainId()),bytes)),
                List.of(predicate(stateKey(header.intentId()),expected.encode()),predicate(headKey(header.domainId()),expected.encode()),
                        predicate(domainKey(header.domainId()),RangeDirectoryCodec.encode(definition.sourceDirectory())),predicate(identityKey(header.intentId()),definition.digest()),
                        predicate(RatisSchemaAdministration.headKey(header.domainId()),schemaHead==null ? null : schemaHead.encode()),predicate(RatisRangeImportCatalogue.headKey(header.domainId()),importHead==null ? null : importHead.encode())));
    }
    public DistributedCommand prepareJoining(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead){expected.requireDefinition(definition);return prepareProgress(operation,definition,expected,expected.beginJoining(),schemaHead,importHead,false);}
    public DistributedCommand prepareReconfigured(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,TopologyChange.RelocationProof proof,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead){expected.requireDefinition(definition);return prepareProgress(operation,definition,expected,expected.reconfigured(proof),schemaHead,importHead,false);}
    public DistributedCommand prepareRelocationPublication(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead){expected.requireDefinition(definition);return prepareProgress(operation,definition,expected,expected.publishRelocation(),schemaHead,importHead,true);}
    public static boolean relocationExecution(DistributedCommand command){return isCommand(command) && command.mutations().size()>=2 && TopologyChange.State.decode(command.mutations().getFirst().value()).header().kind()==TopologyChange.Kind.RELOCATE;}
    public static void admitRelocation(DistributedCommand command,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,ImmutableReader reader,RelocationOwnerReader owners)throws IOException{
        validator(cluster,database,group,epoch,schema,column,reader).accept(command);if(!requiresOwnerAdmission(command) || !relocationExecution(command) || owners==null)throw new IOException("Fresh relocation owner admission is not configured");requireCurrentWitnesses(command,reader);var next=TopologyChange.State.decode(command.mutations().getFirst().value());var definition=TopologyChange.Definition.decode(reader.read(definitionKey(next.header().definitionDigest())));var expected=next.relocation();var actual=owners.read(definition,expected);if(actual==null || !expected.equals(actual.proof()) || !covers(actual.completedCut(),expected.completedPosition()))throw new IOException("Relocation lost exact committed configuration/native readiness");requireCurrentWitnesses(command,reader);
    }
    private static boolean covers(ApplyPosition cut,ApplyPosition expected){return cut.term()>=expected.term() && cut.index()>=expected.index() && (cut.index()!=expected.index() || cut.term()==expected.term());}
    public DistributedCommand prepareProof(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,TopologyChange.Proof proof) {
        UUID domain=definition.header().domainId();
        byte[] schemaHead=uncheckedRead(RatisSchemaAdministration.headKey(domain)),importHead=uncheckedRead(RatisRangeImportCatalogue.headKey(domain));
        return prepareProof(operation,definition,expected,proof,schemaHead==null ? null : RatisSchemaAdministration.Head.decode(schemaHead),importHead==null ? null : RatisRangeImportCatalogue.Head.decode(importHead));
    }
    public DistributedCommand prepareProof(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,TopologyChange.Proof proof,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead) {
        expected.requireDefinition(definition); var next=switch(proof.stage()) { case FREEZE -> expected.frozen(proof); case SEED -> expected.seeded(proof); case ACTIVATE -> expected.activated(proof); };
        if(next.equals(expected)) throw new IllegalArgumentException("Recorded topology proof must retry its original command");
        return prepareProgress(operation,definition,expected,next,schemaHead,importHead,false);
    }
    public DistributedCommand preparePublication(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead) {
        expected.requireDefinition(definition);
        if(expected.phase()!=TopologyChange.Phase.SEEDED) throw new IllegalArgumentException("Topology publication requires exact SEEDED intent");
        return prepareProgress(operation,definition,expected,expected.publish(),schemaHead,importHead,true);
    }
    /** Completes administration only; native parents and their receipt/lineage/pin references remain retained. */
    public DistributedCommand prepareRetirement(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead) {
        expected.requireDefinition(definition);
        requireRetirementCoverage(expected);
        return prepareProgress(operation,definition,expected,expected.retire(),schemaHead,importHead,false);
    }
    private static void requireRetirementCoverage(TopologyChange.State state) {
        if(state.phase()!=TopologyChange.Phase.ACTIVE) throw new IllegalArgumentException("Topology retirement requires exact ACTIVE intent");
        for(var target:state.header().targets()) for(var owner:target.owners()) {
            var key=new TopologyChange.Key(target.groupId(),owner.replicaId());
            if(!state.seeded().containsKey(key) || !state.activated().containsKey(key)) throw new IllegalArgumentException("Topology retirement waits for every configured target owner");
        }
    }
    private DistributedCommand prepareProgress(OperationId operation,TopologyChange.Definition definition,TopologyChange.State expected,TopologyChange.State next,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead,boolean publication) {
        var header=next.header(); byte[] bytes=next.encode();
        var directory=expected.phase()==TopologyChange.Phase.PUBLISHED || expected.phase()==TopologyChange.Phase.ACTIVE ? definition.targetDirectory() : definition.sourceDirectory();
        var writes=new ArrayList<Mutation>(); writes.add(put(stateKey(header.intentId()),bytes)); writes.add(put(headKey(header.domainId()),bytes));
        if(publication) writes.add(put(domainKey(header.domainId()),RangeDirectoryCodec.encode(definition.targetDirectory())));
        return checked(operation,writes,List.of(predicate(stateKey(header.intentId()),expected.encode()),predicate(headKey(header.domainId()),expected.encode()),
                predicate(domainKey(header.domainId()),RangeDirectoryCodec.encode(directory)),predicate(identityKey(header.intentId()),definition.digest()),
                predicate(RatisSchemaAdministration.headKey(header.domainId()),schemaHead==null ? null : schemaHead.encode()),predicate(RatisRangeImportCatalogue.headKey(header.domainId()),importHead==null ? null : importHead.encode())));
    }
    public static boolean requiresOwnerAdmission(DistributedCommand command) {
        if(!isCommand(command) || command.mutations().size()<2 || command.predicates().size()!=6 || command.predicates().getFirst().expectedValue()==null) return false;
        var previous=TopologyChange.State.decode(command.predicates().getFirst().expectedValue());
        var next=TopologyChange.State.decode(command.mutations().getFirst().value());
        return previous.phase()!=TopologyChange.Phase.PREPARED || next.phase()!=TopologyChange.Phase.FENCING && next.phase()!=TopologyChange.Phase.JOINING;
    }
    /** Fresh preparation only. Deterministic validation and committed replay never call remote owners. */
    public static void admitExecution(DistributedCommand command,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,ImmutableReader reader,OwnerProofReader owners) throws IOException {
        validator(cluster,database,group,epoch,schema,column,reader).accept(command);
        if(!requiresOwnerAdmission(command)) throw new IllegalArgumentException("Expected topology execution progress/publication");
        if(reader==null || owners==null) throw new IOException("Fresh topology owner admission is not configured");
        requireCurrentWitnesses(command,reader);
        var previous=TopologyChange.State.decode(command.predicates().getFirst().expectedValue()); var next=TopologyChange.State.decode(command.mutations().getFirst().value());
        var definition=TopologyChange.Definition.decode(reader.read(definitionKey(next.header().definitionDigest()))); next.requireDefinition(definition);
        if((previous.phase()==TopologyChange.Phase.SEEDED && next.phase()==TopologyChange.Phase.PUBLISHED) || (previous.phase()==TopologyChange.Phase.ACTIVE && next.phase()==TopologyChange.Phase.RETIRED)) {
            boolean retirement=next.phase()==TopologyChange.Phase.RETIRED;
            for(var target:definition.header().targets()) {
                int qualified=0; IOException unavailable=null;
                for(var owner:target.owners()) {
                    if(owner.role()!=ReplicaDescriptor.Role.VOTER) continue;
                    var proof=(retirement ? previous.activated() : previous.seeded()).get(new TopologyChange.Key(target.groupId(),owner.replicaId())); if(proof==null) continue;
                    try { requireQualified(proof,owners.read(definition,proof)); qualified++; }
                    catch(IOException invalid) { unavailable=invalid; }
                    if(qualified==target.quorum()) break;
                }
                if(qualified<target.quorum()) throw new IOException(retirement ? "Topology retirement lacks fresh committed child ACTIVATE quorum" : "Topology publication lacks fresh committed child SEED quorum",unavailable);
            }
        } else {
            var proof=addedProof(previous,next); requireQualified(proof,owners.read(definition,proof));
        }
        requireCurrentWitnesses(command,reader);
    }
    private static void requireCurrentWitnesses(DistributedCommand command,ImmutableReader reader) throws IOException {
        for(var predicate:command.predicates()) if(!Arrays.equals(reader.read(predicate.key()),predicate.expectedValue())) throw new IOException("Topology execution lost exact committed metadata witnesses");
    }
    public static TopologyChange.Proof executionProof(DistributedCommand command) {
        if(!requiresOwnerAdmission(command) || command.mutations().size()!=2) throw new IllegalArgumentException("Expected single-owner topology proof command");
        var previous=TopologyChange.State.decode(command.predicates().getFirst().expectedValue()); var next=TopologyChange.State.decode(command.mutations().getFirst().value());
        next.requireSuccessorOf(previous); return addedProof(previous,next);
    }
    public static List<TopologyChange.Proof> publicationProofs(DistributedCommand command) {
        if(!requiresOwnerAdmission(command) || command.mutations().size()!=3) throw new IllegalArgumentException("Expected topology publication");
        var previous=TopologyChange.State.decode(command.predicates().getFirst().expectedValue()); var next=TopologyChange.State.decode(command.mutations().getFirst().value());
        next.requireSuccessorOf(previous);
        if(previous.phase()!=TopologyChange.Phase.SEEDED || next.phase()!=TopologyChange.Phase.PUBLISHED) throw new IllegalArgumentException("Expected SEEDED publication");
        return previous.seeded().values().stream().filter(proof -> proof.owner().role()==ReplicaDescriptor.Role.VOTER).sorted(Comparator.comparing(TopologyChange.Proof::groupId,RangeDirectoryCodec.UUID_ORDER).thenComparing(proof -> proof.owner().replicaId(),RangeDirectoryCodec.UUID_ORDER)).toList();
    }
    private static void requireQualified(TopologyChange.Proof expected,QualifiedOwner actual) throws IOException {
        if(actual==null || !expected.equals(actual.proof())) throw new IOException("Topology owner changed its original native proof");
        var cut=actual.completedCut(); var original=expected.position();
        if(cut.term()<1 || cut.index()<original.index() || cut.term()<original.term() || original.term()>0 && cut.index()==original.index() && cut.term()!=original.term()) throw new IOException("Topology owner lacks a completed committed native cut covering its proof");
    }
    private static Map<TopologyChange.Key,TopologyChange.Proof> proofs(TopologyChange.State state,TopologyChange.Stage stage) { return switch(stage) { case FREEZE -> state.frozen(); case SEED -> state.seeded(); case ACTIVATE -> state.activated(); }; }
    /** Decode the single new native owner proof; never derive one owner's proof from another row. */
    private static TopologyChange.Proof addedProof(TopologyChange.State previous,TopologyChange.State next) {
        TopologyChange.Proof result=null;
        for(var stage:TopologyChange.Stage.values()) for(var row:proofs(next,stage).entrySet()) if(!proofs(previous,stage).containsKey(row.getKey())) {
            if(result!=null) throw new IllegalArgumentException("Topology progress adds more than one owner proof"); result=row.getValue();
        }
        if(result==null) throw new IllegalArgumentException("Topology progress must add one original owner proof"); return result;
    }
    public CommandResult execute(DistributedCommand command) throws IOException { validator.accept(command); try { return CommandResult.decode(content(client.io().send(message(DistributedCommandCodec.encode(command))),8)); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology command reply",invalid); } }
    private DistributedCommand checked(OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) { var command=new DistributedCommand(group,epoch,schema,operation,mutations,predicates); validator.accept(command); DistributedCommandCodec.encode(command); return command; }
    private Mutation put(byte[] key,byte[] value) { return new Mutation(Mutation.Kind.PUT,column,key,value); }
    private KeyPredicate predicate(byte[] key,byte[] value) { return new KeyPredicate(column,key,value); }
    public static boolean isCommand(DistributedCommand command) { return command.mutations().stream().anyMatch(m->{ byte[] key=m.key(); return key.length==33 && key[0]==DEFINITION || key.length==17 && (key[0]==STATE || key[0]==HEAD || key[0]==IDENTITY) || key.length==1 && key[0]==CAPACITY; }); }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,ImmutableReader reader) {
        byte[] digest=schema.clone(); return command->{
            if(command.kind()!=DistributedCommand.Kind.DATA || command.hasBucketData() || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1 || !command.groupId().equals(group) || command.rangeEpoch()!=epoch || !Arrays.equals(command.schemaDigest(),digest)) throw new IllegalArgumentException("Invalid topology catalogue envelope");
            for(var mutation:command.mutations()) if(mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(column)) throw new IllegalArgumentException("Invalid topology write");
            if(command.mutations().size()==3 && command.predicates().size()==3) {
                var definition=TopologyChange.Definition.decode(command.mutations().getFirst().value()); requireDefinition(definition,cluster,database,definition.digest());
                byte[][] keys={definitionKey(definition.digest()),identityKey(definition.header().intentId()),capacityKey()};
                for(int i=0;i<3;i++) { mutation(command.mutations().get(i),keys[i]); guard(command.predicates().get(i),column,keys[i]); }
                if(command.predicates().get(0).expectedValue()!=null || command.predicates().get(1).expectedValue()!=null || !Arrays.equals(command.mutations().get(1).value(),definition.digest())
                        || !Capacity.decode(command.mutations().get(2).value()).equals(Capacity.decode(command.predicates().get(2).expectedValue()).reserve(definition.encode().length))) throw new IllegalArgumentException("Topology definition identity/capacity differs");
                return;
            }
            if((command.mutations().size()!=2 && command.mutations().size()!=3) || command.predicates().size()!=2 && command.predicates().size()!=6) throw new IllegalArgumentException("Invalid topology intent shape");
            var next=TopologyChange.State.decode(command.mutations().getFirst().value()); var header=next.header(); requireNamespace(header,cluster,database);
            mutation(command.mutations().get(0),stateKey(header.intentId())); mutation(command.mutations().get(1),headKey(header.domainId()));
            if(!Arrays.equals(command.mutations().get(0).value(),command.mutations().get(1).value())) throw new IllegalArgumentException("Topology head/state differ");
            guard(command.predicates().get(0),column,stateKey(header.intentId())); guard(command.predicates().get(1),column,headKey(header.domainId()));
            if(command.predicates().size()==2) {
                var previous=TopologyChange.State.decode(command.predicates().get(0).expectedValue());
                if(!Arrays.equals(command.predicates().get(0).expectedValue(),command.predicates().get(1).expectedValue())) throw new IllegalArgumentException("Topology predecessor head/state differ");
                next.requireSuccessorOf(previous);
                if(command.mutations().size()!=2 || previous.phase()!=TopologyChange.Phase.PREPARED || next.phase()!=TopologyChange.Phase.ABORTED) throw new IllegalArgumentException("Native topology execution authority is not configured");
                return;
            }
            var oldState=command.predicates().get(0).expectedValue(); var oldHead=command.predicates().get(1).expectedValue();
            if(oldState==null) {
                if(command.mutations().size()!=2 || !next.equals(TopologyChange.State.begin(header))) throw new IllegalArgumentException("Topology BEGIN must be initial");
                if(oldHead!=null) { var previous=TopologyChange.State.decode(oldHead); requireNamespace(previous.header(),cluster,database); if(!previous.header().domainId().equals(header.domainId()) || !previous.terminal()) throw new IllegalArgumentException("Topology predecessor is not terminal"); }
            } else {
                var previous=TopologyChange.State.decode(oldState); next.requireSuccessorOf(previous);
                if(!Arrays.equals(oldState,oldHead)) throw new IllegalArgumentException("Topology progress requires exact predecessor head/state");
                if(previous.phase()==TopologyChange.Phase.PREPARED) {
                    if(command.mutations().size()!=2 || next.phase()!=(header.kind()==TopologyChange.Kind.RELOCATE?TopologyChange.Phase.JOINING:TopologyChange.Phase.FENCING)) throw new IllegalArgumentException("Topology execution requires exact prepared head/state");
                } else if(header.kind()==TopologyChange.Kind.RELOCATE){
                    if(previous.phase()==TopologyChange.Phase.JOINING && next.phase()==TopologyChange.Phase.RECONFIGURED){if(command.mutations().size()!=2)throw new IllegalArgumentException("Relocation progress writes only intent/head");}
                    else if(previous.phase()==TopologyChange.Phase.RECONFIGURED && next.phase()==TopologyChange.Phase.ACTIVE){if(command.mutations().size()!=3)throw new IllegalArgumentException("Relocation publication must atomically replace directory");}
                    else throw new IllegalArgumentException("Invalid relocation execution step");
                } else if(previous.phase()==TopologyChange.Phase.SEEDED && next.phase()==TopologyChange.Phase.PUBLISHED) {
                    if(command.mutations().size()!=3) throw new IllegalArgumentException("Topology publication must atomically replace its directory");
                } else if(previous.phase()==TopologyChange.Phase.ACTIVE && next.phase()==TopologyChange.Phase.RETIRED) {
                    if(command.mutations().size()!=2 || !next.equals(previous.retire())) throw new IllegalArgumentException("Topology retirement must retain original execution proofs");
                    requireRetirementCoverage(previous);
                } else {
                    if(command.mutations().size()!=2 || next.phase()==TopologyChange.Phase.RETIRED || previous.terminal()) throw new IllegalArgumentException("Topology terminal retention barriers are not configured");
                    var proof=addedProof(previous,next);
                    boolean allowed=switch(proof.stage()) {
                        case FREEZE -> previous.phase()==TopologyChange.Phase.FENCING || previous.phase()==TopologyChange.Phase.FENCED || previous.phase()==TopologyChange.Phase.SEEDED;
                        case SEED -> previous.phase()==TopologyChange.Phase.FENCED || previous.phase()==TopologyChange.Phase.SEEDED || previous.phase()==TopologyChange.Phase.PUBLISHED || previous.phase()==TopologyChange.Phase.ACTIVE;
                        case ACTIVATE -> previous.phase()==TopologyChange.Phase.PUBLISHED || previous.phase()==TopologyChange.Phase.ACTIVE;
                    };
                    if(!allowed) throw new IllegalArgumentException("Topology native proof belongs to another execution stage");
                }
            }
            if(reader==null) throw new IllegalArgumentException("Topology immutable definition reader unavailable");
            var definition=TopologyChange.Definition.decode(reader.read(definitionKey(header.definitionDigest()))); header.requireDefinition(definition); requireDefinition(definition,cluster,database,header.definitionDigest());
            var previous=oldState==null ? null : TopologyChange.State.decode(oldState);
            var directory=previous!=null && (previous.phase()==TopologyChange.Phase.PUBLISHED || previous.phase()==TopologyChange.Phase.ACTIVE) ? definition.targetDirectory() : definition.sourceDirectory();
            guard(command.predicates().get(2),column,domainKey(header.domainId())); if(!Arrays.equals(command.predicates().get(2).expectedValue(),RangeDirectoryCodec.encode(directory))) throw new IllegalArgumentException("Topology stage directory differs");
            if(command.mutations().size()==3) { mutation(command.mutations().get(2),domainKey(header.domainId())); if(!Arrays.equals(command.mutations().get(2).value(),RangeDirectoryCodec.encode(definition.targetDirectory()))) throw new IllegalArgumentException("Topology publication target directory differs"); }
            guard(command.predicates().get(3),column,identityKey(header.intentId())); if(!Arrays.equals(command.predicates().get(3).expectedValue(),header.definitionDigest())) throw new IllegalArgumentException("Topology definition identity is not committed");
            RatisSchemaAdministration.requireTopologyGuard(command.predicates().get(4),directory); RatisRangeImportCatalogue.requireTopologyGuard(command.predicates().get(5),directory);
            if(!command.predicates().get(4).columnId().equals(column) || !command.predicates().get(5).columnId().equals(column)) throw new IllegalArgumentException("Topology administrative guard column differs");
        };
    }
    private static void mutation(Mutation mutation,byte[] key) { if(!Arrays.equals(mutation.key(),key)) throw new IllegalArgumentException("Topology mutation key differs"); }
    private static void guard(KeyPredicate predicate,UUID column,byte[] key) { if(predicate.bucket() || !predicate.columnId().equals(column) || !Arrays.equals(predicate.key(),key)) throw new IllegalArgumentException("Topology guard differs"); }
    private static void requireDefinition(TopologyChange.Definition definition,UUID cluster,UUID database,byte[] digest) {
        requireNamespace(definition.header(),cluster,database);
        if(!Arrays.equals(definition.digest(),digest)) throw new IllegalArgumentException("Topology definition commitment differs");
        if(RangeDirectoryCodec.encodedSize(definition.sourceDirectory())>RatisCatalogue.MAX_DIRECTORY_BYTES || RangeDirectoryCodec.encodedSize(definition.targetDirectory())>RatisCatalogue.MAX_DIRECTORY_BYTES) throw new IllegalArgumentException("Topology source/target exceeds catalogue directory budget");
    }
    private static void requireNamespace(TopologyChange.Header header,UUID cluster,UUID database) { if(!header.clusterId().equals(cluster) || !header.databaseId().equals(database)) throw new IllegalArgumentException("Foreign topology namespace"); }
    /** Other administration owns the domain only while its topology head is absent or terminal. */
    public static void requireAdministrationGuard(KeyPredicate predicate,RangeDirectory directory) {
        if(predicate.bucket() || !Arrays.equals(predicate.key(),headKey(directory.domainId()))) throw new IllegalArgumentException("Missing topology administration guard");
        if(predicate.expectedValue()!=null) {
            var state=TopologyChange.State.decode(predicate.expectedValue()); requireNamespace(state.header(),directory.clusterId(),directory.databaseId());
            if(!state.terminal() || !state.header().domainId().equals(directory.domainId())) throw new IllegalArgumentException("Active/foreign topology prevents other administration");
        }
    }
    public static byte[] stateKey(UUID id) { return idKey(STATE,id); }
    public static byte[] statePrefix() { return new byte[]{STATE}; }
    public static byte[] headKey(UUID id) { return idKey(HEAD,id); }
    public static byte[] identityKey(UUID id) { return idKey(IDENTITY,id); }
    public static byte[] definitionKey(byte[] digest) { if(digest.length!=32) throw new IllegalArgumentException("Invalid topology digest"); return ByteBuffer.allocate(33).put(DEFINITION).put(digest).array(); }
    public static byte[] capacityKey() { return new byte[]{CAPACITY}; }
    private static byte[] idKey(byte tag,UUID id) { return ByteBuffer.allocate(17).put(tag).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
    private static byte[] domainKey(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
    private byte[] uncheckedRead(byte[] key) { try { return read(key,TopologyChange.MAX_DEFINITION_BYTES); } catch(IOException failure) { throw new IllegalArgumentException("Topology definition read failed",failure); } }
    private byte[] read(byte[] key,int maximum) throws IOException { try { return ScopedTransactionRpc.pointValue(content(client.io().sendReadOnly(message(ScopedTransactionRpc.pointRequest(column,key))),ScopedTransactionRpc.HEADER_BYTES+4+maximum),maximum); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology read reply",invalid); } }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException { if(reply==null || !reply.isSuccess() || reply.getMessage()==null) throw new IOException("Topology catalogue operation failed",reply==null ? null : reply.getException()); var bytes=reply.getMessage().getContent(); if(bytes.size()>maximum) throw new IOException("Topology reply exceeds operation budget"); return bytes.toByteArray(); }
}
