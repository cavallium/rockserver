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
    public CommandResult execute(DistributedCommand command) throws IOException { validator.accept(command); try { return CommandResult.decode(content(client.io().send(message(DistributedCommandCodec.encode(command))),8)); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology command reply",invalid); } }
    private DistributedCommand checked(OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) { var command=new DistributedCommand(group,epoch,schema,operation,mutations,predicates); validator.accept(command); return command; }
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
            if(command.mutations().size()!=2 || command.predicates().size()!=2 && command.predicates().size()!=6) throw new IllegalArgumentException("Invalid topology intent shape");
            var next=TopologyChange.State.decode(command.mutations().getFirst().value()); var header=next.header(); requireNamespace(header,cluster,database);
            mutation(command.mutations().get(0),stateKey(header.intentId())); mutation(command.mutations().get(1),headKey(header.domainId()));
            if(!Arrays.equals(command.mutations().get(0).value(),command.mutations().get(1).value())) throw new IllegalArgumentException("Topology head/state differ");
            guard(command.predicates().get(0),column,stateKey(header.intentId())); guard(command.predicates().get(1),column,headKey(header.domainId()));
            if(command.predicates().size()==2) {
                var previous=TopologyChange.State.decode(command.predicates().get(0).expectedValue());
                if(!Arrays.equals(command.predicates().get(0).expectedValue(),command.predicates().get(1).expectedValue())) throw new IllegalArgumentException("Topology predecessor head/state differ");
                next.requireSuccessorOf(previous);
                if(previous.phase()!=TopologyChange.Phase.PREPARED || next.phase()!=TopologyChange.Phase.ABORTED) throw new IllegalArgumentException("Native topology execution authority is not configured");
                return;
            }
            if(command.predicates().get(0).expectedValue()!=null || !next.equals(TopologyChange.State.begin(header))) throw new IllegalArgumentException("Topology BEGIN must be initial");
            var oldHead=command.predicates().get(1).expectedValue(); if(oldHead!=null) { var previous=TopologyChange.State.decode(oldHead); requireNamespace(previous.header(),cluster,database); if(!previous.header().domainId().equals(header.domainId()) || !previous.terminal()) throw new IllegalArgumentException("Topology predecessor is not terminal"); }
            if(reader==null) throw new IllegalArgumentException("Topology immutable definition reader unavailable");
            var definition=TopologyChange.Definition.decode(reader.read(definitionKey(header.definitionDigest()))); header.requireDefinition(definition);
            guard(command.predicates().get(2),column,domainKey(header.domainId())); if(!Arrays.equals(command.predicates().get(2).expectedValue(),RangeDirectoryCodec.encode(definition.sourceDirectory()))) throw new IllegalArgumentException("Topology source directory differs");
            guard(command.predicates().get(3),column,identityKey(header.intentId())); if(!Arrays.equals(command.predicates().get(3).expectedValue(),header.definitionDigest())) throw new IllegalArgumentException("Topology definition identity is not committed");
            RatisSchemaAdministration.requireTopologyGuard(command.predicates().get(4),definition.sourceDirectory()); RatisRangeImportCatalogue.requireTopologyGuard(command.predicates().get(5),definition.sourceDirectory());
            if(!command.predicates().get(4).columnId().equals(column) || !command.predicates().get(5).columnId().equals(column)) throw new IllegalArgumentException("Topology administrative guard column differs");
        };
    }
    private static void mutation(Mutation mutation,byte[] key) { if(!Arrays.equals(mutation.key(),key)) throw new IllegalArgumentException("Topology mutation key differs"); }
    private static void guard(KeyPredicate predicate,UUID column,byte[] key) { if(predicate.bucket() || !predicate.columnId().equals(column) || !Arrays.equals(predicate.key(),key)) throw new IllegalArgumentException("Topology guard differs"); }
    private static void requireDefinition(TopologyChange.Definition definition,UUID cluster,UUID database,byte[] digest) { requireNamespace(definition.header(),cluster,database); if(!Arrays.equals(definition.digest(),digest)) throw new IllegalArgumentException("Topology definition commitment differs"); }
    private static void requireNamespace(TopologyChange.Header header,UUID cluster,UUID database) { if(!header.clusterId().equals(cluster) || !header.databaseId().equals(database)) throw new IllegalArgumentException("Foreign topology namespace"); }
    public static byte[] stateKey(UUID id) { return idKey(STATE,id); }
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
