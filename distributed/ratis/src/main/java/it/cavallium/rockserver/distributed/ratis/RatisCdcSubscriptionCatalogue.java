package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Protected catalogue rows. Creation reserves fixed state capacity; closure retains its identity.
 * New delivery bounds require fresh configured admission; ACKs consume only persisted bounds. */
public final class RatisCdcSubscriptionCatalogue {
    public static final int MAX_SUBSCRIPTIONS=1024;
    public static final long MAX_STATE_BYTES=64L*1024*1024;
    private static final byte STATE=(byte)0x80,CAPACITY=(byte)0x81;
    public record Capacity(int count,long bytes) {
        public Capacity {
            if(count<0 || count>MAX_SUBSCRIPTIONS || bytes<0 || bytes>MAX_STATE_BYTES || (count==0)!=(bytes==0)) throw new IllegalArgumentException("Invalid CDC capacity ledger");
        }
        public byte[] encode() { return ByteBuffer.allocate(12).putInt(count).putLong(bytes).array(); }
        public static Capacity decode(byte[] bytes) {
            if(bytes==null) return new Capacity(0,0);
            if(bytes.length!=12) throw new IllegalArgumentException("Invalid CDC capacity ledger length"); var input=ByteBuffer.wrap(bytes); return new Capacity(input.getInt(),input.getLong());
        }
        Capacity reserve(int stateBytes) { return new Capacity(Math.incrementExact(count),Math.addExact(bytes,17L+stateBytes)); }
    }
    private final RaftClient client;
    private final UUID cluster,database,group,column;
    private final long epoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;
    public RatisCdcSubscriptionCatalogue(RaftClient client,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column);
        if(!client.getGroupId().getUuid().equals(group) || epoch<0 || schema.length!=32) throw new IllegalArgumentException("Invalid CDC catalogue binding");
        this.epoch=epoch; this.schema=schema.clone(); validator=validator(cluster,database,group,epoch,schema,column);
    }
    public DistributedCdcSubscription read(UUID subscription) throws IOException {
        byte[] bytes=point(stateKey(subscription),DistributedCdcSubscription.MAX_BYTES); if(bytes==null) return null;
        try { var state=DistributedCdcSubscription.decode(bytes); requireNamespace(state.definition(),cluster,database); if(!state.definition().subscriptionId().equals(subscription)) throw new IllegalArgumentException("CDC read identity changed"); return state; }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC catalogue state",invalid); }
    }
    /** Null retains the exact absent-ledger CAS guard. */
    public Capacity readCapacity() throws IOException {
        byte[] bytes=point(capacityKey(),12); if(bytes==null) return null;
        try { return Capacity.decode(bytes); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC capacity reply",invalid); }
    }
    public DistributedCommand prepareCreate(OperationId operation,DistributedCdcSubscription.Definition definition,RangeDirectory source,Capacity expected) {
        var state=DistributedCdcSubscription.initial(definition); byte[] encoded=state.encode(); var capacity=(expected==null ? new Capacity(0,0) : expected).reserve(encoded.length);
        var command=new DistributedCommand(group,epoch,schema,operation,List.of(new Mutation(Mutation.Kind.PUT,column,stateKey(definition.subscriptionId()),encoded),new Mutation(Mutation.Kind.PUT,column,capacityKey(),capacity.encode())),
                List.of(new KeyPredicate(column,stateKey(definition.subscriptionId()),null),new KeyPredicate(column,capacityKey(),expected==null ? null : expected.encode()),new KeyPredicate(column,domainKey(definition.domainId()),RangeDirectoryCodec.encode(source))));
        validator.accept(command); return command;
    }
    public DistributedCommand prepareClose(OperationId operation,DistributedCdcSubscription expected) {
        if(expected.phase()!=DistributedCdcSubscription.Phase.ACTIVE) throw new IllegalArgumentException("CDC close needs active predecessor");
        byte[] key=stateKey(expected.definition().subscriptionId()); var command=new DistributedCommand(group,epoch,schema,operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,expected.close().encode())),List.of(new KeyPredicate(column,key,expected.encode())));
        validator.accept(command); return command;
    }
    public DistributedCommand prepareDelivery(OperationId operation,DistributedCdcSubscription expected,DistributedCdcCheckpoint delivered) {
        return transition(operation,expected,expected.deliver(delivered));
    }
    public DistributedCommand prepareAcknowledgement(OperationId operation,DistributedCdcSubscription expected,DistributedCdcCheckpoint acknowledged) {
        return transition(operation,expected,expected.acknowledge(acknowledged));
    }
    private DistributedCommand transition(OperationId operation,DistributedCdcSubscription expected,DistributedCdcSubscription next) {
        byte[] key=stateKey(expected.definition().subscriptionId()); var command=new DistributedCommand(group,epoch,schema,operation,
                List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,expected.encode())));
        validator.accept(command); return command;
    }
    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command);
        try { return CommandResult.decode(content(client.io().send(message(DistributedCommandCodec.encode(command))),8)); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC command reply",invalid); }
    }
    public void prepareDeliveryOwner(DistributedCommand command) throws IOException {
        validator.accept(command);
        byte[] request=RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.PREPARE,command);
        try { RatisCdcDeliveryRpc.requireEcho(content(client.io().sendReadOnly(message(request)),request.length),RatisCdcDeliveryRpc.PREPARE,command); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC delivery preparation reply",invalid); }
    }
    public void cancelDelivery(DistributedCommand command) throws IOException {
        byte[] request=RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.CANCEL,command);
        try { RatisCdcDeliveryRpc.requireEcho(content(client.io().sendReadOnly(message(request)),request.length),RatisCdcDeliveryRpc.CANCEL,command); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC delivery cancellation reply",invalid); }
    }
    public static boolean isCommand(DistributedCommand command) {
        return command.mutations().stream().anyMatch(m->{ var key=m.key(); return key.length==17 && key[0]==STATE || key.length==1 && key[0]==CAPACITY; });
    }
    public static boolean requiresOwnerAdmission(DistributedCommand command) {
        if(!isCommand(command) || command.mutations().size()!=1 || command.predicates().size()!=1 || command.predicates().getFirst().expectedValue()==null) return false;
        var next=DistributedCdcSubscription.decode(command.mutations().getFirst().value()); var previous=DistributedCdcSubscription.decode(command.predicates().getFirst().expectedValue());
        return next.phase()==DistributedCdcSubscription.Phase.ACTIVE && next.checkpoint().equals(previous.checkpoint()) && !next.delivered().equals(previous.delivered());
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(group); Objects.requireNonNull(column); byte[] digest=schema.clone();
        return command->{
            if(command.kind()!=DistributedCommand.Kind.DATA || command.hasBucketData() || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1
                    || !command.groupId().equals(group) || command.rangeEpoch()!=epoch || !Arrays.equals(command.schemaDigest(),digest)) throw new IllegalArgumentException("Invalid CDC catalogue envelope");
            if(command.mutations().isEmpty()) throw new IllegalArgumentException("Missing CDC state mutation");
            var mutation=command.mutations().getFirst();
            if(mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(column)) throw new IllegalArgumentException("Invalid CDC state write");
            var next=DistributedCdcSubscription.decode(mutation.value()); var definition=next.definition(); requireNamespace(definition,cluster,database);
            if(!Arrays.equals(mutation.key(),stateKey(definition.subscriptionId()))) throw new IllegalArgumentException("CDC state key differs");
            if(command.mutations().size()==2 && command.predicates().size()==3) {
                if(!next.equals(DistributedCdcSubscription.initial(definition))) throw new IllegalArgumentException("CDC creation must be initial state");
                predicate(command.predicates().get(0),column,mutation.key()); if(command.predicates().get(0).expectedValue()!=null) throw new IllegalArgumentException("CDC creation needs absent identity");
                var ledger=command.mutations().get(1); predicate(command.predicates().get(1),column,capacityKey());
                if(ledger.kind()!=Mutation.Kind.PUT || ledger.returnMode()!=Mutation.ReturnMode.NONE || !ledger.columnId().equals(column) || !Arrays.equals(ledger.key(),capacityKey())
                        || !Capacity.decode(ledger.value()).equals(Capacity.decode(command.predicates().get(1).expectedValue()).reserve(mutation.value().length))) throw new IllegalArgumentException("CDC capacity reservation changed");
                var source=command.predicates().get(2); predicate(source,column,domainKey(definition.domainId()));
                if(source.expectedValue()==null || source.expectedValue().length>RatisCatalogue.MAX_DIRECTORY_BYTES) throw new IllegalArgumentException("CDC creation lacks bounded source directory");
                var directory=RangeDirectoryCodec.decode(source.expectedValue());
                if(!definition.equals(DistributedCdcSubscription.Definition.create(definition.subscriptionId(),definition.incarnationId(),directory,definition.columns(),definition.start()))) throw new IllegalArgumentException("CDC creation source binding changed");
            } else if(command.mutations().size()==1 && command.predicates().size()==1) {
                var previous=command.predicates().getFirst(); predicate(previous,column,mutation.key()); if(previous.expectedValue()==null) throw new IllegalArgumentException("CDC closure lacks predecessor");
                var old=DistributedCdcSubscription.decode(previous.expectedValue());
                if(mutation.value().length!=previous.expectedValue().length) throw new IllegalArgumentException("CDC transition changed reserved capacity");
                next.requireSuccessorOf(old);
            } else throw new IllegalArgumentException("Invalid CDC lifecycle command shape");
        };
    }
    private static void predicate(KeyPredicate predicate,UUID column,byte[] key) { if(predicate.bucket() || !predicate.columnId().equals(column) || !Arrays.equals(predicate.key(),key)) throw new IllegalArgumentException("CDC predicate binding changed"); }
    private static void requireNamespace(DistributedCdcSubscription.Definition definition,UUID cluster,UUID database) { if(!definition.clusterId().equals(cluster) || !definition.databaseId().equals(database)) throw new IllegalArgumentException("Foreign CDC namespace"); }
    public static byte[] stateKey(UUID subscription) { return ByteBuffer.allocate(17).put(STATE).putLong(subscription.getMostSignificantBits()).putLong(subscription.getLeastSignificantBits()).array(); }
    public static byte[] capacityKey() { return new byte[]{CAPACITY}; }
    private static byte[] domainKey(UUID domain) { return ByteBuffer.allocate(16).putLong(domain.getMostSignificantBits()).putLong(domain.getLeastSignificantBits()).array(); }
    private byte[] point(byte[] key,int maximum) throws IOException {
        try { return ScopedTransactionRpc.pointValue(content(client.io().sendReadOnly(message(ScopedTransactionRpc.pointRequest(column,key))),ScopedTransactionRpc.HEADER_BYTES+4+maximum),maximum); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC point reply",invalid); }
    }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(org.apache.ratis.protocol.RaftClientReply reply,int maximum) throws IOException {
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null) throw new IOException("CDC catalogue operation failed",reply==null ? null : reply.getException());
        var bytes=reply.getMessage().getContent();
        if(bytes.size()>maximum) throw new IOException("CDC catalogue reply exceeds operation budget");
        return bytes.toByteArray();
    }
}
