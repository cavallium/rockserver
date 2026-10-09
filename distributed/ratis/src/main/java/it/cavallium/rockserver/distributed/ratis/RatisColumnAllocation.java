package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Permanent global high water and immutable request-bound reservations. Borrowed client, no implicit rebasing. */
public final class RatisColumnAllocation {
    public static final int COUNTER_TAG=0x61, RESERVATION_TAG=0x62, RESERVATION_BYTES=40;
    private final RaftClient client;
    private final UUID group,column;
    private final long epoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;
    public RatisColumnAllocation(RaftClient client,UUID group,long epoch,byte[] schema,UUID column) {
        this.client=Objects.requireNonNull(client); this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column);
        if (epoch<0 || schema.length!=32 || !client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Invalid allocation binding");
        this.epoch=epoch; this.schema=schema.clone(); validator=validator(group,epoch,schema,column);
    }
    public record Reservation(UUID requestId,UUID domainId,UUID columnId,long apiId) {
        public Reservation { Objects.requireNonNull(requestId); Objects.requireNonNull(domainId); Objects.requireNonNull(columnId); if (apiId<=0) throw new IllegalArgumentException("Invalid reserved API ID"); }
        public byte[] encode() { return ByteBuffer.allocate(RESERVATION_BYTES).putLong(domainId.getMostSignificantBits()).putLong(domainId.getLeastSignificantBits()).putLong(columnId.getMostSignificantBits()).putLong(columnId.getLeastSignificantBits()).putLong(apiId).array(); }
        public static Reservation decode(UUID requestId,byte[] value) {
            if (value==null || value.length!=RESERVATION_BYTES) throw new IllegalArgumentException("Invalid reservation value");
            var in=ByteBuffer.wrap(value); return new Reservation(requestId,new UUID(in.getLong(),in.getLong()),new UUID(in.getLong(),in.getLong()),in.getLong());
        }
        public void requireOwner(UUID domain,UUID column,long api) { if (!domainId.equals(domain) || !columnId.equals(column) || apiId!=api) throw new IllegalArgumentException("Reservation belongs to another issuance"); }
    }
    public static byte[] counterKey() { return new byte[]{COUNTER_TAG}; }
    public static byte[] reservationKey(UUID request) { Objects.requireNonNull(request); return ByteBuffer.allocate(17).put((byte)RESERVATION_TAG).putLong(request.getMostSignificantBits()).putLong(request.getLeastSignificantBits()).array(); }
    public static long counter(byte[] value) { if (value==null) return 0; if (value.length!=8) throw new IllegalArgumentException("Invalid allocation counter witness"); long n=ByteBuffer.wrap(value).getLong(); if (n<0) throw new IllegalArgumentException("Negative allocation counter"); return n; }
    public static byte[] counterValue(long value) { if (value<0) throw new IllegalArgumentException("Negative allocation counter"); return ByteBuffer.allocate(8).putLong(value).array(); }
    public byte[] readCounter() throws IOException { byte[] value=read(counterKey(),8); try { counter(value); return value; } catch (IllegalArgumentException invalid) { throw new IOException("Invalid global allocation counter",invalid); } }
    public Reservation read(UUID request) throws IOException { byte[] value=read(reservationKey(request),RESERVATION_BYTES); try { return value==null ? null : Reservation.decode(request,value); } catch (IllegalArgumentException invalid) { throw new IOException("Invalid column reservation",invalid); } }
    public DistributedCommand prepare(OperationId operation,UUID request,UUID domain,UUID targetColumn,byte[] frozenCounterWitness) {
        long previous=counter(frozenCounterWitness); if (previous==Long.MAX_VALUE) throw new IllegalStateException("Global column API IDs exhausted");
        var reservation=new Reservation(request,domain,targetColumn,previous+1);
        var command=new DistributedCommand(group,epoch,schema,operation,
                List.of(new Mutation(Mutation.Kind.PUT,column,reservationKey(request),reservation.encode()),new Mutation(Mutation.Kind.PUT,column,counterKey(),counterValue(previous+1))),
                List.of(new KeyPredicate(column,reservationKey(request),null),new KeyPredicate(column,counterKey(),frozenCounterWitness)));
        validator.accept(command); return command;
    }
    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command); var result=content(RatisRoutedClient.rpc(() -> client.io().send(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command))))),CommandResult.MAX_BYTES);
        try { var decoded=CommandResult.decode(result); if (decoded.valueCount()!=0 || decoded.status()==CommandResult.Status.RESULT_TOO_LARGE) throw new IllegalArgumentException("Invalid allocation outcome"); return decoded; }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid allocation reply",invalid); }
    }
    public static boolean isAllocationCommand(DistributedCommand command) { if (command.mutations().isEmpty()) return false; byte[] key=command.mutations().getFirst().key(); return key.length!=16 && key.length>0 && (key[0]==COUNTER_TAG || key[0]==RESERVATION_TAG); }
    public static Consumer<DistributedCommand> validator(UUID group,long epoch,byte[] schema,UUID column) {
        byte[] expected=schema.clone();
        return command -> {
            if (command.kind()!=DistributedCommand.Kind.DATA || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1 || command.hasBucketData() || !command.groupId().equals(group) || command.rangeEpoch()!=epoch || !Arrays.equals(command.schemaDigest(),expected)
                    || command.mutations().size()!=2 || command.predicates().size()!=2) throw new IllegalArgumentException("Invalid allocation envelope");
            byte[] key=command.mutations().getFirst().key(); if (key.length!=17 || key[0]!=RESERVATION_TAG) throw new IllegalArgumentException("Invalid reservation key");
            var in=ByteBuffer.wrap(key); in.get(); var request=new UUID(in.getLong(),in.getLong());
            var reservation=Reservation.decode(request,command.mutations().getFirst().value());
            requirePair(command,0,column,key); requirePair(command,1,column,counterKey());
            long previous=counter(command.predicates().get(1).expectedValue());
            if (command.predicates().getFirst().expectedValue()!=null || previous==Long.MAX_VALUE || reservation.apiId()!=previous+1
                    || counter(command.mutations().get(1).value())!=reservation.apiId()) throw new IllegalArgumentException("Invalid reservation/counter successor");
        };
    }
    static void requirePair(DistributedCommand command,int index,UUID column,byte[] key) {
        var m=command.mutations().get(index); var p=command.predicates().get(index);
        if (m.kind()!=Mutation.Kind.PUT || m.returnMode()!=Mutation.ReturnMode.NONE || m.bucket() || p.bucket() || !m.columnId().equals(column) || !p.columnId().equals(column)
                || !Arrays.equals(m.key(),key) || !Arrays.equals(p.key(),key)) throw new IllegalArgumentException("Invalid allocation CAS pair");
    }
    private byte[] read(byte[] key,int maximum) throws IOException {
        try { return ScopedTransactionRpc.pointValue(content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.pointRequest(column,key))))),maximum+20),maximum); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid allocation point reply",invalid); }
    }
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException {
        if (reply==null || !reply.isSuccess() || reply.getMessage()==null) throw new IOException("Allocation request failed",reply==null ? null : reply.getException());
        var bytes=reply.getMessage().getContent(); if (bytes==null || bytes.size()>maximum) throw new IOException("Allocation reply exceeds budget"); return bytes.toByteArray();
    }
}
