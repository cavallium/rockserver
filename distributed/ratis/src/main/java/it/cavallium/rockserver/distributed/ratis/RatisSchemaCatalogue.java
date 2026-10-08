package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Immutable schema content in the directory CF, separated from its 16-byte domain keys. Client remains borrowed. */
public final class RatisSchemaCatalogue {
    public static final int MAX_SCHEMA_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES-256;
    private final RaftClient client;
    private final UUID cluster,database,group,column;
    private final long epoch;
    private final byte[] catalogueSchema;
    private final Consumer<DistributedCommand> validator;
    public RatisSchemaCatalogue(RaftClient client,UUID cluster,UUID database,UUID group,long epoch,byte[] catalogueSchema,UUID column) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database);
        this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column);
        if (epoch<0 || catalogueSchema.length!=32 || !client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Invalid schema catalogue binding");
        this.epoch=epoch; this.catalogueSchema=catalogueSchema.clone(); validator=validator(cluster,database,group,epoch,catalogueSchema,column);
    }
    public DistributedSchema read(byte[] digest) throws IOException {
        byte[] key=key(digest), query=ByteBuffer.allocate(49).put(uuid(column)).put(key).array();
        byte[] reply=content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(query))),MAX_SCHEMA_BYTES+4);
        if (reply.length<4) throw new IOException("Truncated schema catalogue reply");
        var input=ByteBuffer.wrap(reply); int size=input.getInt();
        if (size== -1 && !input.hasRemaining()) return null;
        if (size<0 || size!=input.remaining()) throw new IOException("Invalid schema catalogue reply length");
        byte[] encoded=new byte[size]; input.get(encoded);
        try { return decode(encoded,cluster,database,key); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid schema catalogue content",malformed); }
    }
    /** Prepare once; uncertain outcomes must retry these exact bytes and operation identity. */
    public DistributedCommand prepare(OperationId operation,DistributedSchema schema) {
        byte[] encoded=DistributedSchemaCodec.encode(Objects.requireNonNull(schema));
        if (encoded.length>MAX_SCHEMA_BYTES) throw new IllegalArgumentException("Schema exceeds publication command budget");
        byte[] key=key(schema.digest());
        var command=new DistributedCommand(group,epoch,catalogueSchema,operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,encoded)),
                List.of(new KeyPredicate(column,key,null)));
        validator.accept(command); return command;
    }
    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command);
        byte[] reply=content(RatisRoutedClient.rpc(() -> client.io().send(message(DistributedCommandCodec.encode(command)))),CommandResult.MAX_BYTES);
        try {
            var result=CommandResult.decode(reply);
            if (result.valueCount()!=0 || result.status()==CommandResult.Status.RESULT_TOO_LARGE) throw new IOException("Unexpected schema publication outcome");
            return result;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid schema publication result",malformed); }
    }
    public static byte[] key(byte[] digest) {
        if (digest==null || digest.length!=32) throw new IllegalArgumentException("Schema content digest must be 32 bytes");
        return ByteBuffer.allocate(33).put((byte)0x53).put(digest).array();
    }
    /** Classification only; callers must still invoke the validator, including for malformed namespace keys. */
    public static boolean isSchemaCommand(DistributedCommand command) {
        return command.mutations().size()==1 && command.mutations().getFirst().key().length==33
                && command.mutations().getFirst().key()[0]==0x53;
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] catalogueSchema,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(group); Objects.requireNonNull(column);
        if (epoch<0 || catalogueSchema.length!=32) throw new IllegalArgumentException("Invalid schema catalogue validator binding");
        byte[] expected=catalogueSchema.clone();
        return command -> {
            if (command.formatVersion()!=2 || !command.groupId().equals(group) || command.rangeEpoch()!=epoch
                    || !Arrays.equals(command.schemaDigest(),expected) || command.mutations().size()!=1 || command.predicates().size()!=1) {
                throw new IllegalArgumentException("Schema publication requires one v2 conditional content-addressed PUT");
            }
            var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst(); byte[] key=mutation.key();
            if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE
                    || !mutation.columnId().equals(column) || !predicate.columnId().equals(column) || predicate.expectedValue()!=null
                    || key.length!=33 || key[0]!=0x53 || !Arrays.equals(key,predicate.key())) {
                throw new IllegalArgumentException("Invalid schema publication namespace/CAS");
            }
            decode(mutation.value(),cluster,database,key);
        };
    }
    private static DistributedSchema decode(byte[] encoded,UUID cluster,UUID database,byte[] key) {
        if (encoded==null || encoded.length>MAX_SCHEMA_BYTES) throw new IllegalArgumentException("Schema exceeds publication byte budget");
        var schema=DistributedSchemaCodec.decode(encoded);
        if (!schema.clusterId().equals(cluster) || !schema.databaseId().equals(database)
                || !Arrays.equals(key(schema.digest()),key) || !Arrays.equals(encoded,DistributedSchemaCodec.encode(schema))) {
            throw new IllegalArgumentException("Schema publication content identity mismatch");
        }
        return schema;
    }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException {
        if (reply==null || !reply.isSuccess() || reply.getMessage()==null) throw new IOException("Schema catalogue request failed",reply==null ? null : reply.getException());
        var bytes=reply.getMessage().getContent();
        if (bytes==null || bytes.size()>maximum) throw new IOException("Schema catalogue reply exceeds byte budget");
        return bytes.toByteArray();
    }
    private static byte[] uuid(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
}
