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
        byte[] key=key(digest), query=ScopedTransactionRpc.pointRequest(column,key);
        byte[] encoded;
        try { encoded=ScopedTransactionRpc.pointValue(content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(query))),MAX_SCHEMA_BYTES+20),MAX_SCHEMA_BYTES); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid schema catalogue reply",malformed); }
        if (encoded==null) return null;
        try { return decode(encoded,cluster,database,key); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid schema catalogue content",malformed); }
    }
    /** Prepare once; uncertain outcomes must retry these exact bytes and operation identity. */
    public DistributedCommand prepare(OperationId operation,DistributedSchema schema) {
        return prepare(operation,schema,Map.of(),Map.of());
    }
    /** Existing markers are explicit frozen witnesses; no catalogue reads occur during preparation. */
    public DistributedCommand prepare(OperationId operation,DistributedSchema schema,Map<UUID,byte[]> columnWitnesses,Map<Long,byte[]> apiWitnesses) {
        Objects.requireNonNull(schema); Objects.requireNonNull(columnWitnesses); Objects.requireNonNull(apiWitnesses);
        byte[] encoded=DistributedSchemaCodec.encode(schema);
        if (encoded.length>MAX_SCHEMA_BYTES) throw new IllegalArgumentException("Schema exceeds publication command budget");
        if (!schema.columnIds().containsAll(columnWitnesses.keySet()) || !schema.columns().stream().map(DistributedSchema.Column::apiId).toList().containsAll(apiWitnesses.keySet())) {
            throw new IllegalArgumentException("Publication witnesses contain unrelated column identities");
        }
        byte[] key=key(schema.digest()); var mutations=new ArrayList<Mutation>(); var predicates=new ArrayList<KeyPredicate>();
        mutations.add(new Mutation(Mutation.Kind.PUT,column,key,encoded)); predicates.add(new KeyPredicate(column,key,null));
        for (var binding:schema.columns()) {
            byte[] uuidWitness=columnWitnesses.get(binding.columnId()), apiWitness=apiWitnesses.get(binding.apiId());
            if (!Arrays.equals(uuidWitness,apiWitness)) throw new IllegalArgumentException("Column UUID/API issuance witnesses differ");
            byte[] issued=uuidWitness==null ? RatisColumnIdentities.value(schema.domainId(),binding.columnId(),binding.apiId(),schema.digest()) : uuidWitness.clone();
            RatisColumnIdentities.requireOwned(issued,schema.domainId(),binding.columnId(),binding.apiId());
            marker(mutations,predicates,RatisColumnIdentities.columnKey(binding.columnId()),issued,uuidWitness);
            marker(mutations,predicates,RatisColumnIdentities.apiKey(binding.apiId()),issued,apiWitness);
        }
        var command=new DistributedCommand(group,epoch,catalogueSchema,operation,mutations,predicates);
        validator.accept(command); return command;
    }
    private void marker(List<Mutation> mutations,List<KeyPredicate> predicates,byte[] key,byte[] value,byte[] witness) {
        mutations.add(new Mutation(Mutation.Kind.PUT,column,key,value)); predicates.add(new KeyPredicate(column,key,witness));
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
        return !command.mutations().isEmpty() && command.mutations().getFirst().key().length==33
                && command.mutations().getFirst().key()[0]==0x53;
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] catalogueSchema,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(group); Objects.requireNonNull(column);
        if (epoch<0 || catalogueSchema.length!=32) throw new IllegalArgumentException("Invalid schema catalogue validator binding");
        byte[] expected=catalogueSchema.clone();
        return command -> {
            if (command.kind()!=DistributedCommand.Kind.DATA || command.hasBucketData() || !command.groupId().equals(group) || command.rangeEpoch()!=epoch
                    || !Arrays.equals(command.schemaDigest(),expected) || command.mutations().isEmpty()
                    || command.mutations().size()!=command.predicates().size()) {
                throw new IllegalArgumentException("Schema publication requires conditional content and issuance PUTs");
            }
            var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst(); byte[] key=mutation.key();
            requirePut(mutation,predicate,column,key);
            if (predicate.expectedValue()!=null || key.length!=33 || key[0]!=0x53) throw new IllegalArgumentException("Invalid schema content namespace/CAS");
            var schema=decode(mutation.value(),cluster,database,key);
            if (command.mutations().size()!=1+2*schema.columns().size()) throw new IllegalArgumentException("Missing or extra column issuance markers");
            int index=1;
            for (var binding:schema.columns()) {
                byte[] uuidMarker=requireMarker(command,index++,column,RatisColumnIdentities.columnKey(binding.columnId()),schema,binding);
                byte[] apiMarker=requireMarker(command,index++,column,RatisColumnIdentities.apiKey(binding.apiId()),schema,binding);
                if (!Arrays.equals(command.predicates().get(index-2).expectedValue(),command.predicates().get(index-1).expectedValue()) || !Arrays.equals(uuidMarker,apiMarker)) throw new IllegalArgumentException("Column UUID/API issuance markers differ");
            }
        };
    }
    private static void requirePut(Mutation mutation,KeyPredicate predicate,UUID column,byte[] key) {
        if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || mutation.bucket() || predicate.bucket()
                || !mutation.columnId().equals(column) || !predicate.columnId().equals(column)
                || !Arrays.equals(key,mutation.key()) || !Arrays.equals(key,predicate.key())) throw new IllegalArgumentException("Invalid schema publication namespace/CAS");
    }
    private static byte[] requireMarker(DistributedCommand command,int index,UUID column,byte[] key,DistributedSchema schema,DistributedSchema.Column binding) {
        var mutation=command.mutations().get(index); var predicate=command.predicates().get(index); requirePut(mutation,predicate,column,key);
        byte[] witness=predicate.expectedValue(),value=mutation.value();
        byte[] issuedDigest=RatisColumnIdentities.requireOwned(value,schema.domainId(),binding.columnId(),binding.apiId());
        if (witness==null ? !Arrays.equals(issuedDigest,schema.digest()) : !Arrays.equals(value,witness)) throw new IllegalArgumentException("Issuance marker rewrites its first issued schema");
        return value;
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
