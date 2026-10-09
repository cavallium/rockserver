package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Immutable artifact chunks and permanent semantic name/revision bindings. The client is borrowed. */
public final class RatisOperatorCatalogue {
    private static final byte UPLOAD=0x70,CHUNK=0x71,PROOF=0x72,PUBLISHED=0x73,ARTIFACT=0x74,NAME=0x75,BUDGET=0x76;
    @FunctionalInterface public interface AdmissionPolicy { void requireAdmitted(OperatorUpload upload); }
    /** Reads committed native catalogue values; publication verifies bytes without executing an artifact. */
    @FunctionalInterface public interface ArtifactReader { byte[] read(byte[] key); }
    public record Limits(long uploads,long bytes) {
        public Limits { if(uploads<1 || bytes<1) throw new IllegalArgumentException("Invalid operator catalogue limits"); }
    }
    public record Budget(long uploads,long bytes) {
        public Budget { if(uploads<0 || bytes<0) throw new IllegalArgumentException("Invalid operator catalogue budget"); }
        public byte[] encode() { return ByteBuffer.allocate(16).putLong(uploads).putLong(bytes).array(); }
        public static Budget decode(byte[] bytes) { if(bytes.length!=16) throw new IllegalArgumentException("Invalid budget record"); var in=ByteBuffer.wrap(bytes); return new Budget(in.getLong(),in.getLong()); }
    }
    public record NameHead(long revision,UUID uploadId,byte[] uploadDigest) {
        public NameHead { if(revision<0 || uploadDigest.length!=32) throw new IllegalArgumentException("Invalid operator name head"); Objects.requireNonNull(uploadId); uploadDigest=uploadDigest.clone(); }
        @Override public byte[] uploadDigest() { return uploadDigest.clone(); }
        public byte[] encode() { return ByteBuffer.allocate(56).putLong(revision).putLong(uploadId.getMostSignificantBits()).putLong(uploadId.getLeastSignificantBits()).put(uploadDigest).array(); }
        public static NameHead decode(byte[] bytes) { if(bytes.length!=56) throw new IllegalArgumentException("Invalid name head"); var in=ByteBuffer.wrap(bytes); long revision=in.getLong(); UUID id=new UUID(in.getLong(),in.getLong()); byte[] digest=new byte[32]; in.get(digest); return new NameHead(revision,id,digest); }
    }
    private final RaftClient client;
    private final UUID cluster,database,group,column;
    private final long epoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;
    public RatisOperatorCatalogue(RaftClient client,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,
                                  Limits limits,AdmissionPolicy policy) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database);
        this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column); this.epoch=epoch; this.schema=schema.clone();
        if(epoch<0 || schema.length!=32 || !client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Invalid operator catalogue binding");
        validator=validator(cluster,database,group,epoch,schema,column,limits,policy,key -> {
            try { return read(key,OperatorUpload.CHUNK_BYTES); } catch(IOException failure) { throw new IllegalStateException("Operator artifact read failed",failure); }
        });
    }
    public OperatorUpload readUpload(UUID uploadId) throws IOException { return upload(read(uploadKey(uploadId),OperatorUpload.MAX_BYTES)); }
    public OperatorUpload readPublished(String name,long revision) throws IOException {
        var result=upload(read(publishedKey(name,revision),OperatorUpload.MAX_BYTES));
        if(result!=null && (!result.operator().name().equals(name) || result.operator().version()!=revision)) throw new IOException("Operator name/revision differs from binding"); return result;
    }
    /** The first immutable complete artifact publication for this SHA; descriptors also have name/revision bindings. */
    public OperatorUpload readBySha(byte[] sha) throws IOException {
        var result=upload(read(artifactKey(sha),OperatorUpload.MAX_BYTES));
        if(result!=null && !Arrays.equals(result.operator().artifactSha256(),sha)) throw new IOException("Operator artifact SHA differs from binding"); return result;
    }
    public NameHead readNameHead(String name) throws IOException {
        byte[] value=read(nameKey(name),56);
        try { return value==null ? null : NameHead.decode(value); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid operator name head",invalid); }
    }
    public Budget readBudget() throws IOException {
        byte[] value=read(budgetKey(database),16);
        try { return value==null ? null : Budget.decode(value); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid operator budget",invalid); }
    }
    public byte[] readChunk(OperatorUpload upload,int index) throws IOException {
        requireIdentity(upload,cluster,database); byte[] bytes=read(chunkKey(upload.uploadId(),index),OperatorUpload.CHUNK_BYTES);
        if(bytes!=null) upload.requireChunk(index,bytes); return bytes;
    }
    private OperatorUpload upload(byte[] bytes) throws IOException {
        if(bytes==null) return null;
        try { var upload=OperatorUpload.decode(bytes); requireIdentity(upload,cluster,database); return upload; }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid operator upload",invalid); }
    }
    public DistributedCommand prepareBegin(OperationId operation,OperatorUpload upload,NameHead previousName,Budget previousBudget) {
        var nextName=new NameHead(upload.operator().version(),upload.uploadId(),upload.digest());
        var old=previousBudget==null ? new Budget(0,0) : previousBudget;
        var nextBudget=new Budget(Math.addExact(old.uploads(),1),Math.addExact(old.bytes(),upload.artifactLength()));
        return checked(operation,List.of(put(uploadKey(upload.uploadId()),upload.encode()),put(nameKey(upload.operator().name()),nextName.encode()),put(budgetKey(database),nextBudget.encode())),
                List.of(predicate(uploadKey(upload.uploadId()),null),predicate(nameKey(upload.operator().name()),previousName==null ? null : previousName.encode()),predicate(budgetKey(database),previousBudget==null ? null : previousBudget.encode())));
    }
    public DistributedCommand prepareChunk(OperationId operation,OperatorUpload upload,int index,byte[] bytes) {
        upload.requireChunk(index,bytes);
        return checked(operation,List.of(put(chunkKey(upload.uploadId(),index),bytes),put(proofKey(upload.uploadId(),index),chunkProof(upload,index))),
                List.of(predicate(uploadKey(upload.uploadId()),upload.encode()),predicate(chunkKey(upload.uploadId(),index),null),predicate(proofKey(upload.uploadId(),index),null)));
    }
    /** Existing SHA publication is a frozen witness, enabling multiple descriptors for the same admitted artifact. */
    public DistributedCommand preparePublication(OperationId operation,OperatorUpload upload,OperatorUpload previousArtifact) {
        byte[] binding=previousArtifact==null ? upload.encode() : previousArtifact.encode();
        var predicates=new ArrayList<KeyPredicate>(); predicates.add(predicate(uploadKey(upload.uploadId()),upload.encode()));
        predicates.add(predicate(publishedKey(upload.operator().name(),upload.operator().version()),null));
        predicates.add(predicate(artifactKey(upload.operator().artifactSha256()),previousArtifact==null ? null : binding));
        for(int i=0;i<upload.chunks().size();i++) predicates.add(predicate(proofKey(upload.uploadId(),i),chunkProof(upload,i)));
        return checked(operation,List.of(put(publishedKey(upload.operator().name(),upload.operator().version()),upload.encode()),put(artifactKey(upload.operator().artifactSha256()),binding)),predicates);
    }
    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command);
        try { var result=CommandResult.decode(content(RatisRoutedClient.rpc(() -> client.io().send(message(DistributedCommandCodec.encode(command)))),CommandResult.MAX_BYTES));
            if(result.valueCount()!=0 || result.status()==CommandResult.Status.RESULT_TOO_LARGE) throw new IOException("Invalid operator catalogue outcome"); return result;
        } catch(IllegalArgumentException invalid) { throw new IOException("Invalid operator catalogue result",invalid); }
    }
    private DistributedCommand checked(OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) {
        var command=new DistributedCommand(group,epoch,schema,operation,mutations,predicates); validator.accept(command); DistributedCommandCodec.encode(command); return command;
    }
    private Mutation put(byte[] key,byte[] bytes) { return new Mutation(Mutation.Kind.PUT,column,key,bytes); }
    private KeyPredicate predicate(byte[] key,byte[] bytes) { return new KeyPredicate(column,key,bytes); }
    private byte[] read(byte[] key,int maximum) throws IOException {
        try { return ScopedTransactionRpc.pointValue(content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(ScopedTransactionRpc.pointRequest(column,key)))),maximum+20),maximum); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid operator catalogue point response",invalid); }
    }
    public static boolean isCommand(DistributedCommand command) {
        if(command.mutations().isEmpty()) return false; byte[] key=command.mutations().getFirst().key();
        return key.length>0 && key.length!=16 && key[0]>=UPLOAD && key[0]<=BUDGET;
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,
                                                        Limits limits,AdmissionPolicy policy,ArtifactReader reader) {
        Objects.requireNonNull(policy); Objects.requireNonNull(reader); Objects.requireNonNull(limits); byte[] expected=schema.clone();
        return command -> {
            if(command.kind()!=DistributedCommand.Kind.DATA || command.hasBucketData() || command.acceptance()!=null || command.transactionRejection()
                    || !command.groupId().equals(group) || command.rangeEpoch()!=epoch || !Arrays.equals(command.schemaDigest(),expected)
                    || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1 || command.mutations().isEmpty()
                    || command.mutations().stream().anyMatch(m -> m.kind()!=Mutation.Kind.PUT || m.returnMode()!=Mutation.ReturnMode.NONE || !m.columnId().equals(column))
                    || command.predicates().stream().anyMatch(p -> !p.columnId().equals(column) || p.bucket())) throw new IllegalArgumentException("Invalid operator catalogue envelope");
            byte[] key=command.mutations().getFirst().key(); if(key.length==0) throw new IllegalArgumentException("Missing operator namespace");
            if(key[0]==UPLOAD) validateBegin(command,cluster,database,limits,policy);
            else if(key[0]==CHUNK) validateChunk(command,cluster,database);
            else if(key[0]==PUBLISHED) validatePublication(command,cluster,database,policy,reader);
            else throw new IllegalArgumentException("Invalid operator catalogue operation");
        };
    }
    private static void validateBegin(DistributedCommand command,UUID cluster,UUID database,Limits limits,AdmissionPolicy policy) {
        shape(command,3,3); var upload=OperatorUpload.decode(command.mutations().getFirst().value()); requireIdentity(upload,cluster,database); policy.requireAdmitted(upload);
        mutation(command,0,uploadKey(upload.uploadId()),upload.encode()); predicate(command,0,uploadKey(upload.uploadId()),null);
        var oldName=command.predicates().get(1).expectedValue(); long revision=oldName==null ? 0 : Math.addExact(NameHead.decode(oldName).revision(),1);
        if(upload.operator().version()!=revision) throw new IllegalArgumentException("Operator revision must follow permanent name allocation");
        mutation(command,1,nameKey(upload.operator().name()),new NameHead(revision,upload.uploadId(),upload.digest()).encode()); key(command,1,nameKey(upload.operator().name()));
        byte[] oldBudget=command.predicates().get(2).expectedValue(); var old=oldBudget==null ? new Budget(0,0) : Budget.decode(oldBudget);
        var next=new Budget(Math.addExact(old.uploads(),1),Math.addExact(old.bytes(),upload.artifactLength()));
        if(next.uploads()>limits.uploads() || next.bytes()>limits.bytes()) throw new IllegalArgumentException("Operator catalogue retention budget exhausted");
        mutation(command,2,budgetKey(database),next.encode()); key(command,2,budgetKey(database));
    }
    private static void validateChunk(DistributedCommand command,UUID cluster,UUID database) {
        shape(command,2,3); var upload=OperatorUpload.decode(command.predicates().getFirst().expectedValue()); requireIdentity(upload,cluster,database);
        byte[] bytes=command.mutations().getFirst().value(); int index=chunkIndex(command.mutations().getFirst().key(),upload.uploadId(),CHUNK); upload.requireChunk(index,bytes);
        predicate(command,0,uploadKey(upload.uploadId()),upload.encode()); mutation(command,0,chunkKey(upload.uploadId(),index),bytes); mutation(command,1,proofKey(upload.uploadId(),index),chunkProof(upload,index));
        predicate(command,1,chunkKey(upload.uploadId(),index),null); predicate(command,2,proofKey(upload.uploadId(),index),null);
    }
    private static void validatePublication(DistributedCommand command,UUID cluster,UUID database,AdmissionPolicy policy,ArtifactReader reader) {
        var upload=OperatorUpload.decode(command.mutations().getFirst().value()); requireIdentity(upload,cluster,database); policy.requireAdmitted(upload); shape(command,2,3+upload.chunks().size());
        mutation(command,0,publishedKey(upload.operator().name(),upload.operator().version()),upload.encode()); predicate(command,0,uploadKey(upload.uploadId()),upload.encode()); predicate(command,1,publishedKey(upload.operator().name(),upload.operator().version()),null);
        byte[] prior=command.predicates().get(2).expectedValue(),binding=prior==null ? upload.encode() : prior;
        var artifact=OperatorUpload.decode(binding); requireIdentity(artifact,cluster,database);
        if(!Arrays.equals(artifact.operator().artifactSha256(),upload.operator().artifactSha256())) throw new IllegalArgumentException("Artifact lookup changed SHA ownership");
        mutation(command,1,artifactKey(upload.operator().artifactSha256()),binding); key(command,2,artifactKey(upload.operator().artifactSha256()));
        var chunks=new ArrayList<byte[]>();
        for(int i=0;i<upload.chunks().size();i++) { predicate(command,3+i,proofKey(upload.uploadId(),i),chunkProof(upload,i)); byte[] chunk=reader.read(chunkKey(upload.uploadId(),i)); if(chunk==null) throw new IllegalStateException("Operator artifact is incomplete"); chunks.add(chunk); }
        upload.requireArtifact(chunks);
    }
    private static void shape(DistributedCommand command,int mutations,int predicates) { if(command.mutations().size()!=mutations || command.predicates().size()!=predicates) throw new IllegalArgumentException("Invalid operator catalogue command shape"); }
    private static void mutation(DistributedCommand command,int index,byte[] key,byte[] value) { var mutation=command.mutations().get(index); if(!Arrays.equals(mutation.key(),key) || !Arrays.equals(mutation.value(),value)) throw new IllegalArgumentException("Invalid immutable operator value"); }
    private static void key(DistributedCommand command,int index,byte[] key) { if(!Arrays.equals(command.predicates().get(index).key(),key)) throw new IllegalArgumentException("Invalid operator catalogue predicate key"); }
    private static void predicate(DistributedCommand command,int index,byte[] key,byte[] value) { key(command,index,key); if(!Arrays.equals(command.predicates().get(index).expectedValue(),value)) throw new IllegalArgumentException("Invalid frozen operator catalogue witness"); }
    private static void requireIdentity(OperatorUpload upload,UUID cluster,UUID database) { if(!upload.clusterId().equals(cluster) || !upload.databaseId().equals(database)) throw new IllegalArgumentException("Foreign operator upload"); }
    private static byte[] chunkProof(OperatorUpload upload,int index) { var chunk=upload.chunks().get(index); return ByteBuffer.allocate(36).putInt(chunk.length()).put(chunk.sha256()).array(); }
    public static byte[] uploadKey(UUID upload) { return uuidKey(UPLOAD,upload); }
    public static byte[] chunkKey(UUID upload,int index) { return indexed(CHUNK,upload,index); }
    public static byte[] proofKey(UUID upload,int index) { return indexed(PROOF,upload,index); }
    public static byte[] nameKey(String name) { return digestKey(NAME,OperatorUpload.sha256(nameBytes(name))); }
    public static byte[] publishedKey(String name,long revision) { if(revision<0) throw new IllegalArgumentException("Negative revision"); return ByteBuffer.allocate(41).put(PUBLISHED).put(OperatorUpload.sha256(nameBytes(name))).putLong(revision).array(); }
    public static byte[] artifactKey(byte[] sha) { return digestKey(ARTIFACT,sha); }
    public static byte[] budgetKey(UUID database) { return uuidKey(BUDGET,database); }
    private static byte[] uuidKey(byte tag,UUID uuid) { return ByteBuffer.allocate(17).put(tag).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array(); }
    private static byte[] indexed(byte tag,UUID uuid,int index) { if(index<0 || index>=8) throw new IllegalArgumentException("Invalid operator chunk index"); return ByteBuffer.allocate(21).put(tag).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).putInt(index).array(); }
    private static int chunkIndex(byte[] key,UUID upload,byte tag) { if(key.length!=21) throw new IllegalArgumentException("Invalid chunk key"); int index=ByteBuffer.wrap(key).getInt(17); if(!Arrays.equals(key,indexed(tag,upload,index))) throw new IllegalArgumentException("Foreign chunk key"); return index; }
    private static byte[] digestKey(byte tag,byte[] sha) { if(sha.length!=32) throw new IllegalArgumentException("Invalid SHA key"); return ByteBuffer.allocate(33).put(tag).put(sha).array(); }
    private static byte[] nameBytes(String name) { if(name==null || name.isEmpty() || name.length()>4096) throw new IllegalArgumentException("Invalid operator name"); byte[] bytes=name.getBytes(StandardCharsets.UTF_8); if(bytes.length>4096 || !name.equals(new String(bytes,StandardCharsets.UTF_8))) throw new IllegalArgumentException("Invalid operator name bytes"); return bytes; }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException { if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>maximum) throw new IOException("Invalid operator catalogue reply",reply==null ? null : reply.getException()); return reply.getMessage().getContent().toByteArray(); }
}
