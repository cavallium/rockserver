package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** One current bounded source-image protocol. Host paths and native handles never cross this boundary. */
public final class RatisTopologySourceSnapshotRpc {
    public static final int PREPARE=105,TRANSFER=106,RELEASE=107,MAX_BOUND_BYTES=8192,MAX_OWNER_BYTES=441;
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+41+TopologyChange.MAX_HEADER_BYTES+MAX_OWNER_BYTES;
    public static final int MAX_MANIFEST_BYTES=156+2*MAX_BOUND_BYTES+32*RangeDirectoryCodec.MAX_COLUMNS+NativeTopologyImageOrigin.MAX_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+MAX_REQUEST_BYTES+MAX_MANIFEST_BYTES+DistributedSchemaCodec.MAX_BYTES+65536+MAX_OWNER_BYTES+NativeTopologyLearnerSchema.MAX_BYTES+NativeTopologyLearnerSchema.MAX_ROWS*82+65;
    private RatisTopologySourceSnapshotRpc() {}
    public static boolean kind(int kind){return kind==PREPARE || kind==TRANSFER || kind==RELEASE;}
    public enum Purpose { BOOTSTRAP,RECOVERY }
    public record Request(UUID transferId,TopologyChange.Header header,UUID catalogue,ReplicaDescriptor receiver,Purpose purpose) {
        public Request {
            Objects.requireNonNull(transferId);Objects.requireNonNull(header);Objects.requireNonNull(catalogue);Objects.requireNonNull(receiver);Objects.requireNonNull(purpose);
            if(header.kind()!=TopologyChange.Kind.RELOCATE || purpose==Purpose.BOOTSTRAP && receiver.role()!=ReplicaDescriptor.Role.LISTENER)throw new IllegalArgumentException("Source transfer requires a joining relocation listener");
            var parent=header.parents().getFirst();var target=header.targets().getFirst();
            if(!target.owner(receiver.replicaId()).equals(TopologyChange.Owner.of(receiver)))throw new IllegalArgumentException("Transfer receiver differs from exact target");
            var existing=parent.owners().stream().filter(owner->owner.replicaId().equals(receiver.replicaId())).findFirst().orElse(null);
            if(purpose==Purpose.BOOTSTRAP && existing!=null || purpose==Purpose.RECOVERY && existing==null)throw new IllegalArgumentException("Transfer purpose differs from receiver membership");
            if(existing!=null){var original=new ReplicaDescriptor(receiver.nodeId(),receiver.nodeIncarnation(),receiver.replicaId(),receiver.peerId(),receiver.address(),existing.role());if(!existing.equals(TopologyChange.Owner.of(original)))throw new IllegalArgumentException("Recovery changed original receiver identity");}
            owner(receiver);
        }
    }
    public record Prepared(Request request,NativeRangeImage.Manifest image,DistributedSchema schema,ReplicaDescriptor sourceOwner,
                           ReplicatedRangeStore.Configuration sourceCfg,Map<String,byte[]> schemaRows) {
        public Prepared {
            Objects.requireNonNull(request);Objects.requireNonNull(image);Objects.requireNonNull(schema);Objects.requireNonNull(sourceOwner);Objects.requireNonNull(sourceCfg);
            var header=request.header();var parent=header.parents().getFirst();
            if(!image.owner().groupId().equals(parent.groupId()) || image.owner().epoch()!=parent.rangeEpoch() || image.position().term()<1 || image.bytes()<1
                    || !image.interval().domainId().equals(header.domainId()) || !image.interval().columns().equals(schema.columnIds())
                    || !schema.clusterId().equals(header.clusterId()) || !schema.databaseId().equals(header.databaseId()) || !schema.domainId().equals(header.domainId())
                    || !Arrays.equals(image.schemaDigest(),header.schemaDigest()) || !Arrays.equals(schema.digest(),header.schemaDigest())
                    || !parent.owner(sourceOwner.replicaId()).equals(TopologyChange.Owner.of(sourceOwner)) || sourceOwner.role()!=ReplicaDescriptor.Role.VOTER
                    || !(sourceCfg.term()==0 && sourceCfg.index()==-1) && (sourceCfg.term()<1 || sourceCfg.index()<0 || sourceCfg.index()>image.position().index() || sourceCfg.term()>image.position().term()
                            || sourceCfg.index()==image.position().index() && sourceCfg.term()!=image.position().term()))throw new IllegalArgumentException("Prepared image differs from source/schema/configuration");
            var merges=new HashSet<UUID>();for(var column:schema.columns())if(column.operator()!=null)merges.add(column.columnId());
            if(!image.deterministicMergeColumns().equals(merges))throw new IllegalArgumentException("Image merge admission differs from qualified schema");
            bound(image.interval().lowerInclusive());bound(image.interval().upperExclusive());owner(sourceOwner);
            schemaRows=NativeTopologyLearnerSchema.copy(Objects.requireNonNull(schemaRows));
            for(var key:schemaRows.keySet())if(!key.equals(HexFormat.of().formatHex(HexFormat.of().parseHex(key))))throw new IllegalArgumentException("Noncanonical schema row key");
        }
        @Override public Map<String,byte[]> schemaRows(){return NativeTopologyLearnerSchema.copy(schemaRows);}
    }
    public record Abandoned(Request original,UUID successorId,ApplyPosition cut,ReplicaDescriptor sourceOwner,byte[] configurationDigest) {
        public Abandoned {Objects.requireNonNull(original);Objects.requireNonNull(successorId);Objects.requireNonNull(cut);Objects.requireNonNull(sourceOwner);if(successorId.equals(original.transferId()) || cut.term()<1 || configurationDigest.length!=32 || !original.header().parents().getFirst().owner(sourceOwner.replicaId()).equals(TopologyChange.Owner.of(sourceOwner)))throw new IllegalArgumentException("Invalid abandoned source receipt");configurationDigest=configurationDigest.clone();}
        @Override public byte[] configurationDigest(){return configurationDigest.clone();}
        public Request successorRequest(){return new Request(successorId,original.header(),original.catalogue(),original.receiver(),original.purpose());}
    }
    public static final class AbandonedException extends IOException {
        private final Abandoned receipt;public AbandonedException(Abandoned receipt){super("Source transfer was durably abandoned; retry its acknowledged successor");this.receipt=Objects.requireNonNull(receipt);}public Abandoned receipt(){return receipt;}
    }
    public static byte[] reply(int kind,Abandoned abandoned){if(kind!=PREPARE && kind!=TRANSFER)throw new IllegalArgumentException("Expected source publication reply");return ScopedTransactionRpc.frame(kind,bytes(out->{out.writeByte(2);blob(out,body(abandoned.original()));uuid(out,abandoned.successorId());out.writeLong(abandoned.cut().term());out.writeLong(abandoned.cut().index());blob(out,owner(abandoned.sourceOwner()));out.write(abandoned.configurationDigest());},MAX_REPLY_BYTES));}
    public static byte[] request(int kind,Request request){if(!kind(kind))throw new IllegalArgumentException("Unknown source snapshot action");return ScopedTransactionRpc.frame(kind,body(request));}
    public static Request request(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);if(!kind(packet.kind()))throw new IllegalArgumentException("Unknown source snapshot action");var request=readRequest(ByteBuffer.wrap(packet.body()));if(!Arrays.equals(body(request),packet.body()))throw new IllegalArgumentException("Noncanonical transfer request");return request;}
    public static byte[] reply(int kind,Prepared prepared){if(kind!=PREPARE && kind!=TRANSFER)throw new IllegalArgumentException("Expected image reply");byte[] body=preparedBody(prepared);return ScopedTransactionRpc.frame(kind,ByteBuffer.allocate(1+body.length).put((byte)1).put(body).array());}
    public static Prepared reply(byte[] bytes,int kind,Request expected)throws AbandonedException{
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=kind || kind!=PREPARE && kind!=TRANSFER)throw new IllegalArgumentException("Wrong source image reply kind");
        var in=ByteBuffer.wrap(packet.body());if(!in.hasRemaining())throw new IllegalArgumentException("Missing source publication kind");int publication=in.get()&255;
        if(publication==2){var original=readRequest(ByteBuffer.wrap(blob(in,MAX_REQUEST_BYTES)));requireRequest(expected,original);UUID successor=uuid(in);var cut=new ApplyPosition(in.getLong(),in.getLong());var source=owner(blob(in,MAX_OWNER_BYTES));byte[] cfg=new byte[32];in.get(cfg);if(in.hasRemaining())throw new IllegalArgumentException("Trailing abandoned source reply");var receipt=new Abandoned(original,successor,cut,source,cfg);if(!Arrays.equals(bytes,reply(kind,receipt)))throw new IllegalArgumentException("Noncanonical abandoned source reply");throw new AbandonedException(receipt);}
        if(publication!=1)throw new IllegalArgumentException("Unknown source publication kind");var request=readRequest(ByteBuffer.wrap(blob(in,MAX_REQUEST_BYTES)));requireRequest(expected,request);
        var image=manifest(blob(in,MAX_MANIFEST_BYTES));var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES));var source=owner(blob(in,MAX_OWNER_BYTES));
        if(in.remaining()<20)throw new IllegalArgumentException("Truncated source configuration");var configuration=new ReplicatedRangeStore.Configuration(in.getLong(),in.getLong(),blob(in,65536));
        if(in.remaining()<4)throw new IllegalArgumentException("Truncated schema row count");int count=in.getInt();if(count<0 || count>NativeTopologyLearnerSchema.MAX_ROWS)throw new IllegalArgumentException("Schema row count exceeds bound");
        var rows=new TreeMap<String,byte[]>();long total=0;
        for(int n=0;n<count;n++){byte[] key=blob(in,74);if(in.remaining()<4)throw new IllegalArgumentException("Truncated schema row");int size=in.getInt();if(size<0 || size>in.remaining() || (total+=key.length+size)>NativeTopologyLearnerSchema.MAX_BYTES)throw new IllegalArgumentException("Schema row payload exceeds bound");byte[] value=new byte[size];in.get(value);if(rows.put(HexFormat.of().formatHex(key),value)!=null)throw new IllegalArgumentException("Duplicate schema row");}
        if(in.hasRemaining())throw new IllegalArgumentException("Trailing source snapshot reply");var result=new Prepared(request,image,schema,source,configuration,rows);if(!Arrays.equals(reply(kind,result),bytes))throw new IllegalArgumentException("Noncanonical source snapshot reply");return result;
    }
    public static void requireRequest(Request expected,Request actual){if(!Arrays.equals(body(expected),body(actual)))throw new IllegalArgumentException("Source transfer identity/input changed");}
    static int preparedBytes(Prepared prepared){return preparedBody(prepared).length+1+ScopedTransactionRpc.HEADER_BYTES;}
    private static byte[] body(Request request){return bytes(out->{out.writeByte(request.purpose().ordinal());uuid(out,request.transferId());uuid(out,request.catalogue());blob(out,request.header().encode());blob(out,owner(request.receiver()));},MAX_REQUEST_BYTES);}
    private static Request readRequest(ByteBuffer in){if(in.remaining()<41)throw new IllegalArgumentException("Truncated source transfer request");int purpose=in.get()&255;if(purpose>=Purpose.values().length)throw new IllegalArgumentException("Unknown source transfer purpose");UUID transfer=uuid(in),catalogue=uuid(in);var result=new Request(transfer,TopologyChange.Header.decode(blob(in,TopologyChange.MAX_HEADER_BYTES)),catalogue,owner(blob(in,MAX_OWNER_BYTES)),Purpose.values()[purpose]);if(in.hasRemaining())throw new IllegalArgumentException("Trailing transfer request");return result;}
    private static byte[] preparedBody(Prepared prepared){return bytes(out->{blob(out,body(prepared.request()));blob(out,manifest(prepared.image()));blob(out,DistributedSchemaCodec.encode(prepared.schema()));blob(out,owner(prepared.sourceOwner()));out.writeLong(prepared.sourceCfg().term());out.writeLong(prepared.sourceCfg().index());blob(out,prepared.sourceCfg().data());var rows=prepared.schemaRows();out.writeInt(rows.size());for(var row:rows.entrySet()){blob(out,HexFormat.of().parseHex(row.getKey()));blob(out,row.getValue());}},MAX_REPLY_BYTES);}
    private static byte[] manifest(NativeRangeImage.Manifest image){return bytes(out->{uuid(out,image.owner().groupId());out.writeLong(image.owner().epoch());uuid(out,image.interval().domainId());ids(out,image.interval().columns());nullable(out,image.interval().lowerInclusive());nullable(out,image.interval().upperExclusive());out.write(image.schemaDigest());out.writeLong(image.position().term());out.writeLong(image.position().index());ids(out,image.deterministicMergeColumns());out.writeLong(image.rows());out.writeLong(image.bytes());out.write(image.sha256());blob(out,image.topologyOrigin()==null?new byte[0]:image.topologyOrigin().encode());},MAX_MANIFEST_BYTES);}
    private static NativeRangeImage.Manifest manifest(byte[] encoded){var in=ByteBuffer.wrap(encoded);if(in.remaining()<40)throw new IllegalArgumentException("Truncated image manifest");var owner=new RangeOwnership.Owner(uuid(in),in.getLong());UUID domain=uuid(in);var columns=ids(in);byte[] lower=nullable(in),upper=nullable(in);if(in.remaining()<48)throw new IllegalArgumentException("Truncated image cut");byte[] schema=new byte[32];in.get(schema);var cut=new ApplyPosition(in.getLong(),in.getLong());var merges=ids(in);if(in.remaining()<52)throw new IllegalArgumentException("Truncated image metrics");long rows=in.getLong(),size=in.getLong();byte[] hash=new byte[32];in.get(hash);byte[] origin=blob(in,NativeTopologyImageOrigin.MAX_BYTES);if(in.hasRemaining())throw new IllegalArgumentException("Trailing image manifest");return new NativeRangeImage.Manifest(owner,new RangeOwnership.Interval(domain,columns,lower,upper),schema,cut,merges,rows,size,hash,origin.length==0?null:NativeTopologyImageOrigin.decode(origin));}
    private static byte[] owner(ReplicaDescriptor owner){byte[] peer=ascii(owner.peerId(),128),address=ascii(owner.address(),256);return bytes(out->{uuid(out,owner.nodeId());uuid(out,owner.nodeIncarnation());uuid(out,owner.replicaId());out.writeByte(owner.role().ordinal());blob(out,peer);blob(out,address);},MAX_OWNER_BYTES);}
    private static ReplicaDescriptor owner(byte[] encoded){var in=ByteBuffer.wrap(encoded);if(in.remaining()<57)throw new IllegalArgumentException("Truncated replica");UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in);int role=in.get()&255;if(role>=ReplicaDescriptor.Role.values().length)throw new IllegalArgumentException("Invalid replica role");byte[] peer=blob(in,128),address=blob(in,256);for(byte b:peer)if(b<0)throw new IllegalArgumentException("Non-ASCII peer");for(byte b:address)if(b<0)throw new IllegalArgumentException("Non-ASCII address");if(in.hasRemaining())throw new IllegalArgumentException("Trailing replica");return new ReplicaDescriptor(node,incarnation,replica,new String(peer,StandardCharsets.US_ASCII),new String(address,StandardCharsets.US_ASCII),ReplicaDescriptor.Role.values()[role]);}
    private static byte[] ascii(String value,int limit){if(value.length()>limit || !StandardCharsets.US_ASCII.newEncoder().canEncode(value))throw new IllegalArgumentException("Replica string exceeds ASCII bound");return value.getBytes(StandardCharsets.US_ASCII);}
    private static void ids(DataOutputStream out,Set<UUID> values)throws IOException{if(values.size()>RangeDirectoryCodec.MAX_COLUMNS)throw new IllegalArgumentException("Column count exceeds bound");out.writeInt(values.size());for(var id:values.stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList())uuid(out,id);}
    private static Set<UUID> ids(ByteBuffer in){if(in.remaining()<4)throw new IllegalArgumentException("Truncated columns");int count=in.getInt();if(count<0 || count>RangeDirectoryCodec.MAX_COLUMNS || 16L*count>in.remaining())throw new IllegalArgumentException("Invalid column count");var values=new LinkedHashSet<UUID>();UUID last=null;for(int n=0;n<count;n++){UUID id=uuid(in);if(last!=null && RangeDirectoryCodec.UUID_ORDER.compare(last,id)>=0)throw new IllegalArgumentException("Unordered columns");values.add(id);last=id;}return values;}
    private static void bound(byte[] value){if(value!=null && value.length>MAX_BOUND_BYTES)throw new IllegalArgumentException("Image bound exceeds limit");}
    private static void nullable(DataOutputStream out,byte[] value)throws IOException{bound(value);if(value==null)out.writeInt(-1);else blob(out,value);}
    private static byte[] nullable(ByteBuffer in){if(in.remaining()<4)throw new IllegalArgumentException("Truncated bound");int length=in.getInt();if(length==-1)return null;if(length<0 || length>MAX_BOUND_BYTES || length>in.remaining())throw new IllegalArgumentException("Invalid bound");byte[] bytes=new byte[length];in.get(bytes);return bytes;}
    private static void blob(DataOutputStream out,byte[] value)throws IOException{out.writeInt(value.length);out.write(value);}
    private static byte[] blob(ByteBuffer in,int limit){if(in.remaining()<4)throw new IllegalArgumentException("Truncated source image field");int size=in.getInt();if(size<0 || size>limit || size>in.remaining())throw new IllegalArgumentException("Invalid source image field length");byte[] bytes=new byte[size];in.get(bytes);return bytes;}
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}
    private static UUID uuid(ByteBuffer in){if(in.remaining()<16)throw new IllegalArgumentException("Truncated UUID");return new UUID(in.getLong(),in.getLong());}
    @FunctionalInterface private interface Writer{void write(DataOutputStream out)throws IOException;}
    private static byte[] bytes(Writer writer,int maximum){try{var buffer=new ByteArrayOutputStream();writer.write(new DataOutputStream(buffer));if(buffer.size()>maximum)throw new IllegalArgumentException("Source metadata exceeds frame bound");return buffer.toByteArray();}catch(IOException impossible){throw new UncheckedIOException(impossible);}}
}
