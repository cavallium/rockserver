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
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+65+TopologyChange.MAX_HEADER_BYTES+MAX_OWNER_BYTES+RatisCatalogue.MAX_DIRECTORY_BYTES;
    public static final int MAX_MANIFEST_BYTES=156+2*MAX_BOUND_BYTES+32*RangeDirectoryCodec.MAX_COLUMNS+NativeTopologyImageOrigin.MAX_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+MAX_REQUEST_BYTES+MAX_MANIFEST_BYTES+DistributedSchemaCodec.MAX_BYTES+65536+MAX_OWNER_BYTES+NativeTopologyLearnerSchema.MAX_BYTES+NativeTopologyLearnerSchema.MAX_ROWS*82+69+NativeTopologyFreeze.MAX_BYTES;
    private RatisTopologySourceSnapshotRpc() {}
    public static boolean kind(int kind){return kind==PREPARE || kind==TRANSFER || kind==RELEASE;}
    public enum Purpose { BOOTSTRAP,RECOVERY,CURRENT_LEAF,ANCESTOR_METADATA }
    /** One canonical format. Relocation and current/DAG recovery are distinct semantic authorities. */
    public record Request(UUID transferId,TopologyChange.Header header,UUID catalogue,ReplicaDescriptor receiver,Purpose purpose,UUID sourceGroup,RangeDirectory sourceDirectory) {
        public Request(UUID transferId,TopologyChange.Header header,UUID catalogue,ReplicaDescriptor receiver,Purpose purpose){this(transferId,header,catalogue,receiver,purpose,Objects.requireNonNull(header).parents().getFirst().groupId(),null);}
        public Request {
            Objects.requireNonNull(transferId);Objects.requireNonNull(catalogue);Objects.requireNonNull(receiver);Objects.requireNonNull(purpose);Objects.requireNonNull(sourceGroup);
            if(purpose==Purpose.BOOTSTRAP || purpose==Purpose.RECOVERY) {
                if(header==null || header.kind()!=TopologyChange.Kind.RELOCATE || sourceDirectory!=null || !sourceGroup.equals(header.parents().getFirst().groupId()) || purpose==Purpose.BOOTSTRAP && receiver.role()!=ReplicaDescriptor.Role.LISTENER)throw new IllegalArgumentException("Source transfer requires exact relocation source");
                var parent=header.parents().getFirst();var target=header.targets().getFirst();
                if(!target.owner(receiver.replicaId()).equals(TopologyChange.Owner.of(receiver)))throw new IllegalArgumentException("Transfer receiver differs from exact target");
                var existing=parent.owners().stream().filter(value->value.replicaId().equals(receiver.replicaId())).findFirst().orElse(null);
                if(purpose==Purpose.BOOTSTRAP && existing!=null || purpose==Purpose.RECOVERY && existing==null)throw new IllegalArgumentException("Transfer purpose differs from receiver membership");
                if(existing!=null){var original=new ReplicaDescriptor(receiver.nodeId(),receiver.nodeIncarnation(),receiver.replicaId(),receiver.peerId(),receiver.address(),existing.role());if(!existing.equals(TopologyChange.Owner.of(original)))throw new IllegalArgumentException("Recovery changed original receiver identity");}
            } else {
                Objects.requireNonNull(sourceDirectory);var range=sourceDirectory.ranges().stream().filter(r->r.groupId().equals(sourceGroup)).findFirst().orElseThrow(()->new IllegalArgumentException("Recovery source group is outside directory"));
                if(!range.replicas().contains(receiver))throw new IllegalArgumentException("Recovery receiver is outside exact configured source membership");
                if(purpose==Purpose.CURRENT_LEAF && range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)throw new IllegalArgumentException("Current recovery requires an ACTIVE leaf");
                if(purpose==Purpose.ANCESTOR_METADATA && (header==null || !Arrays.equals(header.sourceDirectoryDigest(),sourceDirectory.payloadDigest()) || header.parents().stream().noneMatch(p->p.groupId().equals(sourceGroup))))throw new IllegalArgumentException("Metadata recovery requires exact original sealed source");
                if(header!=null && (!header.clusterId().equals(sourceDirectory.clusterId()) || !header.databaseId().equals(sourceDirectory.databaseId()) || !header.domainId().equals(sourceDirectory.domainId())))throw new IllegalArgumentException("Recovery lineage crossed namespace");
            }
            owner(receiver);
        }
        public boolean metadataOnly(){return purpose==Purpose.ANCESTOR_METADATA;}
        public boolean relocation(){return purpose==Purpose.BOOTSTRAP || purpose==Purpose.RECOVERY;}
        public UUID clusterId(){return sourceDirectory==null?header.clusterId():sourceDirectory.clusterId();}
        public UUID databaseId(){return sourceDirectory==null?header.databaseId():sourceDirectory.databaseId();}
        public UUID domainId(){return sourceDirectory==null?header.domainId():sourceDirectory.domainId();}
        public byte[] schemaDigest(){return sourceDirectory==null?header.schemaDigest():sourceDirectory.schemaDigest();}
        public RangeDescriptor range(){if(sourceDirectory==null)throw new IllegalStateException("Relocation range requires immutable definition");return sourceDirectory.ranges().stream().filter(r->r.groupId().equals(sourceGroup)).findFirst().orElseThrow();}
        public long rangeEpoch(){return sourceDirectory==null?header.parents().getFirst().rangeEpoch():range().rangeEpoch();}
        public boolean sourceOwner(ReplicaDescriptor value){return value.role()==ReplicaDescriptor.Role.VOTER && (sourceDirectory==null?header.parents().getFirst().owner(value.replicaId()).equals(TopologyChange.Owner.of(value)):range().replicas().contains(value));}
        public Request successor(UUID id){return new Request(id,header,catalogue,receiver,purpose,sourceGroup,sourceDirectory);}
    }
    public static final class Prepared {
        private final Request request;private final NativeRangeImage.Manifest image;private final DistributedSchema schema;private final ReplicaDescriptor sourceOwner;
        private final ReplicatedRangeStore.Configuration sourceCfg;private final Map<String,byte[]> schemaRows;private final NativeTopologyFreeze metadataFreeze;private final int encodedSize;
        public Prepared(Request request,NativeRangeImage.Manifest image,DistributedSchema schema,ReplicaDescriptor sourceOwner,ReplicatedRangeStore.Configuration sourceCfg,Map<String,byte[]> schemaRows,NativeTopologyFreeze metadataFreeze) {
            Objects.requireNonNull(request);Objects.requireNonNull(image);Objects.requireNonNull(schema);Objects.requireNonNull(sourceOwner);Objects.requireNonNull(sourceCfg);
            if(!image.owner().groupId().equals(request.sourceGroup()) || image.owner().epoch()!=request.rangeEpoch() || image.position().term()<1 || image.bytes()<1
                    || !image.interval().domainId().equals(request.domainId()) || !image.interval().columns().equals(schema.columnIds())
                    || !schema.clusterId().equals(request.clusterId()) || !schema.databaseId().equals(request.databaseId()) || !schema.domainId().equals(request.domainId())
                    || !Arrays.equals(image.schemaDigest(),request.schemaDigest()) || !Arrays.equals(schema.digest(),request.schemaDigest()) || !request.sourceOwner(sourceOwner)
                    || !(sourceCfg.term()==0 && sourceCfg.index()==-1) && (sourceCfg.term()<1 || sourceCfg.index()<0 || sourceCfg.index()>image.position().index() || sourceCfg.term()>image.position().term()
                            || sourceCfg.index()==image.position().index() && sourceCfg.term()!=image.position().term()))throw new IllegalArgumentException("Prepared image differs from exact source/schema/configuration");
            if(request.sourceDirectory()!=null){schema.requireMatches(request.sourceDirectory());var range=request.range();if(!Arrays.equals(range.lowerInclusive(),image.interval().lowerInclusive()) || !Arrays.equals(range.upperExclusive(),image.interval().upperExclusive()))throw new IllegalArgumentException("Recovery image changed exact source geometry");}
            requireMetadataFreeze(request,metadataFreeze,image.position(),image.interval().lowerInclusive(),image.interval().upperExclusive());
            if(request.metadataOnly() && image.topologyOrigin()!=null)throw new IllegalArgumentException("Metadata-only image acquired USER birth provenance");
            var merges=new HashSet<UUID>();for(var column:schema.columns())if(column.operator()!=null)merges.add(column.columnId());
            if(!image.deterministicMergeColumns().equals(merges))throw new IllegalArgumentException("Image merge admission differs from qualified schema");
            bound(image.interval().lowerInclusive());bound(image.interval().upperExclusive());owner(sourceOwner);
            schemaRows=NativeTopologyLearnerSchema.copy(Objects.requireNonNull(schemaRows));
            for(var key:schemaRows.keySet())if(!key.equals(HexFormat.of().formatHex(HexFormat.of().parseHex(key))))throw new IllegalArgumentException("Noncanonical schema row key");
            this.request=request;this.image=image;this.schema=schema;this.sourceOwner=sourceOwner;this.sourceCfg=sourceCfg;this.schemaRows=schemaRows;this.metadataFreeze=metadataFreeze;
            // Capture/decode construction already owns metadata admission. Cache lengths here so a
            // receiver can reserve its exact construction workspace without copying any payload.
            byte[] lower=image.interval().lowerInclusive(),upper=image.interval().upperExclusive();var origin=image.topologyOrigin();
            long manifest=156L+16L*(image.interval().columns().size()+image.deterministicMergeColumns().size())+(lower==null?0:lower.length)+(upper==null?0:upper.length)+(origin==null?0:78L+origin.seed().encodedSize()+237L+headerBytes(origin.activation().header()));
            long size=45L+encodedRequestBytes(request)+manifest+DistributedSchemaCodec.encodedSize(schema)+57L+sourceOwner.peerId().length()+sourceOwner.address().length()+sourceCfg.data().length+(metadataFreeze==null?0:104L+headerBytes(metadataFreeze.header()));
            for(var row:schemaRows.entrySet())size+=8L+row.getKey().length()/2+row.getValue().length;
            encodedSize=Math.toIntExact(size);if(encodedSize>MAX_REPLY_BYTES)throw new IllegalArgumentException("Prepared source reply exceeds byte bound");
        }
        public Request request(){return request;}public NativeRangeImage.Manifest image(){return image;}public DistributedSchema schema(){return schema;}public ReplicaDescriptor sourceOwner(){return sourceOwner;}public ReplicatedRangeStore.Configuration sourceCfg(){return sourceCfg;}public NativeTopologyFreeze metadataFreeze(){return metadataFreeze;}
        public Map<String,byte[]> schemaRows(){return NativeTopologyLearnerSchema.copy(schemaRows);}
        /** Exact PREPARE/TRANSFER reply length; constant-time and allocation-free. */
        public int encodedSize(){return encodedSize;}
        @Override public boolean equals(Object other){return other instanceof Prepared p && request.equals(p.request) && image.equals(p.image) && schema.equals(p.schema) && sourceOwner.equals(p.sourceOwner) && sourceCfg.equals(p.sourceCfg) && schemaRows.equals(p.schemaRows) && Objects.equals(metadataFreeze,p.metadataFreeze);}
        @Override public int hashCode(){int result=request.hashCode();result=31*result+image.hashCode();result=31*result+schema.hashCode();result=31*result+sourceOwner.hashCode();result=31*result+sourceCfg.hashCode();result=31*result+schemaRows.hashCode();return 31*result+Objects.hashCode(metadataFreeze);}
        @Override public String toString(){return "Prepared[request="+request+", image="+image+", schema="+schema+", sourceOwner="+sourceOwner+", sourceCfg="+sourceCfg+", schemaRows="+schemaRows+", metadataFreeze="+metadataFreeze+"]";}
    }
    static boolean metadataFreezeKey(UUID group,byte[] key){return key.length==17 && key[16]==0x60 && uuid(ByteBuffer.wrap(key)).equals(group);}
    static void requireMetadataFreeze(Request request,NativeTopologyFreeze freeze,ApplyPosition cut,byte[] lower,byte[] upper){
        if(request.metadataOnly()!=(freeze!=null))throw new IllegalArgumentException("Source metadata FREEZE must be present exactly for metadata images");
        if(freeze!=null){if(!Arrays.equals(freeze.header().encode(),request.header().encode()))throw new IllegalArgumentException("Source metadata FREEZE changed original header");freeze.requireParent(request.sourceGroup(),request.rangeEpoch(),request.domainId(),request.schemaDigest(),lower,upper);freeze.requireCut(cut);}
    }
    public record Abandoned(Request original,UUID successorId,ApplyPosition cut,ReplicaDescriptor sourceOwner,byte[] configurationDigest) {
        public Abandoned {Objects.requireNonNull(original);Objects.requireNonNull(successorId);Objects.requireNonNull(cut);Objects.requireNonNull(sourceOwner);if(successorId.equals(original.transferId()) || cut.term()<1 || configurationDigest.length!=32 || !original.sourceOwner(sourceOwner))throw new IllegalArgumentException("Invalid abandoned source receipt");configurationDigest=configurationDigest.clone();}
        @Override public byte[] configurationDigest(){return configurationDigest.clone();}
        public Request successorRequest(){return original.successor(successorId);}
    }
    public static final class AbandonedException extends IOException {
        private final Abandoned receipt;public AbandonedException(Abandoned receipt){super("Source transfer was durably abandoned; retry its acknowledged successor");this.receipt=Objects.requireNonNull(receipt);}public Abandoned receipt(){return receipt;}
    }
    static int encodedRequestBytes(Request request) {
        long header=request.header()==null?0:headerBytes(request.header());
        long directory=request.sourceDirectory()==null?0:RangeDirectoryCodec.encodedSize(request.sourceDirectory());
        return Math.toIntExact(ScopedTransactionRpc.HEADER_BYTES+61L+header+directory+57L+request.receiver().peerId().length()+request.receiver().address().length());
    }
    private static long headerBytes(TopologyChange.Header header){long size=229;for(var participant:header.parents())size+=52L+49L*participant.owners().size();for(var participant:header.targets())size+=52L+49L*participant.owners().size();return size;}
    /** The directory/header in this request is already fixed; reserve only its actual bytes. */
    static int maximumPreparedBytes(Request request){return Math.addExact(MAX_REPLY_BYTES-MAX_REQUEST_BYTES-(request.metadataOnly()?NativeTopologyImageOrigin.MAX_BYTES:NativeTopologyFreeze.MAX_BYTES),encodedRequestBytes(request));}
    static int maximumPublicationBytes(Request request){return Math.addExact(encodedRequestBytes(request),MAX_OWNER_BYTES+181+(request.metadataOnly()?NativeTopologyFreeze.MAX_BYTES:0));}
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
        var freezeBytes=blob(in,NativeTopologyFreeze.MAX_BYTES);var metadataFreeze=freezeBytes.length==0?null:NativeTopologyFreeze.decode(freezeBytes);
        if(in.remaining()<4)throw new IllegalArgumentException("Truncated schema row count");int count=in.getInt();if(count<0 || count>NativeTopologyLearnerSchema.MAX_ROWS)throw new IllegalArgumentException("Schema row count exceeds bound");
        var rows=new TreeMap<String,byte[]>();long total=0;
        for(int n=0;n<count;n++){byte[] key=blob(in,74);if(in.remaining()<4)throw new IllegalArgumentException("Truncated schema row");int size=in.getInt();if(size<0 || size>in.remaining() || (total+=key.length+size)>NativeTopologyLearnerSchema.MAX_BYTES)throw new IllegalArgumentException("Schema row payload exceeds bound");byte[] value=new byte[size];in.get(value);if(rows.put(HexFormat.of().formatHex(key),value)!=null)throw new IllegalArgumentException("Duplicate schema row");}
        if(in.hasRemaining())throw new IllegalArgumentException("Trailing source snapshot reply");var result=new Prepared(request,image,schema,source,configuration,rows,metadataFreeze);if(!Arrays.equals(reply(kind,result),bytes))throw new IllegalArgumentException("Noncanonical source snapshot reply");return result;
    }
    public static void requireRequest(Request expected,Request actual){if(!Arrays.equals(body(expected),body(actual)))throw new IllegalArgumentException("Source transfer identity/input changed");}
    static int preparedBytes(Prepared prepared){return prepared.encodedSize();}
    private static byte[] body(Request request){return bytes(out->{out.writeByte(request.purpose().ordinal());uuid(out,request.transferId());uuid(out,request.catalogue());blob(out,request.header()==null?new byte[0]:request.header().encode());uuid(out,request.sourceGroup());blob(out,request.sourceDirectory()==null?new byte[0]:RangeDirectoryCodec.encode(request.sourceDirectory()));blob(out,owner(request.receiver()));},MAX_REQUEST_BYTES);}
    private static Request readRequest(ByteBuffer in){if(in.remaining()<65)throw new IllegalArgumentException("Truncated source transfer request");int purpose=in.get()&255;if(purpose>=Purpose.values().length)throw new IllegalArgumentException("Unknown source transfer purpose");UUID transfer=uuid(in),catalogue=uuid(in);byte[] header=blob(in,TopologyChange.MAX_HEADER_BYTES);var group=uuid(in);byte[] directory=blob(in,RatisCatalogue.MAX_DIRECTORY_BYTES);var receiver=owner(blob(in,MAX_OWNER_BYTES));var result=new Request(transfer,header.length==0?null:TopologyChange.Header.decode(header),catalogue,receiver,Purpose.values()[purpose],group,directory.length==0?null:RangeDirectoryCodec.decode(directory));if(in.hasRemaining())throw new IllegalArgumentException("Trailing transfer request");return result;}
    private static byte[] preparedBody(Prepared prepared){return bytes(out->{blob(out,body(prepared.request()));blob(out,manifest(prepared.image()));blob(out,DistributedSchemaCodec.encode(prepared.schema()));blob(out,owner(prepared.sourceOwner()));out.writeLong(prepared.sourceCfg().term());out.writeLong(prepared.sourceCfg().index());blob(out,prepared.sourceCfg().data());blob(out,prepared.metadataFreeze()==null?new byte[0]:prepared.metadataFreeze().encode());var rows=prepared.schemaRows();out.writeInt(rows.size());for(var row:rows.entrySet()){blob(out,HexFormat.of().parseHex(row.getKey()));blob(out,row.getValue());}},MAX_REPLY_BYTES);}
    private static byte[] manifest(NativeRangeImage.Manifest image){return bytes(out->{uuid(out,image.owner().groupId());out.writeLong(image.owner().epoch());uuid(out,image.interval().domainId());ids(out,image.interval().columns());nullable(out,image.interval().lowerInclusive());nullable(out,image.interval().upperExclusive());out.write(image.schemaDigest());out.writeLong(image.position().term());out.writeLong(image.position().index());ids(out,image.deterministicMergeColumns());out.writeLong(image.rows());out.writeLong(image.bytes());out.write(image.sha256());blob(out,image.topologyOrigin()==null?new byte[0]:image.topologyOrigin().encode());},MAX_MANIFEST_BYTES);}
    private static NativeRangeImage.Manifest manifest(byte[] encoded){var in=ByteBuffer.wrap(encoded);if(in.remaining()<40)throw new IllegalArgumentException("Truncated image manifest");var owner=new RangeOwnership.Owner(uuid(in),in.getLong());UUID domain=uuid(in);var columns=ids(in);byte[] lower=nullable(in),upper=nullable(in);if(in.remaining()<48)throw new IllegalArgumentException("Truncated image cut");byte[] schema=new byte[32];in.get(schema);var cut=new ApplyPosition(in.getLong(),in.getLong());var merges=ids(in);if(in.remaining()<52)throw new IllegalArgumentException("Truncated image metrics");long rows=in.getLong(),size=in.getLong();byte[] hash=new byte[32];in.get(hash);byte[] origin=blob(in,NativeTopologyImageOrigin.MAX_BYTES);if(in.hasRemaining())throw new IllegalArgumentException("Trailing image manifest");return new NativeRangeImage.Manifest(owner,new RangeOwnership.Interval(domain,columns,lower,upper),schema,cut,merges,rows,size,hash,origin.length==0?null:NativeTopologyImageOrigin.decode(origin));}
    static byte[] owner(ReplicaDescriptor owner){byte[] peer=ascii(owner.peerId(),128),address=ascii(owner.address(),256);return bytes(out->{uuid(out,owner.nodeId());uuid(out,owner.nodeIncarnation());uuid(out,owner.replicaId());out.writeByte(owner.role().ordinal());blob(out,peer);blob(out,address);},MAX_OWNER_BYTES);}
    static ReplicaDescriptor owner(byte[] encoded){var in=ByteBuffer.wrap(encoded);if(in.remaining()<57)throw new IllegalArgumentException("Truncated replica");UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in);int role=in.get()&255;if(role>=ReplicaDescriptor.Role.values().length)throw new IllegalArgumentException("Invalid replica role");byte[] peer=blob(in,128),address=blob(in,256);for(byte b:peer)if(b<0)throw new IllegalArgumentException("Non-ASCII peer");for(byte b:address)if(b<0)throw new IllegalArgumentException("Non-ASCII address");if(in.hasRemaining())throw new IllegalArgumentException("Trailing replica");return new ReplicaDescriptor(node,incarnation,replica,new String(peer,StandardCharsets.US_ASCII),new String(address,StandardCharsets.US_ASCII),ReplicaDescriptor.Role.values()[role]);}
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
