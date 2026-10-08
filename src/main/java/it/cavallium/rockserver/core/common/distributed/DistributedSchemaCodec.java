package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.security.*;
import java.util.*;

/** Canonical bounded content encoding, independent of routing generations and placements. */
public final class DistributedSchemaCodec {
    public static final int MAX_BYTES=2*1024*1024, MAX_KEY_COMPONENTS=256, MAX_NAME_BYTES=4096;
    private static final int MAGIC=0x44534348, VERSION=1, HEADER_BYTES=76, MIN_COLUMN_BYTES=39;
    static final Comparator<UUID> UUID_ORDER=(a,b) -> {
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    private DistributedSchemaCodec() {}
    public static byte[] encode(DistributedSchema schema) {
        long size=encodedSize(schema);
        if (size>MAX_BYTES) throw new IllegalArgumentException("Distributed schema exceeds byte budget");
        var output=ByteBuffer.allocate((int)size).putInt(MAGIC).putInt(VERSION).putInt(DistributedSchema.PHYSICAL_KEY_CODEC_VERSION);
        uuid(output,schema.clusterId()); uuid(output,schema.databaseId()); uuid(output,schema.domainId());
        output.putLong(schema.schemaEpoch()).putInt(schema.routingPrefixBytes()).putInt(schema.columns().size());
        for (var column:schema.columns()) {
            uuid(output,column.columnId()); output.putLong(column.apiId()); text(output,column.name()); var shape=column.schema();
            output.putInt(shape.keysCount()); for (int width:shape.keys()) output.putInt(width);
            output.putInt(shape.variableLengthKeysCount()); for (var hash:shape.variableTailKeys()) output.put((byte)wire(hash));
            output.put((byte)(shape.hasValue() ? 1 : 0)).put((byte)(column.operator()==null ? 0 : 1));
            var operator=column.operator();
            if (operator!=null) {
                text(output,operator.name()); output.putLong(operator.version()); text(output,operator.className());
                output.put(operator.artifactSha256()).putInt(operator.valueCodecVersion()).putInt(operator.operandCodecVersion());
            }
        }
        return output.array();
    }
    public static DistributedSchema decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<HEADER_BYTES+MIN_COLUMN_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid distributed schema size");
        var input=ByteBuffer.wrap(bytes);
        if (input.getInt()!=MAGIC || input.getInt()!=VERSION || input.getInt()!=DistributedSchema.PHYSICAL_KEY_CODEC_VERSION) {
            throw new IllegalArgumentException("Unsupported distributed schema/physical key codec");
        }
        UUID cluster=uuid(input), database=uuid(input), domain=uuid(input); long epoch=input.getLong();
        int prefix=input.getInt(), count=input.getInt();
        if (epoch<0 || prefix<0 || prefix>RangeDirectoryCodec.MAX_ENCODED_BYTES || count<1 || count>RangeDirectoryCodec.MAX_COLUMNS || (long)count*MIN_COLUMN_BYTES>input.remaining()) {
            throw new IllegalArgumentException("Invalid distributed schema header bounds");
        }
        var columns=new ArrayList<DistributedSchema.Column>(count); var ids=new HashSet<Long>(); var names=new HashSet<String>(); UUID previous=null;
        for (int i=0;i<count;i++) {
            require(input,24); UUID id=uuid(input); long apiId=input.getLong();
            if (apiId<=0 || !ids.add(apiId) || (previous!=null && UUID_ORDER.compare(previous,id)>=0)) {
                throw new IllegalArgumentException("Noncanonical distributed column UUID/API ID");
            }
            previous=id; String name=text(input);
            if (!names.add(name)) throw new IllegalArgumentException("Duplicate distributed column name");
            require(input,4); int components=input.getInt();
            if (components<0 || components>MAX_KEY_COMPONENTS || (long)components*4>input.remaining()-6) {
                throw new IllegalArgumentException("Invalid physical key component count");
            }
            var keys=new IntArrayList(components); long keyBytes=0;
            for (int k=0;k<components;k++) {
                int width=input.getInt(); keyBytes+=width;
                if (width<=0 || keyBytes>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid physical key widths/budget");
                keys.add(width);
            }
            require(input,4); int tails=input.getInt();
            if (tails<0 || tails>components || tails>input.remaining()-2) throw new IllegalArgumentException("Invalid variable-tail key count");
            var hashes=new ObjectArrayList<ColumnHashType>(tails);
            for (int k=0;k<tails;k++) {
                var hash=hash(input.get() & 255);
                if (keys.getInt(components-tails+k)!=hash.bytesSize()) throw new IllegalArgumentException("Physical key hash width differs from codec");
                hashes.add(hash);
            }
            boolean hasValue=flag(input), hasOperator=flag(input); DistributedSchema.Operator operator=null;
            if (hasOperator) {
                String operatorName=text(input); require(input,8); long version=input.getLong(); String className=text(input);
                require(input,40); byte[] artifact=new byte[32]; input.get(artifact);
                operator=new DistributedSchema.Operator(operatorName,version,className,artifact,input.getInt(),input.getInt());
            }
            var shape=new ColumnSchema(keys,hashes,hasValue,operator==null ? null : operator.name(),
                    operator==null ? null : operator.version(),operator==null ? null : operator.className());
            columns.add(new DistributedSchema.Column(apiId,id,name,shape,operator));
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing distributed schema bytes");
        return new DistributedSchema(cluster,database,domain,epoch,prefix,columns);
    }
    public static byte[] digest(DistributedSchema schema) {
        try { return MessageDigest.getInstance("SHA-256").digest(encode(schema)); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    static long encodedSize(DistributedSchema schema) {
        long size=HEADER_BYTES;
        for (var column:schema.columns()) {
            var shape=column.schema(); size+=38L+text(column.name()).length+4L*shape.keysCount()+shape.variableLengthKeysCount();
            var operator=column.operator();
            if (operator!=null) size+=56L+text(operator.name()).length+text(operator.className()).length;
        }
        return size;
    }
    static byte[] text(String value) {
        if (value==null || value.isEmpty() || value.length()>MAX_NAME_BYTES) throw new IllegalArgumentException("Invalid distributed schema name length");
        try {
            var encoded=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
            if (encoded.remaining()>MAX_NAME_BYTES) throw new IllegalArgumentException("Distributed schema name exceeds byte budget");
            byte[] bytes=new byte[encoded.remaining()]; encoded.get(bytes); return bytes;
        } catch (CharacterCodingException malformed) { throw new IllegalArgumentException("Invalid distributed schema UTF-8 name",malformed); }
    }
    private static String text(ByteBuffer input) {
        require(input,4); int length=input.getInt();
        if (length<1 || length>MAX_NAME_BYTES || length>input.remaining()) throw new IllegalArgumentException("Invalid distributed schema name byte count");
        var bytes=input.slice(); bytes.limit(length);
        try {
            String value=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(bytes).toString();
            input.position(input.position()+length); return value;
        } catch (CharacterCodingException malformed) { throw new IllegalArgumentException("Invalid distributed schema UTF-8 name",malformed); }
    }
    private static int wire(ColumnHashType hash) {
        return switch(hash) { case XXHASH32 -> 1; case XXHASH8 -> 2; case ALLSAME8 -> 3; case FIXEDINTEGER32 -> 4; };
    }
    private static ColumnHashType hash(int wire) {
        return switch(wire) { case 1 -> ColumnHashType.XXHASH32; case 2 -> ColumnHashType.XXHASH8; case 3 -> ColumnHashType.ALLSAME8;
            case 4 -> ColumnHashType.FIXEDINTEGER32; default -> throw new IllegalArgumentException("Unknown physical key hash wire ID"); };
    }
    private static boolean flag(ByteBuffer input) {
        require(input,1); int flag=input.get() & 255;
        if (flag>1) throw new IllegalArgumentException("Invalid distributed schema flag"); return flag==1;
    }
    private static void require(ByteBuffer input,int bytes) { if (input.remaining()<bytes) throw new IllegalArgumentException("Truncated distributed schema"); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static void text(ByteBuffer output,String value) { byte[] bytes=text(value); output.putInt(bytes.length).put(bytes); }
}
