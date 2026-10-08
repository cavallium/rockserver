package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.util.*;

/** Immutable logical/API bindings and the existing physical key/operator content identity. */
public final class DistributedSchema {
    public static final int PHYSICAL_KEY_CODEC_VERSION=1;
    public record Operator(String name,long version,String className,byte[] artifactSha256,int valueCodecVersion,int operandCodecVersion) {
        public Operator {
            DistributedSchemaCodec.text(name); DistributedSchemaCodec.text(className);
            if (version<0 || artifactSha256.length!=32 || valueCodecVersion<1 || operandCodecVersion<1) {
                throw new IllegalArgumentException("Invalid distributed operator identity");
            }
            artifactSha256=artifactSha256.clone();
        }
        @Override public byte[] artifactSha256() { return artifactSha256.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof Operator operator && name.equals(operator.name) && version==operator.version && className.equals(operator.className)
                    && Arrays.equals(artifactSha256,operator.artifactSha256) && valueCodecVersion==operator.valueCodecVersion && operandCodecVersion==operator.operandCodecVersion;
        }
        @Override public int hashCode() { return Objects.hash(name,version,className,Arrays.hashCode(artifactSha256),valueCodecVersion,operandCodecVersion); }
    }
    public record Column(long apiId,UUID columnId,String name,ColumnSchema schema,Operator operator) {
        public Column {
            Objects.requireNonNull(columnId); DistributedSchemaCodec.text(name);
            if (apiId<=0) throw new IllegalArgumentException("Distributed API column ID must be positive");
            schema=copy(schema);
            if (operator==null ? schema.mergeOperatorName()!=null || schema.mergeOperatorVersion()!=null || schema.mergeOperatorClass()!=null
                    : !operator.name().equals(schema.mergeOperatorName()) || !Long.valueOf(operator.version()).equals(schema.mergeOperatorVersion())
                            || !operator.className().equals(schema.mergeOperatorClass())) {
                throw new IllegalArgumentException("Column merge fields differ from operator identity");
            }
        }
        @Override public ColumnSchema schema() { return copy(schema); }
    }
    private final UUID clusterId,databaseId,domainId;
    private final long schemaEpoch;
    private final int routingPrefixBytes;
    private final List<Column> columns;
    private final Map<UUID,Column> byUuid;
    private final Map<Long,Column> byApiId;
    private final Map<String,Column> byName;
    private final byte[] digest;

    public DistributedSchema(UUID clusterId,UUID databaseId,UUID domainId,long schemaEpoch,int routingPrefixBytes,List<Column> columns) {
        this.clusterId=Objects.requireNonNull(clusterId); this.databaseId=Objects.requireNonNull(databaseId); this.domainId=Objects.requireNonNull(domainId);
        if (schemaEpoch<0 || routingPrefixBytes<0 || routingPrefixBytes>RangeDirectoryCodec.MAX_ENCODED_BYTES || columns.isEmpty() || columns.size()>RangeDirectoryCodec.MAX_COLUMNS) {
            throw new IllegalArgumentException("Invalid distributed schema bounds");
        }
        this.schemaEpoch=schemaEpoch; this.routingPrefixBytes=routingPrefixBytes;
        this.columns=columns.stream().sorted(Comparator.comparing(Column::columnId,DistributedSchemaCodec.UUID_ORDER)).toList();
        var uuids=new HashMap<UUID,Column>(); var ids=new HashMap<Long,Column>(); var names=new HashMap<String,Column>();
        List<Integer> prefix=null;
        for (var column:this.columns) {
            if (uuids.put(column.columnId(),column)!=null || ids.put(column.apiId(),column)!=null || names.put(column.name(),column)!=null) {
                throw new IllegalArgumentException("Duplicate distributed column UUID/API ID/name");
            }
            var widths=new ArrayList<Integer>(); long bytes=0; var schema=column.schema;
            for (int i=0;i<schema.fixedLengthKeysCount() && bytes<routingPrefixBytes;i++) { int width=schema.key(i); widths.add(width); bytes+=width; }
            if (bytes!=routingPrefixBytes || (prefix!=null && !prefix.equals(widths))) {
                throw new IllegalArgumentException("Routing prefix must end at identical fixed component boundaries");
            }
            prefix=widths;
        }
        byUuid=Map.copyOf(uuids); byApiId=Map.copyOf(ids); byName=Map.copyOf(names);
        if (DistributedSchemaCodec.encodedSize(this)>DistributedSchemaCodec.MAX_BYTES) throw new IllegalArgumentException("Distributed schema exceeds byte budget");
        digest=DistributedSchemaCodec.digest(this);
    }
    public UUID clusterId() { return clusterId; }
    public UUID databaseId() { return databaseId; }
    public UUID domainId() { return domainId; }
    public long schemaEpoch() { return schemaEpoch; }
    public int routingPrefixBytes() { return routingPrefixBytes; }
    public int physicalKeyCodecVersion() { return PHYSICAL_KEY_CODEC_VERSION; }
    public List<Column> columns() { return columns; }
    public Set<UUID> columnIds() { return byUuid.keySet(); }
    public Column column(UUID id) { return require(byUuid.get(id)); }
    public Column column(long apiId) { return require(byApiId.get(apiId)); }
    public Column column(String name) { return require(byName.get(name)); }
    public byte[] digest() { return digest.clone(); }
    private static Column require(Column column) { if (column==null) throw new IllegalArgumentException("Unknown distributed column"); return column; }
    public void requireMatches(RangeDirectory directory) {
        if (!clusterId.equals(directory.clusterId()) || !databaseId.equals(directory.databaseId()) || !domainId.equals(directory.domainId())
                || schemaEpoch!=directory.schemaEpoch() || !Arrays.equals(digest,directory.schemaDigest())
                || !columnIds().equals(directory.columns()) || routingPrefixBytes!=directory.routingPrefixBytes()) {
            throw new IllegalArgumentException("Directory differs from distributed schema content identity");
        }
    }
    /** Fixed components are opaque existing bytes: concatenation changes neither signed values nor byte order. */
    public byte[] encodeFixedKey(UUID columnId,Keys keys) {
        var schema=fixed(columnId); Objects.requireNonNull(keys);
        if (keys.keys().length!=schema.keysCount()) throw new IllegalArgumentException("Fixed key component count differs from schema");
        int size=0;
        for (int i=0;i<schema.keysCount();i++) {
            if (keys.keys()[i]==null || keys.keys()[i].size()!=schema.key(i)) throw new IllegalArgumentException("Fixed key component width differs from schema");
            size+=schema.key(i);
        }
        byte[] encoded=new byte[size]; int offset=0;
        for (Buf key:keys.keys()) { byte[] bytes=key.toByteArray(); System.arraycopy(bytes,0,encoded,offset,bytes.length); offset+=bytes.length; }
        return encoded;
    }
    public void requireFixedKey(UUID columnId,byte[] encoded) {
        var shape=fixed(columnId); Objects.requireNonNull(encoded); int bytes=0; for (int width:shape.keys()) bytes+=width;
        if (encoded.length!=bytes) throw new IllegalArgumentException("Physical fixed key width differs from schema");
    }
    public Keys decodeFixedKey(UUID columnId,byte[] encoded) {
        var schema=fixed(columnId); Objects.requireNonNull(encoded); int size=0;
        for (int width:schema.keys()) size+=width;
        if (encoded.length!=size) throw new IllegalArgumentException("Physical fixed key width differs from schema");
        var keys=new Buf[schema.keysCount()]; int offset=0;
        for (int i=0;i<keys.length;i++) { int width=schema.key(i); keys[i]=Buf.wrap(Arrays.copyOfRange(encoded,offset,offset+width)); offset+=width; }
        return new Keys(keys);
    }
    /** Existing physical hash key plus original collision-member components, both defensively owned. */
    public record EncodedKey(byte[] physicalKey,List<byte[]> variableKeys) {
        public EncodedKey {
            Objects.requireNonNull(physicalKey); Objects.requireNonNull(variableKeys);
            if (physicalKey.length>DistributedCommandCodec.MAX_ENCODED_BYTES || variableKeys.size()>DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Encoded logical key exceeds bounds");
            long bytes=2L*variableKeys.size();
            for (var key:variableKeys) {
                Objects.requireNonNull(key); bytes+=key.length;
                if (key.length>Character.MAX_VALUE || bytes>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Encoded collision member exceeds budget");
            }
            physicalKey=physicalKey.clone(); variableKeys=variableKeys.stream().map(byte[]::clone).toList();
        }
        @Override public byte[] physicalKey() { return physicalKey.clone(); }
        @Override public List<byte[]> variableKeys() { return variableKeys.stream().map(byte[]::clone).toList(); }
        @Override public boolean equals(Object other) {
            if (!(other instanceof EncodedKey key) || !Arrays.equals(physicalKey,key.physicalKey) || variableKeys.size()!=key.variableKeys.size()) return false;
            for (int i=0;i<variableKeys.size();i++) if (!Arrays.equals(variableKeys.get(i),key.variableKeys.get(i))) return false; return true;
        }
        @Override public int hashCode() { int hash=Arrays.hashCode(physicalKey); for (var key:variableKeys) hash=31*hash+Arrays.hashCode(key); return hash; }
    }
    /** Reproduces standalone calculateKey, retaining every original variable key so collisions remain distinct. */
    public EncodedKey encodeLogicalKey(UUID columnId,Keys logical) {
        var shape=column(columnId).schema; Objects.requireNonNull(logical); var parts=logical.keys();
        if (parts.length!=shape.keysCount()) throw new IllegalArgumentException("Logical key component count differs from schema");
        long original=2L*shape.variableLengthKeysCount(); int physical=0;
        for (int i=0;i<parts.length;i++) {
            var part=Objects.requireNonNull(parts[i]); int size=part.size(); original+=size; physical+=shape.key(i);
            if (i<shape.fixedLengthKeysCount() ? size!=shape.key(i) : size>Character.MAX_VALUE) throw new IllegalArgumentException("Logical key component width differs from codec");
            if (original>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Logical key exceeds command budget");
        }
        byte[] encoded=new byte[physical]; int offset=0; var variables=new ArrayList<byte[]>(shape.variableLengthKeysCount());
        for (int i=0;i<parts.length;i++) {
            byte[] bytes;
            if (i<shape.fixedLengthKeysCount()) bytes=parts[i].toByteArray();
            else { variables.add(parts[i].toByteArray()); bytes=hash(shape.variableTailKey(i),variables.getLast()); }
            System.arraycopy(bytes,0,encoded,offset,bytes.length); offset+=bytes.length;
        }
        return new EncodedKey(encoded,variables);
    }
    /** Verify that a bucket-member envelope names the physical bucket under this exact schema. */
    public void requireBucketKey(UUID columnId,byte[] physicalKey,List<byte[]> variables) {
        var shape=column(columnId).schema; Objects.requireNonNull(physicalKey); variables=List.copyOf(variables);
        if (shape.variableLengthKeysCount()==0 || variables.size()!=shape.variableLengthKeysCount()) throw new IllegalArgumentException("Bucket member shape differs from schema");
        int size=0, offset=0; for (int i=0;i<shape.keysCount();i++) { size+=shape.key(i); if (i<shape.fixedLengthKeysCount()) offset+=shape.key(i); }
        if (physicalKey.length!=size) throw new IllegalArgumentException("Bucket physical key width differs from schema");
        long bytes=2L*variables.size();
        for (int i=0;i<variables.size();i++) {
            var key=Objects.requireNonNull(variables.get(i)); bytes+=key.length;
            if (key.length>Character.MAX_VALUE || bytes>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Bucket member key exceeds budget");
            byte[] digest=hash(shape.variableTailKey(shape.fixedLengthKeysCount()+i),key);
            for (int b=0;b<digest.length;b++) if (physicalKey[offset+b]!=digest[b]) throw new IllegalArgumentException("Bucket member hash differs from physical key");
            offset+=digest.length;
        }
    }
    /** Strict complete-row decoding, including placement of every stored collision member. */
    public DistributedBucket decodeBucket(UUID columnId,byte[] physicalKey,byte[] raw) {
        var shape=column(columnId).schema; Objects.requireNonNull(physicalKey); int bytes=0; for (int width:shape.keys()) bytes+=width;
        if (shape.variableLengthKeysCount()==0 || physicalKey.length!=bytes) throw new IllegalArgumentException("Stored bucket row differs from qualified schema");
        var bucket=DistributedBucket.decode(shape,raw);
        for (var member:bucket.members()) requireBucketKey(columnId,physicalKey,member.tails()); return bucket;
    }
    private static byte[] hash(ColumnHashType hash,byte[] key) {
        var result=Buf.createZeroes(hash.bytesSize());
        try { hash.hash(Buf.wrap(key),result); } catch (UnsupportedOperationException malformed) { throw new IllegalArgumentException("Variable key differs from hash codec input",malformed); }
        return result.toByteArray();
    }
    private ColumnSchema fixed(UUID id) {
        var schema=column(id).schema;
        if (schema.variableLengthKeysCount()!=0) throw new UnsupportedOperationException("Variable-tail physical keys require the existing bucket protocol");
        return schema;
    }
    private static ColumnSchema copy(ColumnSchema schema) {
        Objects.requireNonNull(schema);
        int count=schema.keysCount(), tails=schema.variableLengthKeysCount();
        if (count>DistributedSchemaCodec.MAX_KEY_COMPONENTS || tails>count) throw new IllegalArgumentException("Invalid distributed key component count");
        var keys=new IntArrayList(schema.keys()); var hashes=new ObjectArrayList<>(schema.variableTailKeys()); long bytes=0;
        for (int i=0;i<count;i++) {
            int width=keys.getInt(i);
            if (width<=0 || (i>=count-tails && (hashes.get(i-count+tails)==null || hashes.get(i-count+tails).bytesSize()!=width))) {
                throw new IllegalArgumentException("Invalid physical key component width/hash");
            }
            bytes+=width;
        }
        if (bytes>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Physical key exceeds command byte budget");
        return new ColumnSchema(IntLists.unmodifiable(keys),ObjectLists.unmodifiable(hashes),schema.hasValue(),schema.mergeOperatorName(),schema.mergeOperatorVersion(),schema.mergeOperatorClass());
    }
}
