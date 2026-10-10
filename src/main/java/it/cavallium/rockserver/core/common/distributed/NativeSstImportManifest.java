package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Exact PUT-only external-table import identity. Files have no host paths. */
public final class NativeSstImportManifest {
    public static final int MAX_BYTES=1024*1024, MAX_FILES=64, MAX_KEY_BYTES=65536;
    public static final long MAX_FILE_BYTES=64L*1024*1024, MAX_TOTAL_BYTES=512L*1024*1024;
    private static final Comparator<UUID> ORDER=Comparator.comparing(UUID::toString);
    public record File(UUID fileId,long bytes,byte[] sha256,long entries,byte[] firstKey,byte[] lastKey) {
        public File {
            Objects.requireNonNull(fileId);
            if(bytes<=0 || bytes>MAX_FILE_BYTES || entries<=0 || sha256.length!=32 || firstKey.length>MAX_KEY_BYTES || lastKey.length>MAX_KEY_BYTES || Arrays.compareUnsigned(firstKey,lastKey)>0) throw new IllegalArgumentException("Invalid SST file identity");
            sha256=sha256.clone(); firstKey=firstKey.clone(); lastKey=lastKey.clone();
        }
        @Override public byte[] sha256() { return sha256.clone(); }
        @Override public byte[] firstKey() { return firstKey.clone(); }
        @Override public byte[] lastKey() { return lastKey.clone(); }
    }
    /** Empty file list explicitly means an empty column. */
    public record Column(UUID columnId,String physicalName,boolean merge,List<File> files) {
        public Column {
            Objects.requireNonNull(columnId); Objects.requireNonNull(physicalName);
            if(physicalName.isEmpty() || physicalName.length()>4096 || physicalName.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(physicalName)) throw new IllegalArgumentException("Invalid SST physical binding");
            files=List.copyOf(files); byte[] last=null;
            for(var file:files) { if(last!=null && Arrays.compareUnsigned(last,file.firstKey())>=0) throw new IllegalArgumentException("Overlapping or unordered SST files"); last=file.lastKey(); }
        }
    }
    public record Receipt(UUID importId,UUID groupId,long rangeEpoch,byte[] manifestDigest,ApplyPosition position) {
        public static final int BYTES=88;
        public Receipt { Objects.requireNonNull(importId); Objects.requireNonNull(groupId); Objects.requireNonNull(position); if(rangeEpoch<0 || manifestDigest.length!=32) throw new IllegalArgumentException("Invalid import receipt"); manifestDigest=manifestDigest.clone(); }
        @Override public byte[] manifestDigest() { return manifestDigest.clone(); }
        public byte[] encode() { var b=java.nio.ByteBuffer.allocate(BYTES); b.putLong(importId.getMostSignificantBits()).putLong(importId.getLeastSignificantBits()).putLong(groupId.getMostSignificantBits()).putLong(groupId.getLeastSignificantBits()).putLong(rangeEpoch).put(manifestDigest).putLong(position.term()).putLong(position.index()); return b.array(); }
        public static Receipt decode(byte[] bytes) { if(bytes.length!=BYTES) throw new IllegalArgumentException("Invalid receipt size"); var b=java.nio.ByteBuffer.wrap(bytes); UUID id=new UUID(b.getLong(),b.getLong()),group=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); byte[] digest=new byte[32]; b.get(digest); return new Receipt(id,group,epoch,digest,new ApplyPosition(b.getLong(),b.getLong())); }
        public void requireManifest(NativeSstImportManifest manifest) { if(!importId.equals(manifest.importId) || !groupId.equals(manifest.groupId) || rangeEpoch!=manifest.rangeEpoch || !Arrays.equals(manifestDigest,manifest.digest())) throw new IllegalArgumentException("Import receipt differs from manifest"); }
        public void requireCut(ApplyPosition cut) { if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term())throw new IllegalArgumentException("Import receipt exceeds/conflicts with cut"); }
    }
    /** Original committed import entry, forced before any interval reset or ingestion. */
    public record Intent(NativeSstImportManifest manifest,ApplyPosition position) {
        public Intent { Objects.requireNonNull(manifest); Objects.requireNonNull(position); }
        public byte[] encode() { byte[] definition=manifest.encode(); return java.nio.ByteBuffer.allocate(24+definition.length).putInt(0x4e53494e).putInt(definition.length).put(definition).putLong(position.term()).putLong(position.index()).array(); }
        public static Intent decode(byte[] bytes) { if(bytes.length<24 || bytes.length>MAX_BYTES+24)throw new IllegalArgumentException("Invalid import intent size"); var in=java.nio.ByteBuffer.wrap(bytes); if(in.getInt()!=0x4e53494e)throw new IllegalArgumentException("Invalid import intent tag"); int n=in.getInt(); if(n<0 || n!=in.remaining()-16)throw new IllegalArgumentException("Invalid import intent manifest size"); byte[] manifest=new byte[n]; in.get(manifest); return new Intent(NativeSstImportManifest.decode(manifest),new ApplyPosition(in.getLong(),in.getLong())); }
    }
    private final UUID importId,groupId,domainId;
    private final long rangeEpoch;
    private final byte[] schemaDigest,lower,upper,encoded;
    private final List<Column> columns;
    public NativeSstImportManifest(UUID importId,UUID groupId,long rangeEpoch,UUID domainId,byte[] schemaDigest,byte[] lowerInclusive,byte[] upperExclusive,Collection<Column> columns) {
        this.importId=Objects.requireNonNull(importId); this.groupId=Objects.requireNonNull(groupId); this.domainId=Objects.requireNonNull(domainId); this.rangeEpoch=rangeEpoch;
        if(rangeEpoch<0 || schemaDigest.length!=32 || lowerInclusive!=null && lowerInclusive.length>MAX_KEY_BYTES || upperExclusive!=null && upperExclusive.length>MAX_KEY_BYTES || lowerInclusive!=null && upperExclusive!=null && Arrays.compareUnsigned(lowerInclusive,upperExclusive)>=0) throw new IllegalArgumentException("Invalid import range");
        this.schemaDigest=schemaDigest.clone(); lower=lowerInclusive==null?null:lowerInclusive.clone(); upper=upperExclusive==null?null:upperExclusive.clone();
        this.columns=columns.stream().sorted(Comparator.comparing(Column::columnId,ORDER)).toList();
        if(this.columns.size()>256) throw new IllegalArgumentException("Too many import columns");
        var ids=new HashSet<UUID>(); var names=new HashSet<String>(); var files=new HashSet<UUID>(); long total=0;
        for(var column:this.columns) { if(!ids.add(column.columnId()) || !names.add(column.physicalName())) throw new IllegalArgumentException("Duplicate import column"); for(var file:column.files()) { if(!files.add(file.fileId())) throw new IllegalArgumentException("Duplicate import file"); total=Math.addExact(total,file.bytes()); if(!contains(file.firstKey()) || !contains(file.lastKey())) throw new IllegalArgumentException("SST bounds outside import interval"); } }
        if(files.size()>MAX_FILES || total>MAX_TOTAL_BYTES) throw new IllegalArgumentException("SST import exceeds capacity"); encoded=serialize(); if(encoded.length>MAX_BYTES) throw new IllegalArgumentException("Import manifest exceeds byte bound");
    }
    public UUID importId() { return importId; } public UUID groupId() { return groupId; } public UUID domainId() { return domainId; } public long rangeEpoch() { return rangeEpoch; }
    public byte[] schemaDigest() { return schemaDigest.clone(); } public byte[] lowerInclusive() { return lower==null?null:lower.clone(); } public byte[] upperExclusive() { return upper==null?null:upper.clone(); }
    public List<Column> columns() { return columns; }
    public boolean contains(byte[] key) { return (lower==null || Arrays.compareUnsigned(key,lower)>=0) && (upper==null || Arrays.compareUnsigned(key,upper)<0); }
    public void requireSchema(DistributedSchema schema) { if(!domainId.equals(schema.domainId()) || !Arrays.equals(schemaDigest,schema.digest()) || !schema.columnIds().equals(physicalNames().keySet())) throw new IllegalArgumentException("Import schema differs"); for(var column:columns) if(column.merge()!=(schema.column(column.columnId()).operator()!=null)) throw new IllegalArgumentException("Import operator binding differs"); }
    public Map<UUID,String> physicalNames() { var result=new HashMap<UUID,String>(); columns.forEach(c->result.put(c.columnId(),c.physicalName())); return Map.copyOf(result); }
    public Set<UUID> mergeColumns() { var result=new HashSet<UUID>(); columns.stream().filter(Column::merge).forEach(c->result.add(c.columnId())); return Set.copyOf(result); }
    public byte[] encode() { return encoded.clone(); }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encoded); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    private byte[] serialize() { try { var bytes=new ByteArrayOutputStream(); var o=new DataOutputStream(bytes); o.writeInt(0x4e535349); uuid(o,importId); uuid(o,groupId); o.writeLong(rangeEpoch); uuid(o,domainId); o.write(schemaDigest); blob(o,lower); blob(o,upper); o.writeInt(columns.size()); for(var c:columns) { uuid(o,c.columnId()); blob(o,c.physicalName().getBytes(StandardCharsets.US_ASCII)); o.writeBoolean(c.merge()); o.writeInt(c.files().size()); for(var f:c.files()) { uuid(o,f.fileId()); o.writeLong(f.bytes()); o.write(f.sha256()); o.writeLong(f.entries()); blob(o,f.firstKey()); blob(o,f.lastKey()); } } return bytes.toByteArray(); } catch(IOException impossible) { throw new AssertionError(impossible); } }
    public static NativeSstImportManifest decode(byte[] bytes) { if(bytes==null || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid import manifest size"); try { var i=new DataInputStream(new ByteArrayInputStream(bytes)); if(i.readInt()!=0x4e535349) throw new IOException("Invalid import tag"); UUID id=uuid(i),group=uuid(i); long epoch=i.readLong(); UUID domain=uuid(i); byte[] schema=i.readNBytes(32),lo=blob(i,true),hi=blob(i,true); int n=i.readInt(); if(n<0 || n>256) throw new IOException("Column bound"); var cols=new ArrayList<Column>(); int count=0; for(int c=0;c<n;c++) { UUID column=uuid(i); String name=new String(blob(i,false),StandardCharsets.US_ASCII); boolean merge=i.readBoolean(); int size=i.readInt(); if(size<0 || (count+=size)>MAX_FILES) throw new IOException("File bound"); var fs=new ArrayList<File>(); for(int f=0;f<size;f++) fs.add(new File(uuid(i),i.readLong(),i.readNBytes(32),i.readLong(),blob(i,false),blob(i,false))); cols.add(new Column(column,name,merge,fs)); } if(i.available()!=0) throw new IOException("Trailing manifest bytes"); var result=new NativeSstImportManifest(id,group,epoch,domain,schema,lo,hi,cols); if(!Arrays.equals(bytes,result.encode())) throw new IOException("Noncanonical import manifest"); return result; } catch(IOException|RuntimeException invalid) { throw new IllegalArgumentException("Invalid import manifest",invalid); } }
    private static void uuid(DataOutputStream o,UUID id)throws IOException { o.writeLong(id.getMostSignificantBits()); o.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream i)throws IOException { return new UUID(i.readLong(),i.readLong()); }
    private static void blob(DataOutputStream o,byte[] b)throws IOException { o.writeInt(b==null?-1:b.length); if(b!=null)o.write(b); }
    private static byte[] blob(DataInputStream i,boolean nullable)throws IOException { int n=i.readInt(); if(n==-1&&nullable)return null; if(n<0||n>MAX_KEY_BYTES||n>i.available())throw new IOException("Blob bound"); return i.readNBytes(n); }
}
