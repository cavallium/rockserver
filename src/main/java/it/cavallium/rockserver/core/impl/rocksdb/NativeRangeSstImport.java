package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.*;
import org.rocksdb.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Native PUT-table qualification and immutable local original-file ownership. */
public final class NativeRangeSstImport {
    public static final int MAX_ROW_BYTES=3*1024*1024;
    public enum Stage { ORIGINALS_FORCED, INTENT_FORCED, INTERVAL_RESET, BATCH_INGESTED, COLUMN_INGESTED, DATA_DURABLE, RECEIPT_FORCED }
    @FunctionalInterface public interface Observer { void at(Stage stage) throws IOException; }
    /** Trusted catalogue publication authority, supplied only after the matching global import is published. */
    public record Activation(NativeSstImportManifest.Receipt receipt,UUID catalogueGroup,ApplyPosition cataloguePosition,byte[] publicationDigest,ApplyPosition position) {
        public Activation { Objects.requireNonNull(receipt); Objects.requireNonNull(catalogueGroup); Objects.requireNonNull(cataloguePosition); Objects.requireNonNull(position); if(publicationDigest.length!=32 || position.index()<=receipt.position().index() || position.term()<receipt.position().term()) throw new IllegalArgumentException("Invalid import publication/activation cut"); publicationDigest=publicationDigest.clone(); }
        @Override public byte[] publicationDigest() { return publicationDigest.clone(); }
        public byte[] encode() { return ByteBuffer.allocate(168).put(receipt.encode()).putLong(catalogueGroup.getMostSignificantBits()).putLong(catalogueGroup.getLeastSignificantBits()).putLong(cataloguePosition.term()).putLong(cataloguePosition.index()).put(publicationDigest).putLong(position.term()).putLong(position.index()).array(); }
        public static Activation decode(byte[] encoded) { if(encoded.length!=168) throw new IllegalArgumentException("Invalid import activation"); var b=ByteBuffer.wrap(encoded); byte[] receipt=new byte[88],digest=new byte[32]; b.get(receipt); UUID catalogue=new UUID(b.getLong(),b.getLong()); var cut=new ApplyPosition(b.getLong(),b.getLong()); b.get(digest); return new Activation(NativeSstImportManifest.Receipt.decode(receipt),catalogue,cut,digest,new ApplyPosition(b.getLong(),b.getLong())); }
        public void requireCut(ApplyPosition cut) { receipt.requireCut(position); if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term())throw new IllegalArgumentException("Import activation exceeds/conflicts with cut"); }
    }
    private NativeRangeSstImport() {}
    /** Original bounded file acknowledgment, atomically committed with its native apply marker. */
    public record FileReceipt(UUID clientId,long callId,byte[] requestDigest,ApplyPosition position,byte[] result) {
        public static final int MAX_RESULT_BYTES=128,MAX_RECEIPTS=4096;
        public FileReceipt { Objects.requireNonNull(clientId);Objects.requireNonNull(position);if(callId<0 || requestDigest.length!=32 || position.term()<1 || result.length<1 || result.length>MAX_RESULT_BYTES)throw new IllegalArgumentException("Invalid import file receipt");requestDigest=requestDigest.clone();result=result.clone(); }
        @Override public byte[] requestDigest(){return requestDigest.clone();}
        @Override public byte[] result(){return result.clone();}
        public byte[] encode(){return ByteBuffer.allocate(80+result.length).putInt(0x4e534652).putLong(clientId.getMostSignificantBits()).putLong(clientId.getLeastSignificantBits()).putLong(callId).put(requestDigest).putLong(position.term()).putLong(position.index()).putInt(result.length).put(result).array();}
        public static FileReceipt decode(byte[] bytes){if(bytes==null || bytes.length<81 || bytes.length>80+MAX_RESULT_BYTES)throw new IllegalArgumentException("Invalid file receipt length");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x4e534652)throw new IllegalArgumentException("Invalid file receipt tag");UUID client=new UUID(in.getLong(),in.getLong());long call=in.getLong();byte[] digest=new byte[32];in.get(digest);var position=new ApplyPosition(in.getLong(),in.getLong());int size=in.getInt();if(size!=in.remaining())throw new IllegalArgumentException("Invalid file receipt body");byte[] result=new byte[size];in.get(result);return new FileReceipt(client,call,digest,position,result);}
        public void requireCut(ApplyPosition cut){if(cut==null || position.term()>cut.term() || position.index()>cut.index() || position.index()==cut.index() && position.term()!=cut.term())throw new IllegalArgumentException("File receipt exceeds completed native cut");}
    }
    private static Path directory(Path root,NativeSstImportManifest manifest) { return root.resolve(manifest.groupId()+"-"+manifest.importId()); }
    public static Path original(Path root,NativeSstImportManifest manifest,NativeSstImportManifest.File file) { return directory(root,manifest).resolve(file.fileId()+".sst"); }
    public static void retain(Path root,NativeSstImportManifest manifest,Map<UUID,Path> sources) throws IOException {
        var needed=new HashSet<UUID>(); manifest.columns().forEach(c->c.files().forEach(f->needed.add(f.fileId())));
        if(!needed.containsAll(sources.keySet())) throw new IllegalArgumentException("Unknown import original");
        Files.createDirectories(root); Path directory=directory(root,manifest); Files.createDirectories(directory); forceDirectory(root); forceDirectory(root.getParent());
        // Caller exclusively owns this retention directory; private unlinked copies are recoverable.
        try(var leftovers=Files.newDirectoryStream(directory,"incoming-*.sst")) { for(var file:leftovers)Files.delete(file); }
        for(var column:manifest.columns()) for(var file:column.files()) {
            Path destination=original(root,manifest,file);
            if(Files.exists(destination)) { verifyBytes(destination,file); try(var channel=FileChannel.open(destination,StandardOpenOption.READ)) { channel.force(true); } continue; }
            Path source=sources.get(file.fileId()); if(source==null) throw new IOException("Missing immutable import original "+file.fileId());
            Path temp=Files.createTempFile(directory,"incoming-",".sst");
            try {
                try(var input=FileChannel.open(source,StandardOpenOption.READ); var output=FileChannel.open(temp,StandardOpenOption.WRITE)) {
                    if(input.size()!=file.bytes()) throw new IOException("Import original size differs"); var buffer=ByteBuffer.allocate(64*1024); long remaining=file.bytes();
                    while(remaining>0) { buffer.clear().limit((int)Math.min(buffer.capacity(),remaining)); int n=input.read(buffer); if(n<0)throw new EOFException("Import original shortened"); remaining-=n; buffer.flip(); while(buffer.hasRemaining())output.write(buffer); } if(input.size()!=file.bytes())throw new IOException("Import original changed"); output.force(true);
                }
                verifyBytes(temp,file); Files.createLink(destination,temp); forceDirectory(directory);
            } finally { Files.deleteIfExists(temp); }
        }
        forceDirectory(directory);
    }
    static void verifyBytes(Path path,NativeSstImportManifest.File file) throws IOException {
        if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)!=file.bytes())throw new IOException("SST original missing or changed");
        try(var input=Files.newInputStream(path)) { var digest=sha(); byte[] buffer=new byte[64*1024]; long total=0; int n; while((n=input.read(buffer))!=-1) { total+=n; if(total>file.bytes())throw new IOException("SST original exceeds exact length"); digest.update(buffer,0,n); } if(total!=file.bytes() || !MessageDigest.isEqual(digest.digest(),file.sha256()))throw new IOException("SST original digest differs"); }
    }
    static void qualify(Path path,NativeSstImportManifest.File file,NativeSstImportManifest manifest,UUID column,DistributedSchema schema,Options options) throws IOException,RocksDBException {
        verifyBytes(path,file);
        try(var reader=new SstFileReader(options); var reads=new ReadOptions().setFillCache(false)) {
            reader.open(path.toString()); reader.verifyChecksum(); var properties=reader.getTableProperties();
            if(properties.getNumDeletions()!=0 || properties.getNumMergeOperands()!=0 || properties.getNumRangeDeletions()!=0 || properties.getNumEntries()!=file.entries())throw new IOException("Import requires exact materialized PUT-only SST");
            if(!"leveldb.BytewiseComparator".equals(properties.getComparatorName()))throw new IOException("Import comparator differs");
            long count=0; byte[] previous=null,first=null;var sizing=ByteBuffer.allocate(0);
            try(var iterator=reader.newIterator(reads)) {
                for(iterator.seekToFirst();iterator.isValid();iterator.next()) {
                    int keyBytes=iterator.key(sizing),valueBytes=iterator.value(sizing);if(keyBytes<0 || valueBytes<0 || keyBytes+(long)valueBytes>MAX_ROW_BYTES || valueBytes>DistributedCommandCodec.MAX_ENCODED_BYTES)throw new IOException("SST row exceeds bounded native/public value allocation");byte[] key=iterator.key(),value=iterator.value(); if(key.length+(long)value.length>MAX_ROW_BYTES || value.length>DistributedCommandCodec.MAX_ENCODED_BYTES || !manifest.contains(key) || previous!=null && Arrays.compareUnsigned(previous,key)>=0)throw new IOException("Invalid import row bounds/order/public value limit");
                    var shape=schema.column(column).schema();
                    try { if(shape.variableLengthKeysCount()!=0) schema.decodeBucket(column,key,value); else { schema.requireFixedKey(column,key); if(!shape.hasValue() && value.length!=0)throw new IllegalArgumentException("Key-only import has value bytes"); } } catch(IllegalArgumentException invalid) { throw new IOException("SST row differs from logical schema",invalid); }
                    if(first==null)first=key; previous=key; count++;
                }
                iterator.status();
            }
            if(count!=file.entries() || !Arrays.equals(first,file.firstKey()) || !Arrays.equals(previous,file.lastKey()))throw new IOException("SST row identity differs from manifest");
        }
    }
    static void forceDirectory(Path path) throws IOException { try(var channel=FileChannel.open(path,StandardOpenOption.READ)) { channel.force(true); } }
    private static MessageDigest sha() { try { return MessageDigest.getInstance("SHA-256"); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
