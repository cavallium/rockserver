package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import org.rocksdb.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Independently copied destination bytes. A caller must qualify the configured physical failure domain. */
public final class RatisDatabaseBackupArchive implements AutoCloseable {
    private final Path root;
    private final DatabaseBackupDefinition definition;
    private final DatabaseBackupManifest.Destination destination;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private boolean closed;
    private static final int JOURNAL_MAX=DatabaseBackupDefinition.MAX_BYTES+32;

    public RatisDatabaseBackupArchive(Path root,DatabaseBackupDefinition definition,DatabaseBackupManifest.Destination destination) throws IOException {
        this.root=Objects.requireNonNull(root).toAbsolutePath().normalize();this.definition=Objects.requireNonNull(definition);this.destination=Objects.requireNonNull(destination);
        Files.createDirectories(this.root);
        if(Files.isSymbolicLink(this.root) || !this.root.toRealPath().equals(this.root)) throw new IOException("Archive root is not a direct owned directory");
        Path lockPath=this.root.resolve("owner.lock"); if(Files.isSymbolicLink(lockPath))throw new IOException("Archive lock is foreign");
        lockChannel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
        FileLock acquired=null;
        try {
            acquired=lockChannel.tryLock(); if(acquired==null)throw new IOException("Archive already owned");
            byte[] identity=ByteBuffer.allocate(32+definition.encode().length).putLong(destination.archiveId().getMostSignificantBits()).putLong(destination.archiveId().getLeastSignificantBits()).putLong(destination.failureDomainId().getMostSignificantBits()).putLong(destination.failureDomainId().getLeastSignificantBits()).put(definition.encode()).array();
            Path metadata=this.root.resolve("definition");
            if(Files.exists(metadata,LinkOption.NOFOLLOW_LINKS)){regular(metadata);if(!Arrays.equals(identity,SessionJournalFiles.read(metadata,JOURNAL_MAX+32)))throw new IOException("Archive identity changed");}
            else { if(Files.exists(this.root.resolve("definition.pending"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Uncertain archive initialization");SessionJournalFiles.write(metadata,identity,JOURNAL_MAX+32); }
            scan();
            lock=acquired;
        } catch(Throwable failure) { if(acquired!=null)acquired.close();lockChannel.close();throw failure; }
    }

    /** Copies a real retained-cut materialized USER table; no live source path is retained in the archive model. */
    public synchronized DatabaseBackupManifest.Chunk copy(UUID domainId,long ordinal,UUID objectId,NativeRangeSstExport.Manifest source) throws IOException,RocksDBException {
        requireOpen();var domain=definition.domain(domainId);var range=domain.directory().ranges().stream().filter(r->r.groupId().equals(source.groupId())).findFirst().orElseThrow(()->new IllegalArgumentException("Foreign export group"));
        if(source.rangeEpoch()!=range.rangeEpoch() || !Arrays.equals(source.schemaDigest(),domain.schema().digest()) || !domain.directory().columns().contains(source.columnId()))throw new IllegalArgumentException("Export schema/lineage changed");
        if(source.rows()==0)return new DatabaseBackupManifest.Chunk(domainId,source.groupId(),source.columnId(),ordinal,source.position(),source.lowerInclusive(),source.upperExclusive(),null,0,new byte[0],0,null,null);
        Path target=object(objectId,"sst");retain(target,source.file(),source.fileBytes(),source.sha256(),NativeSstImportManifest.MAX_FILE_BYTES);
        byte[][] keys=inspect(target,domain.schema(),source.columnId(),source.rows(),source.lowerInclusive(),source.upperExclusive());
        return new DatabaseBackupManifest.Chunk(domainId,source.groupId(),source.columnId(),ordinal,source.position(),source.lowerInclusive(),source.upperExclusive(),objectId,source.fileBytes(),source.sha256(),source.rows(),keys[0],keys[1]);
    }
    public synchronized DatabaseBackupManifest.OperatorAsset copyOperator(UUID objectId,Path source,byte[] expectedHash) throws IOException {
        requireOpen(); boolean required=definition.domains().stream().flatMap(d->d.schema().columns().stream()).anyMatch(c->c.operator()!=null && MessageDigest.isEqual(c.operator().artifactSha256(),expectedHash));
        if(!required)throw new IllegalArgumentException("Foreign operator asset");regular(source);long size=Files.size(source);
        retain(object(objectId,"jar"),source,size,expectedHash,MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES);
        return new DatabaseBackupManifest.OperatorAsset(objectId,expectedHash,size);
    }
    /** Publication happens only after rereading and forcing every exact destination object. */
    public synchronized void complete(DatabaseBackupManifest manifest) throws IOException,RocksDBException {
        requireOpen();verify(manifest);byte[] bytes=manifest.encode();Path marker=root.resolve("COMPLETE");
        if(Files.exists(marker,LinkOption.NOFOLLOW_LINKS)){regular(marker);if(!Arrays.equals(bytes,SessionJournalFiles.read(marker,JOURNAL_MAX)))throw new IOException("COMPLETE identity changed");return;}
        if(Files.exists(root.resolve("COMPLETE.pending"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Uncertain COMPLETE metadata");
        SessionJournalFiles.write(marker,bytes,JOURNAL_MAX);
    }
    public synchronized DatabaseBackupManifest reopenComplete() throws IOException,RocksDBException {
        requireOpen();Path marker=root.resolve("COMPLETE");regular(marker);var manifest=DatabaseBackupManifest.decode(SessionJournalFiles.read(marker,JOURNAL_MAX));verify(manifest);return manifest;
    }
    public synchronized Path objectPath(UUID id,boolean operator) throws IOException {requireOpen();Path path=object(id,operator?"jar":"sst");regular(path);return path;}
    private void verify(DatabaseBackupManifest manifest) throws IOException,RocksDBException {
        if(!destination.equals(manifest.destination()) || !Arrays.equals(definition.encode(),manifest.definition().encode()))throw new IOException("COMPLETE vector/destination changed");
        var referenced=new HashSet<Path>();
        for(var c:manifest.chunks())if(c.objectId()!=null){Path path=object(c.objectId(),"sst");referenced.add(path);verifyBytes(path,c.bytes(),c.sha256());byte[][] keys=inspect(path,definition.domain(c.domainId()).schema(),c.columnId(),c.entries(),c.lowerInclusive(),c.upperExclusive());if(!Arrays.equals(keys[0],c.firstKey()) || !Arrays.equals(keys[1],c.lastKey()))throw new IOException("Archive row identity changed");forceFile(path);}
        for(var a:manifest.operators()){if(a.bytes()>MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES)throw new IOException("Operator exceeds native budget");Path path=object(a.objectId(),"jar");referenced.add(path);verifyBytes(path,a.bytes(),a.sha256());forceFile(path);}
        try(var files=Files.newDirectoryStream(root)){for(var p:files)if(p.getFileName().toString().endsWith(".sst") || p.getFileName().toString().endsWith(".jar")){if(!referenced.contains(p))throw new IOException("Unreferenced destination object");}}
        scan();SessionJournalFiles.force(root);
    }
    private void retain(Path target,Path source,long bytes,byte[] hash,long maximum) throws IOException {
        if(bytes<1 || bytes>maximum || hash.length!=32)throw new IOException("Object size/hash exceeds bound");regular(source);
        if(Files.exists(root.resolve("COMPLETE"),LinkOption.NOFOLLOW_LINKS)) { verifyBytes(target,bytes,hash);return; }
        long occupied=scan();
        if(Files.exists(target,LinkOption.NOFOLLOW_LINKS)){verifyBytes(target,bytes,hash);if(Files.isSameFile(source,target))throw new IOException("Destination is the source artifact");return;}
        Path pending=target.resolveSibling(target.getFileName()+".pending");
        if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS))throw new IOException("Uncertain archive object");
        if(bytes>definition.limits().maximumArchiveBytes()-occupied)throw new IOException("Destination object quota exhausted before copy");
        // CREATE_NEW, never link the source: destination remains independent after source retention disappears.
        boolean ownedPending=false;
        try {
            Files.createFile(pending); ownedPending=true;
        try(var input=FileChannel.open(source,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS);var output=FileChannel.open(pending,StandardOpenOption.WRITE)) {
            if(input.size()!=bytes)throw new IOException("Source size changed");var buffer=ByteBuffer.allocate(64*1024);long remaining=bytes;
            while(remaining>0){buffer.clear().limit((int)Math.min(buffer.capacity(),remaining));int n=input.read(buffer);if(n<0)throw new EOFException("Source shortened");remaining-=n;buffer.flip();while(buffer.hasRemaining())output.write(buffer);}
            if(input.size()!=bytes)throw new IOException("Source size changed");output.force(true);
        }
        verifyBytes(pending,bytes,hash);Files.move(pending,target,StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(root);
        } finally { if(ownedPending && Files.deleteIfExists(pending))SessionJournalFiles.force(root); }
        if(Files.isSameFile(source,target))throw new IOException("Archive did not produce an independent copy");
    }
    private long scan() throws IOException {
        long bytes=0;int count=0;try(var files=Files.newDirectoryStream(root)){for(var p:files){regular(p);String name=p.getFileName().toString();if(name.equals("owner.lock") || name.equals("definition") || name.equals("COMPLETE"))continue;
            if(!name.matches("[0-9a-f-]{36}\\.(sst|jar)"))throw new IOException("Foreign or uncertain archive file "+name);
            try{UUID.fromString(name.substring(0,36));}catch(IllegalArgumentException invalid){throw new IOException("Invalid object identity",invalid);}
            bytes=Math.addExact(bytes,Files.size(p));if(++count>definition.limits().maximumChunks()+(long)definition.limits().maximumColumnPairs() || bytes>definition.limits().maximumArchiveBytes())throw new IOException("Archive destination exceeds bounds");}}
        return bytes;
    }
    private static byte[][] inspect(Path path,DistributedSchema schema,UUID column,long rows,byte[] lower,byte[] upper) throws IOException,RocksDBException {
        try(var options=new Options();var reader=new SstFileReader(options);var read=new ReadOptions().setFillCache(false)){
            reader.open(path.toString());reader.verifyChecksum();var props=reader.getTableProperties();
            if(props.getNumEntries()!=rows || props.getNumDeletions()!=0 || props.getNumMergeOperands()!=0 || props.getNumRangeDeletions()!=0 || !"leveldb.BytewiseComparator".equals(props.getComparatorName()))throw new IOException("Archive requires materialized PUT-only tables");
            byte[] first=null,last=null;long count=0;try(var iterator=reader.newIterator(read)){for(iterator.seekToFirst();iterator.isValid();iterator.next()){
                byte[] key=iterator.key(),value=iterator.value();if((long)key.length+value.length>NativeRangeSstImport.MAX_ROW_BYTES || value.length>DistributedCommandCodec.MAX_ENCODED_BYTES || lower!=null && Arrays.compareUnsigned(key,lower)<0 || upper!=null && Arrays.compareUnsigned(key,upper)>=0 || last!=null && Arrays.compareUnsigned(last,key)>=0)throw new IOException("Archive row scope/order exceeds bounds");
                try{var shape=schema.column(column).schema();if(shape.variableLengthKeysCount()!=0)schema.decodeBucket(column,key,value);else{schema.requireFixedKey(column,key);if(!shape.hasValue() && value.length!=0)throw new IllegalArgumentException("Key-only value");}}catch(IllegalArgumentException invalid){throw new IOException("Archive physical schema changed",invalid);}
                if(first==null)first=key;last=key;if(++count>rows)throw new IOException("Archive rows exceed declaration");}iterator.status();}
            if(count!=rows || first==null)throw new IOException("Archive rows differ");return new byte[][]{first,last};
        }
    }
    private Path object(UUID id,String suffix){return root.resolve(Objects.requireNonNull(id)+"."+suffix);}
    private static void regular(Path path)throws IOException{if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Missing or foreign archive file "+path);}
    private static void verifyBytes(Path path,long bytes,byte[] hash)throws IOException{regular(path);if(Files.size(path)!=bytes)throw new IOException("Archive object length changed");try(var input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){MessageDigest digest;try{digest=MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}byte[] buffer=new byte[64*1024];long total=0;int n;while((n=input.read(buffer))!=-1){total+=n;if(total>bytes)throw new IOException("Archive object grew");digest.update(buffer,0,n);}if(total!=bytes || !MessageDigest.isEqual(digest.digest(),hash))throw new IOException("Archive object checksum changed");}}
    private static void forceFile(Path path)throws IOException{try(var channel=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){channel.force(true);}}
    private void requireOpen()throws IOException{if(closed)throw new IOException("Archive owner closed");}
    @Override public synchronized void close()throws IOException{if(!closed){closed=true;try{lock.close();}finally{lockChannel.close();}}}
}
