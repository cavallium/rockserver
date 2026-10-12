package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.*;
import org.apache.ratis.util.MD5FileUtil;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Forced transfer-ID artifacts. Closing a live/process owner preserves files; only durable RELEASE reclaims them. */
public final class RatisTopologySourceSnapshotArtifacts implements AutoCloseable {
    private static final int RELEASE_MAGIC=0x54534152,RELEASE_BODY_BYTES=124,MAX_RELEASED=1024;
    private static final int MAX_METADATA_FILE=MAX_REPLY_BYTES+32;
    public record Saved(Prepared prepared,Path image,SnapshotInfo snapshot) {}
    private record Receipt(UUID id,byte[] requestDigest,byte[] metadataDigest,byte[] imageDigest,long imageBytes) {}
    private final Path root;private final NativeRangeImage.Limits imageLimits;
    private final long maximumImageBytes,maximumMetadataBytes;private final int maximumArtifacts;
    private final FileChannel lockChannel;private final FileLock lock;private final SessionJournalFiles.Removal removal;
    private boolean closed;
    public RatisTopologySourceSnapshotArtifacts(Path root,NativeRangeImage.Limits imageLimits,long maximumImageBytes,long maximumMetadataBytes,int maximumArtifacts)throws IOException {
        this(root,imageLimits,maximumImageBytes,maximumMetadataBytes,maximumArtifacts,SessionJournalFiles.FILESYSTEM);
    }
    RatisTopologySourceSnapshotArtifacts(Path ownedRoot,NativeRangeImage.Limits imageLimits,long maximumImageBytes,long maximumMetadataBytes,int maximumArtifacts,SessionJournalFiles.Removal removal)throws IOException {
        this.root=ownedRoot.toAbsolutePath().normalize();directory(root);this.imageLimits=Objects.requireNonNull(imageLimits);this.removal=Objects.requireNonNull(removal);
        if(maximumImageBytes<1 || maximumMetadataBytes<1 || maximumArtifacts<1 || maximumArtifacts>4)throw new IllegalArgumentException("Invalid source artifact quota");
        this.maximumImageBytes=maximumImageBytes;this.maximumMetadataBytes=maximumMetadataBytes;this.maximumArtifacts=maximumArtifacts;
        Path lockPath=root.resolve("artifacts.lock");if(Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS))regular(lockPath,0);
        lockChannel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try{try{acquired=lockChannel.tryLock();}catch(OverlappingFileLockException busy){throw new IOException("Source artifact directory already owned",busy);}if(acquired==null)throw new IOException("Source artifact directory already owned");inventory(null,null);}
        catch(IOException|RuntimeException failure){try{if(acquired!=null)acquired.release();}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{lockChannel.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}lock=acquired;
    }
    public synchronized Saved lookup(Request request,Path managedSnapshotDir)throws IOException {
        requireOpen();managedSnapshotDir=managedSnapshotDir.toAbsolutePath().normalize();directory(managedSnapshotDir);UUID id=request.transferId();
        if(exists(path(id,"released"))){var receipt=receipt(id);requireReceipt(request,receipt);cleanup(receipt);throw new IOException("Source transfer was durably released");}
        if(!exists(path(id,"prepared")) && exists(path(id,"prepared.pending"))){
            var pending=path(id,"prepared.pending");var prepared=decodeMetadata(id,read(pending,MAX_METADATA_FILE));requireRequestIo(request,prepared.request());inventory(null,null);Files.move(pending,path(id,"prepared"),StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(path(id,"prepared"));SessionJournalFiles.force(root);
        }
        if(!exists(path(id,"prepared"))){if(exists(path(id,"image")) || exists(path(id,"image.pending")) || exists(path(id,"prepared.pending")))throw new IOException("Unbound or uncertain source artifact cannot be recaptured");return null;}
        var prepared=metadata(id);requireRequestIo(request,prepared.request());inventory(null,null);cleanMetadataPending(path(id,"prepared"),reply(PREPARE,prepared));
        Path image=path(id,"image");if(!exists(image)){
            Path source=managedSnapshotDir.resolve(name(prepared));if(!exists(source))throw new IOException("Forced source intent has no original image; exact repair required");
            validateImage(prepared,source);publishImage(source,image,prepared);
        }
        validateImage(prepared,image);var saved=new Saved(prepared,image,snapshot(image,prepared));return new Saved(prepared,image,restoreManaged(saved,managedSnapshotDir));
    }
    public synchronized Saved retain(Prepared prepared,SnapshotInfo snapshot)throws IOException {
        requireOpen();Objects.requireNonNull(prepared);Objects.requireNonNull(snapshot);UUID id=prepared.request().transferId();
        if(exists(path(id,"released"))){requireReceipt(prepared.request(),receipt(id));throw new IOException("Released source transfer cannot be retained again");}
        if(snapshot.getFiles().size()!=1 || snapshot.getTerm()!=prepared.image().position().term() || snapshot.getIndex()!=prepared.image().position().index())throw new IOException("Source snapshot identity changed");
        Path source=snapshot.getFiles().getFirst().getPath().toAbsolutePath().normalize();if(!source.getFileName().toString().equals(name(prepared)))throw new IOException("Source snapshot must use the managed Ratis filename");
        regular(source,prepared.image().bytes());validateImage(prepared,source);MD5Hash md5=MD5FileUtil.computeMd5ForFile(source.toFile());var declared=snapshot.getFiles().getFirst().getFileDigest();if(declared!=null && !declared.equals(md5))throw new IOException("Source snapshot MD5 changed");
        byte[] encoded=reply(PREPARE,prepared);inventory(prepared,encoded);
        Path metadata=path(id,"prepared");if(exists(metadata)){byte[] prior=read(metadata,MAX_METADATA_FILE);if(!Arrays.equals(prior,encoded))throw new IOException("Original forced source metadata changed");}
        else {
            if(exists(path(id,"image")) || exists(path(id,"image.pending")))throw new IOException("Foreign image already uses transfer identity");
            cleanMetadataPending(metadata,encoded);SessionJournalFiles.write(metadata,encoded,MAX_METADATA_FILE);
        }
        cleanMetadataPending(metadata,encoded);publishImage(source,path(id,"image"),prepared);validateImage(prepared,path(id,"image"));
        return new Saved(prepared,path(id,"image"),new SingleFileSnapshotInfo(new FileInfo(source,md5),snapshot.getTerm(),snapshot.getIndex()));
    }
    /** Restores one standard file and MD5; never updates or rewinds a state machine's latest pointer. */
    public synchronized SnapshotInfo restoreManaged(Saved saved,Path managedSnapshotDir)throws IOException {
        requireOpen();Objects.requireNonNull(saved);Path directory=managedSnapshotDir.toAbsolutePath().normalize();directory(directory);
        var prepared=saved.prepared();var forced=metadata(prepared.request().transferId());requirePrepared(forced,prepared);if(exists(path(prepared.request().transferId(),"released")))throw new IOException("Released source image cannot be restored");
        inventory(null,null);Path image=path(prepared.request().transferId(),"image");if(!image.equals(saved.image().toAbsolutePath().normalize()))throw new IOException("Saved image escaped owned artifact directory");validateImage(prepared,image);
        Path target=directory.resolve(name(prepared));publishImage(image,target,prepared);validateImage(prepared,target);
        MD5Hash digest=MD5FileUtil.computeMd5ForFile(target.toFile());Path digestFile=MD5FileUtil.getDigestFileForFile(target.toFile()).toPath();
        if(exists(digestFile)){regular(digestFile,512);MD5FileUtil.verifySavedMD5(target.toFile(),digest);}else{if(exists(digestFile.resolveSibling(digestFile.getFileName()+".tmp")))throw new IOException("Uncertain managed snapshot MD5 staging file");MD5FileUtil.saveMD5File(target.toFile(),digest);SessionJournalFiles.force(digestFile);SessionJournalFiles.force(directory);}
        return new SingleFileSnapshotInfo(new FileInfo(target,digest),prepared.image().position().term(),prepared.image().position().index());
    }
    /** Retry an already forced release only. This never creates a receipt or authorizes an unknown transfer. */
    public synchronized boolean isReleased(Request request)throws IOException {
        requireOpen();if(!exists(path(request.transferId(),"released")))return false;
        var receipt=receipt(request.transferId());requireReceipt(request,receipt);cleanup(receipt);return true;
    }
    public synchronized void released(Request request)throws IOException {
        requireOpen();UUID id=request.transferId();Receipt receipt;
        if(exists(path(id,"released"))){receipt=receipt(id);requireReceipt(request,receipt);}
        else {
            var prepared=metadata(id);requireRequestIo(request,prepared.request());if(exists(path(id,"image")))validateImage(prepared,path(id,"image"));
            inventory(null,null);if(metadataUsage()+RELEASE_BODY_BYTES+32>maximumMetadataBytes)throw new IOException("Source release receipt metadata quota exhausted");int releases=0;try(var entries=Files.newDirectoryStream(root,"*.released")){for(var ignored:entries)if(++releases>=MAX_RELEASED)throw new IOException("Source release receipt quota exhausted");}
            byte[] encoded=reply(PREPARE,prepared);receipt=new Receipt(id,digest(request(RELEASE,request)),digest(encoded),prepared.image().sha256(),prepared.image().bytes());
            if(Files.exists(path(id,"released.pending"),LinkOption.NOFOLLOW_LINKS))regular(path(id,"released.pending"),RELEASE_BODY_BYTES+32);
            Path releasePending=path(id,"released.pending");if(exists(releasePending)){byte[] body=encode(receipt);requirePrefixBytes(releasePending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());Files.delete(releasePending);SessionJournalFiles.force(root);}
            SessionJournalFiles.write(path(id,"released"),encode(receipt),RELEASE_BODY_BYTES+32);
        }
        cleanup(receipt);
    }
    private void cleanup(Receipt receipt)throws IOException {
        Path metadata=path(receipt.id(),"prepared"),image=path(receipt.id(),"image"),pending=path(receipt.id(),"image.pending");
        byte[] metadataBody=null;
        if(exists(metadata)){metadataBody=read(metadata,MAX_METADATA_FILE);if(!Arrays.equals(digest(metadataBody),receipt.metadataDigest()))throw new IOException("Released source metadata changed");}
        if(exists(pending)){
            regular(pending,receipt.imageBytes());
            if(exists(image))requirePrefix(pending,image);
            else {var partial=NativeRangeImage.inspect(pending,imageLimits);if(!Arrays.equals(partial.sha256(),receipt.imageDigest()) || partial.bytes()!=receipt.imageBytes())throw new IOException("Uncertain staging image cannot be reclaimed");}
        }
        Path metadataPending=path(receipt.id(),"prepared.pending");
        if(exists(metadataPending)){if(metadataBody==null){metadataBody=read(metadataPending,MAX_METADATA_FILE);if(!Arrays.equals(digest(metadataBody),receipt.metadataDigest()))throw new IOException("Uncertain released metadata staging");}requirePrefixBytes(metadataPending,ByteBuffer.allocate(metadataBody.length+32).put(metadataBody).put(digest(metadataBody)).array());}
        if(exists(image)){regular(image,receipt.imageBytes());var manifest=NativeRangeImage.inspect(image,imageLimits);if(manifest.bytes()!=receipt.imageBytes() || !Arrays.equals(manifest.sha256(),receipt.imageDigest()))throw new IOException("Released source image changed");removal.delete(image);}
        if(exists(pending)){regular(pending,receipt.imageBytes());removal.delete(pending);}
        if(exists(metadataPending))removal.delete(metadataPending);
        if(exists(metadata))removal.delete(metadata);removal.force(root);
    }
    private long metadataUsage()throws IOException {long bytes=0;try(var files=Files.newDirectoryStream(root)){for(var file:files){String name=file.getFileName().toString();if(name.endsWith(".prepared") || name.endsWith(".prepared.pending"))bytes=Math.addExact(bytes,3*Files.size(file));else if(name.endsWith(".released") || name.endsWith(".released.pending"))bytes=Math.addExact(bytes,Files.size(file));}}return bytes;}
    private void inventory(Prepared candidate,byte[] candidateBytes)throws IOException {
        var metadata=new HashMap<UUID,Prepared>();var imageIds=new HashSet<UUID>();int releases=0;long images=0,bytes=0;int files=0;
        try(var entries=Files.newDirectoryStream(root)){for(var file:entries){if(++files>MAX_RELEASED+maximumArtifacts*5+1)throw new IOException("Source artifact directory entry quota exceeded");String name=file.getFileName().toString();if(name.equals("artifacts.lock")){regular(file,0);continue;}
            int dot=name.indexOf('.');if(dot!=36)throw new IOException("Foreign source artifact path");UUID id;try{id=UUID.fromString(name.substring(0,dot));}catch(IllegalArgumentException malformed){throw new IOException("Foreign source artifact identity",malformed);}if(!id.toString().equals(name.substring(0,dot)))throw new IOException("Noncanonical source artifact identity");String suffix=name.substring(dot+1);
            if(suffix.equals("released")){regular(file,RELEASE_BODY_BYTES+32);receipt(id);if(++releases>MAX_RELEASED)throw new IOException("Source release receipt quota exceeded");bytes+=Files.size(file);}
            else if(suffix.equals("prepared")){regular(file,MAX_METADATA_FILE);bytes=Math.addExact(bytes,3*Files.size(file));if(bytes>maximumMetadataBytes || !metadata.containsKey(id) && metadata.size()>=maximumArtifacts)throw new IOException("Source metadata admission exhausted before decode");var value=metadata(id);metadata.put(id,value);images=Math.addExact(images,Math.multiplyExact(2,value.image().bytes()));}
            else if(suffix.equals("image") || suffix.equals("image.pending")){regular(file,imageLimits.maxImageBytes());if(suffix.equals("image.pending") && exists(path(id,"image")))throw new IOException("Uncertain duplicate source image staging");imageIds.add(id);}
            else if(suffix.equals("prepared.pending")){regular(file,MAX_METADATA_FILE);bytes=Math.addExact(bytes,3*Files.size(file));if(bytes>maximumMetadataBytes)throw new IOException("Source metadata staging quota exhausted");if(!exists(path(id,"prepared"))){if(!metadata.containsKey(id) && metadata.size()>=maximumArtifacts)throw new IOException("Source metadata staging count exhausted");var value=decodeMetadata(id,read(file,MAX_METADATA_FILE));metadata.put(id,value);images=Math.addExact(images,Math.multiplyExact(2,value.image().bytes()));}}
            else if(suffix.equals("released.pending")){regular(file,RELEASE_BODY_BYTES+32);bytes=Math.addExact(bytes,Files.size(file));}
            else throw new IOException("Uncertain source artifact file");
        }}
        for(UUID id:imageIds){
            var prepared=metadata.get(id);
            if(prepared==null){if(!exists(path(id,"released")))throw new IOException("Image has no forced source intent");images=Math.addExact(images,Math.multiplyExact(2,receipt(id).imageBytes()));}
            else {Path image=path(id,"image"),pending=path(id,"image.pending");if(exists(image) && Files.size(image)!=prepared.image().bytes() || exists(pending) && Files.size(pending)>prepared.image().bytes())throw new IOException("Owned image exceeded its forced size reservation");}
        }
        if(candidate!=null && !metadata.containsKey(candidate.request().transferId())){metadata.put(candidate.request().transferId(),candidate);images=Math.addExact(images,Math.multiplyExact(2,candidate.image().bytes()));bytes=Math.addExact(bytes,3L*(candidateBytes.length+32));}
        if(metadata.size()>maximumArtifacts || images>maximumImageBytes || bytes>maximumMetadataBytes)throw new IOException("Source artifact image/metadata quota exhausted");
    }
    private void publishImage(Path source,Path target,Prepared prepared)throws IOException {
        if(exists(target)){validateImage(prepared,target);SessionJournalFiles.force(target);SessionJournalFiles.force(target.getParent());return;}
        Path pending=target.resolveSibling(target.getFileName()+".pending");
        if(exists(pending)){regular(pending,prepared.image().bytes());requirePrefix(pending,source);Files.delete(pending);SessionJournalFiles.force(pending.getParent());}
        try{Files.createLink(pending,source);}catch(UnsupportedOperationException|IOException unavailable){if(exists(pending))throw new IOException("Uncertain source hardlink publication",unavailable);Files.copy(source,pending);}
        validateImage(prepared,pending);SessionJournalFiles.force(pending);Files.move(pending,target,StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(target.getParent());
    }
    private void validateImage(Prepared prepared,Path file)throws IOException {
        regular(file,prepared.image().bytes());if(Files.size(file)!=prepared.image().bytes())throw new IOException("Source image length changed");var actual=NativeRangeImage.inspect(file,imageLimits);var expected=prepared.image();
        if(!actual.owner().equals(expected.owner()) || !actual.interval().domainId().equals(expected.interval().domainId()) || !actual.interval().columns().equals(expected.interval().columns()) || !Arrays.equals(actual.interval().lowerInclusive(),expected.interval().lowerInclusive()) || !Arrays.equals(actual.interval().upperExclusive(),expected.interval().upperExclusive())
                || !actual.position().equals(expected.position()) || !Arrays.equals(actual.schemaDigest(),expected.schemaDigest()) || !actual.deterministicMergeColumns().equals(expected.deterministicMergeColumns()) || actual.rows()!=expected.rows() || actual.bytes()!=expected.bytes() || !Arrays.equals(actual.sha256(),expected.sha256())
                || !Arrays.equals(actual.topologyOrigin()==null?null:actual.topologyOrigin().encode(),expected.topologyOrigin()==null?null:expected.topologyOrigin().encode()))throw new IOException("Source image checksum/cut/schema/geometry changed");
    }
    private Prepared metadata(UUID id)throws IOException {return decodeMetadata(id,read(path(id,"prepared"),MAX_METADATA_FILE));}
    private static Prepared decodeMetadata(UUID id,byte[] bytes)throws IOException {try{var request=requestFromReply(bytes);var prepared=reply(bytes,PREPARE,request);if(!request.transferId().equals(id))throw new IOException("Source metadata filename identity changed");return prepared;}catch(IllegalArgumentException malformed){throw new IOException("Invalid forced source metadata",malformed);}}
    private static Request requestFromReply(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=PREPARE)throw new IllegalArgumentException("Expected source PREPARE metadata");var in=ByteBuffer.wrap(packet.body());if(in.remaining()<4)throw new IllegalArgumentException("Truncated source intent");int size=in.getInt();if(size<0 || size>MAX_REQUEST_BYTES || size>in.remaining())throw new IllegalArgumentException("Invalid source intent length");byte[] body=new byte[size];in.get(body);return request(ScopedTransactionRpc.frame(PREPARE,body));}
    private Receipt receipt(UUID id)throws IOException {var bytes=read(path(id,"released"),RELEASE_BODY_BYTES+32);if(bytes.length!=RELEASE_BODY_BYTES)throw new IOException("Invalid source release receipt size");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=RELEASE_MAGIC)throw new IOException("Invalid source release receipt header");UUID actual=new UUID(in.getLong(),in.getLong());byte[] request=new byte[32],metadata=new byte[32],image=new byte[32];in.get(request).get(metadata).get(image);long size=in.getLong();if(!actual.equals(id) || size<1 || size>imageLimits.maxImageBytes())throw new IOException("Invalid source release receipt identity/image bound");return new Receipt(id,request,metadata,image,size);}
    private static byte[] encode(Receipt value){return ByteBuffer.allocate(RELEASE_BODY_BYTES).putInt(RELEASE_MAGIC).putLong(value.id().getMostSignificantBits()).putLong(value.id().getLeastSignificantBits()).put(value.requestDigest()).put(value.metadataDigest()).put(value.imageDigest()).putLong(value.imageBytes()).array();}
    private static void requireReceipt(Request request,Receipt receipt)throws IOException{if(!Arrays.equals(receipt.requestDigest(),digest(request(RELEASE,request))))throw new IOException("Released source request changed");}
    private static void requireRequestIo(Request expected,Request actual)throws IOException{try{requireRequest(expected,actual);}catch(IllegalArgumentException changed){throw new IOException("Original source request changed",changed);}}
    private static void requirePrepared(Prepared expected,Prepared actual)throws IOException{if(!Arrays.equals(reply(PREPARE,expected),reply(PREPARE,actual)))throw new IOException("Original source image metadata changed");}
    private static void cleanMetadataPending(Path path,byte[] body)throws IOException{Path pending=path.resolveSibling(path.getFileName()+".pending");if(exists(pending)){regular(pending,MAX_METADATA_FILE);requirePrefixBytes(pending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());Files.delete(pending);SessionJournalFiles.force(path.getParent());}}
    private static void requirePrefix(Path partial,Path source)throws IOException{try(var left=FileChannel.open(partial,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS);var right=FileChannel.open(source,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){if(left.size()>right.size())throw new IOException("Uncertain staging image length");var a=ByteBuffer.allocate(65536);var b=ByteBuffer.allocate(65536);long offset=0;while(offset<left.size()){int count=(int)Math.min(a.capacity(),left.size()-offset);a.clear().limit(count);b.clear().limit(count);fill(left,a);fill(right,b);if(Arrays.mismatch(a.array(),0,count,b.array(),0,count)!=-1)throw new IOException("Uncertain staging image changed");offset+=count;}}}
    private static void requirePrefixBytes(Path partial,byte[] source)throws IOException{try(var file=FileChannel.open(partial,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){if(file.size()>source.length)throw new IOException("Uncertain metadata staging length");var bytes=ByteBuffer.allocate((int)file.size());fill(file,bytes);if(Arrays.mismatch(bytes.array(),0,bytes.capacity(),source,0,bytes.capacity())!=-1)throw new IOException("Uncertain metadata staging changed");}}
    private static void fill(FileChannel file,ByteBuffer bytes)throws IOException{while(bytes.hasRemaining())if(file.read(bytes)<0)throw new IOException("Truncated source artifact");}
    private static byte[] read(Path file,int maximum)throws IOException{regular(file,maximum);Path pending=file.resolveSibling(file.getFileName()+".pending");if(exists(pending))regular(pending,maximum);return SessionJournalFiles.read(file,maximum);}
    private static SnapshotInfo snapshot(Path path,Prepared prepared)throws IOException{return new SingleFileSnapshotInfo(new FileInfo(path,MD5FileUtil.computeMd5ForFile(path.toFile())),prepared.image().position().term(),prepared.image().position().index());}
    private static String name(Prepared prepared){return SimpleStateMachineStorage.getSnapshotFileName(prepared.image().position().term(),prepared.image().position().index());}
    private Path path(UUID id,String suffix){return root.resolve(id+"."+suffix);}
    private static boolean exists(Path path){return Files.exists(path,LinkOption.NOFOLLOW_LINKS);}
    private static void directory(Path path)throws IOException{for(Path part=path.toAbsolutePath().normalize();part!=null;part=part.getParent())if(Files.isSymbolicLink(part))throw new IOException("Source artifact directory crosses a symlink");if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source artifact directory is absent");}
    private static void regular(Path path,long maximum)throws IOException{directory(path.getParent());if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)>maximum)throw new IOException("Source artifact is foreign/symlink/oversized");}
    private static byte[] digest(byte[] body){try{return MessageDigest.getInstance("SHA-256").digest(body);}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private void requireOpen()throws IOException{if(closed || !lock.isValid())throw new IOException("Source artifact owner closed/fenced");}
    @Override public synchronized void close()throws IOException{if(closed)return;closed=true;try{lock.release();}finally{lockChannel.close();}}
}
