package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.common.distributed.*;
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
    private static final int RELEASE_MAGIC=0x54534152,RELEASE_BODY_BYTES=148,MAX_RELEASED=1024;
    private static final int MAX_METADATA_FILE=MAX_REPLY_BYTES+32;
    public record Saved(Prepared prepared,Path image,SnapshotInfo snapshot) {}
    private record Receipt(UUID id,byte[] requestDigest,byte[] metadataDigest,byte[] imageDigest,long imageBytes,long reservedImageBytes,ApplyPosition cut) {}
    private final Path root;private final NativeRangeImage.Limits imageLimits;
    private final long maximumImageBytes,maximumMetadataBytes;private final int maximumArtifacts;
    private final FileChannel lockChannel;private final FileLock lock;private SessionJournalFiles.Removal removal;
    @FunctionalInterface interface CaptureForceObserver{void forced(Path path)throws IOException;}
    private CaptureForceObserver captureForceObserver=path->{};
    synchronized void captureForceObserver(CaptureForceObserver observer){captureForceObserver=Objects.requireNonNull(observer);}
    synchronized void removalObserver(SessionJournalFiles.Removal observer){removal=Objects.requireNonNull(observer);}
    public static final class AdmissionRejected extends IOException{AdmissionRejected(String message){super(message);}}
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
    private record Reservation(Request request,long maximumImageBytes) {}
    private static final int MAX_RESERVATION_FILE=MAX_REQUEST_BYTES+44;
    private Reservation reservation(UUID id)throws IOException{var bytes=read(path(id,"reserved"),MAX_RESERVATION_FILE);try{var in=ByteBuffer.wrap(bytes);long maximum=in.getLong();int length=in.getInt();if(maximum<1 || maximum>imageLimits.maxImageBytes() || length<1 || length!=in.remaining())throw new IOException("Invalid source disk reservation");byte[] requestBytes=new byte[length];in.get(requestBytes);var request=request(requestBytes);if(!request.transferId().equals(id))throw new IOException("Source reservation identity changed");return new Reservation(request,maximum);}catch(IllegalArgumentException malformed){throw new IOException("Invalid source disk reservation",malformed);}}
    private byte[] reservationBytes(Request request){byte[] encoded=request(PREPARE,request);return ByteBuffer.allocate(12+encoded.length).putLong(imageLimits.maxImageBytes()).putInt(encoded.length).put(encoded).array();}
    /** Durable inventory admission precedes source pin acquisition and image materialization. */
    public synchronized void reserve(Request request)throws IOException {
        requireOpen();UUID id=request.transferId();if(exists(path(id,"released")))throw new AdmissionRejected("Source transfer already has durable release authority");
        var file=path(id,"reserved");if(exists(file)){var prior=reservation(id);if(!Arrays.equals(request(PREPARE,request),request(PREPARE,prior.request())))throw new AdmissionRejected("Original source reservation request changed");SessionJournalFiles.force(file);SessionJournalFiles.force(root);inventory(null,null);return;}
        var usage=inventoryUsage();long imageCharge=Math.multiplyExact(2,imageLimits.maxImageBytes()),metadataCharge=6L*MAX_METADATA_FILE+3L*(reservationBytes(request).length+32);
        if(usage.count()>=maximumArtifacts || imageCharge>maximumImageBytes-usage.imageBytes() || metadataCharge>maximumMetadataBytes-usage.metadataBytes())throw new AdmissionRejected("Durable source image/metadata reservation exhausted");
        SessionJournalFiles.write(file,reservationBytes(request),MAX_RESERVATION_FILE);inventory(null,null);
    }
    public synchronized long reservedImageBytes()throws IOException{requireOpen();return inventoryUsage().imageBytes();}
    private record Capture(Request request,byte[] lower,byte[] upper,ApplyPosition cut,DistributedSchema schema,ReplicaDescriptor owner,ReplicatedRangeStore.Configuration configuration,Map<String,byte[]> rows,byte[] origin) {}
    public synchronized void capture(Request request,TopologyChange.Definition definition,ReplicatedRangeStore.SnapshotPin pin,DistributedSchema schema,ReplicaDescriptor owner)throws Exception {
        var reserved=reservation(request.transferId());requireRequestIo(request,reserved.request());request.header().requireDefinition(definition);var parent=definition.parents().getFirst();if(!pin.groupId().equals(parent.groupId()) || pin.rangeEpoch()!=parent.rangeEpoch() || !Arrays.equals(pin.interval().lowerInclusive(),parent.lowerInclusive()) || !Arrays.equals(pin.interval().upperExclusive(),parent.upperExclusive()) || !pin.interval().domainId().equals(definition.sourceDirectory().domainId()) || !pin.interval().columns().equals(definition.sourceDirectory().columns()) || !Arrays.equals(pin.schemaDigest(),schema.digest()))throw new IOException("Source capture pin changed original range/schema");var rows=new TreeMap<String,byte[]>();pin.visitState((key,value)->{if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value.clone());});
        var origin=pin.topologyImageOrigin();var capture=new Capture(request,pin.interval().lowerInclusive(),pin.interval().upperExclusive(),pin.position(),schema,owner,pin.configuration(),NativeTopologyLearnerSchema.copy(rows),origin==null?new byte[32]:digest(origin.encode()));byte[] encoded=encodeCapture(capture);var file=path(request.transferId(),"capture");
        if(exists(file)){if(!Arrays.equals(read(file,MAX_METADATA_FILE),encoded))throw new IOException("Original source capture cut/configuration/schema changed");SessionJournalFiles.force(file);SessionJournalFiles.force(root);}else{SessionJournalFiles.write(file,encoded,MAX_METADATA_FILE);}
        captureForceObserver.forced(file);
    }
    private static byte[] encodeCapture(Capture capture)throws IOException {
        var header=capture.request().header();var parent=header.parents().getFirst();var cfg=capture.configuration();var cut=capture.cut();
        if(cut==null || cut.term()<1 || cfg==null || cfg.term()>cut.term() || cfg.index()>cut.index() || cfg.index()==cut.index()&&cfg.term()!=cut.term() || !capture.schema().clusterId().equals(header.clusterId()) || !capture.schema().databaseId().equals(header.databaseId()) || !capture.schema().domainId().equals(header.domainId()) || capture.schema().schemaEpoch()!=header.schemaEpoch() || !Arrays.equals(capture.schema().digest(),header.schemaDigest()) || !parent.owner(capture.owner().replicaId()).equals(TopologyChange.Owner.of(capture.owner())) || capture.owner().role()!=ReplicaDescriptor.Role.VOTER || capture.origin().length!=32)throw new IOException("Capture intent differs from source owner/schema/native prefix");
        var buffer=new ByteArrayOutputStream();var out=new DataOutputStream(buffer);out.writeInt(0x53434150);blob(out,request(PREPARE,capture.request()));nullable(out,capture.lower());nullable(out,capture.upper());out.writeLong(capture.cut().term());out.writeLong(capture.cut().index());blob(out,DistributedSchemaCodec.encode(capture.schema()));var owner=capture.owner();uuid(out,owner.nodeId());uuid(out,owner.nodeIncarnation());uuid(out,owner.replicaId());out.writeUTF(owner.peerId());out.writeUTF(owner.address());out.writeByte(owner.role().ordinal());out.writeLong(cfg.term());out.writeLong(cfg.index());blob(out,cfg.data());out.write(capture.origin());out.writeInt(capture.rows().size());for(var row:new TreeMap<>(capture.rows()).entrySet()){out.writeUTF(row.getKey());blob(out,row.getValue());if(buffer.size()>MAX_REPLY_BYTES)throw new IOException("Source capture intent exceeds bound");}out.flush();if(buffer.size()>MAX_REPLY_BYTES)throw new IOException("Source capture intent exceeds bound");return buffer.toByteArray();
    }
    private Capture capture(UUID id)throws IOException {
        var result=decodeCapture(id,read(path(id,"capture"),MAX_METADATA_FILE));SessionJournalFiles.force(path(id,"capture"));SessionJournalFiles.force(root);return result;
    }
    private static Capture decodeCapture(UUID id,byte[] bytes)throws IOException{try{var in=new DataInputStream(new ByteArrayInputStream(bytes));if(in.readInt()!=0x53434150)throw new IOException("Invalid source capture intent");var request=request(blob(in,MAX_REQUEST_BYTES));byte[] lower=nullable(in),upper=nullable(in);var cut=new ApplyPosition(in.readLong(),in.readLong());var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES));UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in);String peer=in.readUTF(),address=in.readUTF();int role=in.readUnsignedByte();if(role>=ReplicaDescriptor.Role.values().length)throw new IOException("Invalid source capture role");var owner=new ReplicaDescriptor(node,incarnation,replica,peer,address,ReplicaDescriptor.Role.values()[role]);var cfg=new ReplicatedRangeStore.Configuration(in.readLong(),in.readLong(),blob(in,65536));byte[] origin=in.readNBytes(32);int count=in.readInt();if(count<0 || count>NativeTopologyLearnerSchema.MAX_ROWS)throw new IOException("Invalid source capture history count");var rows=new TreeMap<String,byte[]>();long total=0;for(int n=0;n<count;n++){String key=in.readUTF();byte[] value=blob(in,NativeTopologyLearnerSchema.MAX_BYTES);if((total+=value.length)>NativeTopologyLearnerSchema.MAX_BYTES || rows.put(key,value)!=null)throw new IOException("Invalid source capture history");}var result=new Capture(request,lower,upper,cut,schema,owner,cfg,NativeTopologyLearnerSchema.copy(rows),origin);if(!request.transferId().equals(id) || in.available()!=0 || !Arrays.equals(bytes,encodeCapture(result)))throw new IOException("Source capture intent changed");return result;}catch(IllegalArgumentException malformed){throw new IOException("Invalid source capture intent",malformed);}
    }
    private static void nullable(DataOutputStream out,byte[] bound)throws IOException{if(bound==null)out.writeInt(-1);else{if(bound.length>MAX_BOUND_BYTES)throw new IOException("Source capture bound exceeds limit");blob(out,bound);}}
    private static byte[] nullable(DataInputStream in)throws IOException{int length=in.readInt();if(length==-1)return null;if(length<0 || length>MAX_BOUND_BYTES || length>in.available())throw new IOException("Invalid source capture bound");return in.readNBytes(length);}
    private static void blob(DataOutputStream out,byte[] bytes)throws IOException{out.writeInt(bytes.length);out.write(bytes);}private static byte[] blob(DataInputStream in,int maximum)throws IOException{int size=in.readInt();if(size<0 || size>maximum || size>in.available())throw new IOException("Invalid source capture field");return in.readNBytes(size);}private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    public synchronized Saved lookup(Request request)throws IOException {
        requireOpen();UUID id=request.transferId();
        if(exists(path(id,"released"))){var receipt=receipt(id);requireReceipt(request,receipt);cleanup(receipt);throw new IOException("Source transfer was durably released");}
        if(!exists(path(id,"capture")) && exists(path(id,"capture.pending"))){var pending=path(id,"capture.pending");var original=decodeCapture(id,read(pending,MAX_METADATA_FILE));requireRequestIo(request,original.request());SessionJournalFiles.force(pending);Files.move(pending,path(id,"capture"),StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(path(id,"capture"));SessionJournalFiles.force(root);}
        if(!exists(path(id,"prepared")) && exists(path(id,"prepared.pending"))){
            var pending=path(id,"prepared.pending");var prepared=decodeMetadata(id,read(pending,MAX_METADATA_FILE));requireRequestIo(request,prepared.request());inventory(null,null);Files.move(pending,path(id,"prepared"),StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(path(id,"prepared"));SessionJournalFiles.force(root);
        }
        if(!exists(path(id,"prepared")) && exists(path(id,"capture"))){var original=capture(id);requireRequestIo(request,original.request());var file=sourcePath(id,original.cut());if(exists(file)){var image=NativeRangeImage.inspect(file,imageLimits);byte[] origin=image.topologyOrigin()==null?new byte[32]:digest(image.topologyOrigin().encode());if(!image.position().equals(original.cut()) || !Arrays.equals(image.interval().lowerInclusive(),original.lower()) || !Arrays.equals(image.interval().upperExclusive(),original.upper()) || !Arrays.equals(origin,original.origin()))throw new IOException("Original capture image/provenance changed");var prepared=new Prepared(request,image,original.schema(),original.owner(),original.configuration(),original.rows());retain(prepared,snapshot(file,prepared));}}
        if(!exists(path(id,"prepared"))){if(exists(path(id,"image")) || exists(path(id,"image.pending")) || exists(path(id,"prepared.pending")))throw new IOException("Unbound or uncertain source artifact cannot be recaptured");return null;}
        var prepared=metadata(id);requireRequestIo(request,prepared.request());inventory(null,null);cleanMetadataPending(path(id,"prepared"),reply(PREPARE,prepared));
        Path image=path(id,"image");if(!exists(image)){
            Path source=sourcePath(id,prepared.image().position());if(!exists(source))throw new IOException("Forced source intent has no original image; exact repair required");
            validateImage(prepared,source);publishImage(source,image,prepared);
        }
        validateImage(prepared,image);var saved=new Saved(prepared,image,snapshot(image,prepared));return new Saved(prepared,image,restoreOriginal(saved));
    }
    public synchronized Saved retain(Prepared prepared,SnapshotInfo snapshot)throws IOException {
        requireOpen();Objects.requireNonNull(prepared);Objects.requireNonNull(snapshot);reserve(prepared.request());UUID id=prepared.request().transferId();
        if(exists(path(id,"released"))){requireReceipt(prepared.request(),receipt(id));throw new IOException("Released source transfer cannot be retained again");}
        if(snapshot.getFiles().size()!=1 || snapshot.getTerm()!=prepared.image().position().term() || snapshot.getIndex()!=prepared.image().position().index())throw new IOException("Source snapshot identity changed");
        Path source=snapshot.getFiles().getFirst().getPath().toAbsolutePath().normalize();
        regular(source,prepared.image().bytes());validateImage(prepared,source);MD5Hash md5=MD5FileUtil.computeMd5ForFile(source.toFile());var declared=snapshot.getFiles().getFirst().getFileDigest();if(declared!=null && !declared.equals(md5))throw new IOException("Source snapshot MD5 changed");
        byte[] encoded=reply(PREPARE,prepared);inventory(prepared,encoded);
        Path metadata=path(id,"prepared");if(exists(metadata)){byte[] prior=read(metadata,MAX_METADATA_FILE);if(!Arrays.equals(prior,encoded))throw new IOException("Original forced source metadata changed");}
        else {
            if(exists(path(id,"image")) || exists(path(id,"image.pending")))throw new IOException("Foreign image already uses transfer identity");
            cleanMetadataPending(metadata,encoded);SessionJournalFiles.write(metadata,encoded,MAX_METADATA_FILE);
        }
        cleanMetadataPending(metadata,encoded);var original=sourcePath(id,prepared.image().position());ensureOriginalDirectory(id);publishImage(source,original,prepared);publishImage(original,path(id,"image"),prepared);validateImage(prepared,path(id,"image"));
        return new Saved(prepared,path(id,"image"),new SingleFileSnapshotInfo(new FileInfo(original,md5),snapshot.getTerm(),snapshot.getIndex()));
    }
    private Path sourcePath(UUID id,ApplyPosition cut){return root.resolve(id+".source").resolve(SimpleStateMachineStorage.getSnapshotFileName(cut.term(),cut.index()));}
    private void ensureOriginalDirectory(UUID id)throws IOException {
        var owned=root.resolve(id+".source");directory(root);
        if(!Files.exists(owned,LinkOption.NOFOLLOW_LINKS))Files.createDirectory(owned);
        directory(owned);SessionJournalFiles.force(owned);SessionJournalFiles.force(root);
    }
    public synchronized Path originalImagePath(Request request,ApplyPosition cut)throws IOException {
        var original=capture(request.transferId());requireRequestIo(request,original.request());if(!cut.equals(original.cut()))throw new IOException("Source original cut changed");var file=sourcePath(request.transferId(),cut);ensureOriginalDirectory(request.transferId());return file;
    }
    private SnapshotInfo restoreOriginal(Saved saved)throws IOException {
        requireOpen();var prepared=saved.prepared();requirePrepared(metadata(prepared.request().transferId()),prepared);if(exists(path(prepared.request().transferId(),"released")))throw new IOException("Released source image cannot be restored");
        Path target=sourcePath(prepared.request().transferId(),prepared.image().position());ensureOriginalDirectory(prepared.request().transferId());publishImage(saved.image(),target,prepared);return snapshot(target,prepared);
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
            byte[] encoded=reply(PREPARE,prepared);receipt=new Receipt(id,digest(request(RELEASE,request)),digest(encoded),prepared.image().sha256(),prepared.image().bytes(),reservation(id).maximumImageBytes(),prepared.image().position());
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
        var source=sourcePath(receipt.id(),receipt.cut());var sourceDir=source.getParent();if(Files.exists(sourceDir,LinkOption.NOFOLLOW_LINKS)){
            if(!Files.isDirectory(sourceDir,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source original namespace changed");
            try(var files=Files.newDirectoryStream(sourceDir)){for(var file:files){var name=file.getFileName().toString();if(!name.equals(source.getFileName().toString()) && !name.equals(source.getFileName()+".pending"))throw new IOException("Unsettled source build prevents cleanup");regular(file,receipt.imageBytes());if(name.endsWith(".pending")){if(!exists(source))throw new IOException("Partial original source image lacks settled original");requirePrefix(file,source);}else{var imageManifest=NativeRangeImage.inspect(file,imageLimits);if(!Arrays.equals(imageManifest.sha256(),receipt.imageDigest()) || !imageManifest.position().equals(receipt.cut()))throw new IOException("Source original changed before cleanup");}removal.delete(file);}}
            removal.delete(sourceDir);
        }
        if(exists(metadata))removal.delete(metadata);for(var suffix:List.of("capture.pending","capture","reserved.pending","reserved")){var file=path(receipt.id(),suffix);if(exists(file))removal.delete(file);}removal.force(root);
        SessionJournalFiles.write(path(receipt.id(),"settled"),digest(encode(receipt)),64);
    }
    private record Usage(int count,long imageBytes,long metadataBytes) {}
    private long metadataUsage()throws IOException{return inventoryUsage().metadataBytes();}
    private void inventory(Prepared candidate,byte[] candidateBytes)throws IOException{inventoryUsage(candidate,candidateBytes);}
    private Usage inventoryUsage()throws IOException{return inventoryUsage(null,null);}
    private Usage inventoryUsage(Prepared candidate,byte[] candidateBytes)throws IOException {
        var reservations=new HashMap<UUID,Reservation>();var prepared=new HashMap<UUID,Prepared>();var captures=new HashMap<UUID,Capture>();var receipts=new HashMap<UUID,Receipt>();var settled=new HashSet<UUID>();var imageIds=new HashSet<UUID>();var sourceIds=new HashSet<UUID>();long metadata=0;int files=0;
        try(var entries=Files.newDirectoryStream(root)){for(var file:entries){if(++files>2*MAX_RELEASED+maximumArtifacts*9+1)throw new IOException("Source artifact directory quota exceeded");String name=file.getFileName().toString();if(name.equals("artifacts.lock")){regular(file,0);continue;}int dot=name.indexOf('.');if(dot!=36)throw new IOException("Foreign source artifact path");UUID id;try{id=UUID.fromString(name.substring(0,dot));}catch(IllegalArgumentException invalid){throw new IOException("Foreign source artifact identity",invalid);}if(!id.toString().equals(name.substring(0,dot)))throw new IOException("Noncanonical source artifact identity");String suffix=name.substring(dot+1);
            switch(suffix){
                case "source"->{if(!Files.isDirectory(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid source original directory");sourceIds.add(id);}
                case "reserved"->{regular(file,MAX_RESERVATION_FILE);reservations.put(id,reservation(id));metadata=Math.addExact(metadata,3L*Files.size(file));}
                case "capture"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source capture metadata quota exhausted before decode");captures.put(id,capture(id));}
                case "prepared"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source metadata quota exhausted before decode");prepared.put(id,metadata(id));}
                case "image","image.pending"->{regular(file,imageLimits.maxImageBytes());if(suffix.equals("image.pending")&&exists(path(id,"image")))throw new IOException("Uncertain duplicate source image staging");imageIds.add(id);}
                case "released"->{regular(file,RELEASE_BODY_BYTES+32);receipts.put(id,receipt(id));metadata=Math.addExact(metadata,Files.size(file));}
                case "settled"->{regular(file,64);settled.add(id);metadata=Math.addExact(metadata,Files.size(file));}
                case "prepared.pending"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source metadata staging quota exhausted");if(!exists(path(id,"prepared")))prepared.put(id,decodeMetadata(id,read(file,MAX_METADATA_FILE)));}
                case "capture.pending"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source capture staging quota exhausted");if(!exists(path(id,"capture")))captures.put(id,decodeCapture(id,read(file,MAX_METADATA_FILE)));}
                case "reserved.pending"->throw new IOException("Uncertain disk reservation requires exact force repair");
                case "released.pending","settled.pending"->{regular(file,RELEASE_BODY_BYTES+32);metadata=Math.addExact(metadata,Files.size(file));}
                default->throw new IOException("Uncertain source artifact file");
            }
        }}
        if(candidate!=null&&!prepared.containsKey(candidate.request().transferId())){prepared.put(candidate.request().transferId(),candidate);metadata=Math.addExact(metadata,3L*(candidateBytes.length+32));}
        long images=0;var retained=new HashSet<UUID>();
        for(var entry:reservations.entrySet()){
            UUID id=entry.getKey();var reservation=entry.getValue();var value=prepared.get(id);var capture=captures.get(id);var receipt=receipts.get(id);
            if(value!=null){requireRequestIo(reservation.request(),value.request());if(value.image().bytes()>reservation.maximumImageBytes())throw new IOException("Source image exceeded durable promise");}
            if(capture!=null)requireRequestIo(reservation.request(),capture.request());if(receipt!=null)requireReceipt(reservation.request(),receipt);
            if(settled.contains(id))throw new IOException("Settled source transfer retained an unfinished disk promise");retained.add(id);images=Math.addExact(images,2L*reservation.maximumImageBytes());if(value==null&&receipt==null)metadata=Math.addExact(metadata,3L*MAX_METADATA_FILE+(capture==null?3L*MAX_METADATA_FILE:0));
        }
        for(var entry:receipts.entrySet()){
            UUID id=entry.getKey();if(settled.contains(id)){if(!Arrays.equals(read(path(id,"settled"),64),digest(encode(entry.getValue()))))throw new IOException("Settled source release changed");SessionJournalFiles.force(path(id,"settled"));SessionJournalFiles.force(root);if(prepared.containsKey(id)||captures.containsKey(id)||imageIds.contains(id)||sourceIds.contains(id))throw new IOException("Settled source transfer retained owned artifacts");}
            else if(!retained.contains(id)){retained.add(id);images=Math.addExact(images,2L*entry.getValue().reservedImageBytes());}
        }
        for(var id:prepared.keySet())if(!reservations.containsKey(id)&&!receipts.containsKey(id))throw new IOException("Source image lacks durable disk reservation");for(var id:captures.keySet())if(!reservations.containsKey(id)&&!receipts.containsKey(id))throw new IOException("Source capture lacks durable disk reservation");for(var id:settled)if(!receipts.containsKey(id))throw new IOException("Source settled marker lacks release authority");
        for(var id:imageIds){var value=prepared.get(id);if(value==null&&!receipts.containsKey(id))throw new IOException("Image lacks forced original source metadata");long expected=value==null?receipts.get(id).imageBytes():value.image().bytes();if(exists(path(id,"image"))&&Files.size(path(id,"image"))!=expected || exists(path(id,"image.pending"))&&Files.size(path(id,"image.pending"))>expected)throw new IOException("Owned source image exceeds bound");}
        for(var id:sourceIds){var value=prepared.get(id);var captured=captures.get(id);var receipt=receipts.get(id);ApplyPosition cut=value!=null?value.image().position():captured!=null?captured.cut():receipt!=null?receipt.cut():null;if(cut==null)throw new IOException("Original source directory lacks forced cut");var expected=sourcePath(id,cut).getFileName().toString();int count=0;try(var filesInSource=Files.newDirectoryStream(sourcePath(id,cut).getParent())){for(var file:filesInSource){if(++count>4)throw new IOException("Source original file count exceeded");var name=file.getFileName().toString();if(!name.equals(expected)&&!name.equals(expected+".pending")&&!name.matches("[.]native-range-building-.*[.]tmp"))throw new IOException("Foreign source original file");regular(file,imageLimits.maxImageBytes());}}}
        if(receipts.size()>MAX_RELEASED||retained.size()>maximumArtifacts||images>maximumImageBytes||metadata>maximumMetadataBytes)throw new IOException("Source durable image/metadata quota exhausted");return new Usage(retained.size(),images,metadata);
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
    private Prepared metadata(UUID id)throws IOException {var result=decodeMetadata(id,read(path(id,"prepared"),MAX_METADATA_FILE));SessionJournalFiles.force(path(id,"prepared"));SessionJournalFiles.force(root);return result;}
    private static Prepared decodeMetadata(UUID id,byte[] bytes)throws IOException {try{var request=requestFromReply(bytes);var prepared=reply(bytes,PREPARE,request);if(!request.transferId().equals(id))throw new IOException("Source metadata filename identity changed");return prepared;}catch(IllegalArgumentException malformed){throw new IOException("Invalid forced source metadata",malformed);}}
    private static Request requestFromReply(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=PREPARE)throw new IllegalArgumentException("Expected source PREPARE metadata");var in=ByteBuffer.wrap(packet.body());if(in.remaining()<4)throw new IllegalArgumentException("Truncated source intent");int size=in.getInt();if(size<0 || size>MAX_REQUEST_BYTES || size>in.remaining())throw new IllegalArgumentException("Invalid source intent length");byte[] body=new byte[size];in.get(body);return request(ScopedTransactionRpc.frame(PREPARE,body));}
    private Receipt receipt(UUID id)throws IOException {var bytes=read(path(id,"released"),RELEASE_BODY_BYTES+32);if(bytes.length!=RELEASE_BODY_BYTES)throw new IOException("Invalid source release receipt size");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=RELEASE_MAGIC)throw new IOException("Invalid source release receipt header");UUID actual=new UUID(in.getLong(),in.getLong());byte[] request=new byte[32],metadata=new byte[32],image=new byte[32];in.get(request).get(metadata).get(image);long size=in.getLong(),reserved=in.getLong();var cut=new ApplyPosition(in.getLong(),in.getLong());if(!actual.equals(id) || size<1 || size>imageLimits.maxImageBytes() || reserved<size || reserved>imageLimits.maxImageBytes())throw new IOException("Invalid source release receipt identity/image bound");SessionJournalFiles.force(path(id,"released"));SessionJournalFiles.force(root);return new Receipt(id,request,metadata,image,size,reserved,cut);}
    private static byte[] encode(Receipt value){return ByteBuffer.allocate(RELEASE_BODY_BYTES).putInt(RELEASE_MAGIC).putLong(value.id().getMostSignificantBits()).putLong(value.id().getLeastSignificantBits()).put(value.requestDigest()).put(value.metadataDigest()).put(value.imageDigest()).putLong(value.imageBytes()).putLong(value.reservedImageBytes()).putLong(value.cut().term()).putLong(value.cut().index()).array();}
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
