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
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Forced transfer-ID artifacts. Closing a live/process owner preserves files; only a durable terminal disposition reclaims them. */
public final class RatisTopologySourceSnapshotArtifacts implements AutoCloseable {
    private static final int RELEASE_MAGIC=0x54534152,RELEASE_BODY_BYTES=149,MAX_RELEASED=1024;
    private static final long RELEASE_METADATA_PROMISE=2L*(RELEASE_BODY_BYTES+32)+64;
    private static final int MAX_METADATA_FILE=MAX_REPLY_BYTES+32,MAX_PUBLICATION_FILE=MAX_REQUEST_BYTES+MAX_OWNER_BYTES+181+NativeTopologyFreeze.MAX_BYTES;
    public record Saved(Prepared prepared,Path image,SnapshotInfo snapshot) {}
    private record Receipt(UUID id,byte[] requestDigest,byte[] metadataDigest,byte[] imageDigest,long imageBytes,long reservedImageBytes,ApplyPosition cut,boolean metadataOnly) {}
    private static final int OBSOLETE_MAGIC=0x534f4253,MAX_OBSOLETE_FILE=RatisRecoveryObsoleteRpc.MAX_REQUEST_BYTES+65;
    private record Obsolete(RatisRecoveryObsoleteRpc.Request request,long reservedImageBytes,ApplyPosition cut) {}
    private final Map<UUID,Obsolete> pendingObsolete=new HashMap<>();
    @FunctionalInterface interface ObsoleteForceObserver{void boundary(boolean after)throws IOException;}
    private ObsoleteForceObserver obsoleteForceObserver=after->{};
    synchronized void obsoleteForceObserver(ObsoleteForceObserver observer){obsoleteForceObserver=Objects.requireNonNull(observer);}
    private final Path root;private final NativeRangeImage.Limits imageLimits;private final NativeRangeImage.IoAdmission metadataAdmission;
    private final long maximumImageBytes,maximumMetadataBytes;private final int maximumArtifacts;
    private final FileChannel lockChannel;private final FileLock lock;private SessionJournalFiles.Removal removal;
    @FunctionalInterface interface CaptureForceObserver{void forced(Path path)throws IOException;}
    private CaptureForceObserver captureForceObserver=path->{};
    synchronized void captureForceObserver(CaptureForceObserver observer){captureForceObserver=Objects.requireNonNull(observer);}
    synchronized void removalObserver(SessionJournalFiles.Removal observer){removal=Objects.requireNonNull(observer);}
    public static final class AdmissionRejected extends IOException{AdmissionRejected(String message){super(message);}}
    private volatile boolean closed;
    public enum PublicationPhase { CAPTURING,READY,ABANDONED }
    private record Publication(PublicationPhase phase,Request request,ApplyPosition cut,ReplicaDescriptor owner,byte[] configurationDigest,byte[] captureDigest,byte[] preparedDigest,UUID successor,long maximumImageBytes) {}
    @FunctionalInterface interface PublicationForceObserver{void forced(PublicationPhase phase)throws IOException;}
    private PublicationForceObserver publicationForceObserver=phase->{};
    synchronized void publicationForceObserver(PublicationForceObserver observer){publicationForceObserver=Objects.requireNonNull(observer);}
    public static final class OriginalCutUnavailable extends IOException{OriginalCutUnavailable(){super("Unpublished source capture lost its original native cut");}}
    public RatisTopologySourceSnapshotArtifacts(Path root,NativeRangeImage.Limits imageLimits,long maximumImageBytes,long maximumMetadataBytes,int maximumArtifacts)throws IOException {
        this(root,imageLimits,maximumImageBytes,maximumMetadataBytes,maximumArtifacts,SessionJournalFiles.FILESYSTEM);
    }
    public RatisTopologySourceSnapshotArtifacts(Path root,NativeRangeImage.Limits imageLimits,long maximumImageBytes,long maximumMetadataBytes,int maximumArtifacts,NativeRangeImage.IoAdmission metadataAdmission)throws IOException {
        this(root,imageLimits,maximumImageBytes,maximumMetadataBytes,maximumArtifacts,SessionJournalFiles.FILESYSTEM,metadataAdmission);
    }
    RatisTopologySourceSnapshotArtifacts(Path root,NativeRangeImage.Limits imageLimits,long maximumImageBytes,long maximumMetadataBytes,int maximumArtifacts,SessionJournalFiles.Removal removal)throws IOException {
        this(root,imageLimits,maximumImageBytes,maximumMetadataBytes,maximumArtifacts,removal,NativeRangeImage.IoAdmission.UNLIMITED);
    }
    private RatisTopologySourceSnapshotArtifacts(Path ownedRoot,NativeRangeImage.Limits imageLimits,long maximumImageBytes,long maximumMetadataBytes,int maximumArtifacts,SessionJournalFiles.Removal removal,NativeRangeImage.IoAdmission metadataAdmission)throws IOException {
        this.metadataAdmission=Objects.requireNonNull(metadataAdmission);
        this.root=ownedRoot.toAbsolutePath().normalize();directory(root);this.imageLimits=Objects.requireNonNull(imageLimits);this.removal=Objects.requireNonNull(removal);
        if(maximumImageBytes<1 || maximumMetadataBytes<1 || maximumArtifacts<1 || maximumArtifacts>4)throw new IllegalArgumentException("Invalid source artifact quota");
        this.maximumImageBytes=maximumImageBytes;this.maximumMetadataBytes=maximumMetadataBytes;this.maximumArtifacts=maximumArtifacts;
        Path lockPath=root.resolve("artifacts.lock");if(Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS))regular(lockPath,0);
        lockChannel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try{try{acquired=lockChannel.tryLock();}catch(OverlappingFileLockException busy){throw new IOException("Source artifact directory already owned",busy);}if(acquired==null)throw new IOException("Source artifact directory already owned");recoverPublications();recoverRecoveryReservations();inventory(null,null);}
        catch(IOException|RuntimeException failure){try{if(acquired!=null)acquired.release();}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{lockChannel.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}lock=acquired;
    }
    private void recoverPublications()throws IOException {
        var committed=new HashMap<UUID,Publication>();var pending=new ArrayList<Path>();int count=0;
        try(var files=Files.newDirectoryStream(root)){for(var file:files){if(++count>7*MAX_RELEASED+maximumArtifacts*12+1)throw new IOException("Source artifact directory quota exceeded");String name=file.getFileName().toString();if(!name.endsWith(".publication")&&!name.endsWith(".publication.pending"))continue;int dot=name.indexOf('.');if(dot!=36)throw new IOException("Invalid source publication filename");UUID id;try{id=UUID.fromString(name.substring(0,dot));}catch(IllegalArgumentException bad){throw new IOException("Invalid source publication identity",bad);}if(!id.toString().equals(name.substring(0,dot)))throw new IOException("Noncanonical source publication identity");regular(file,MAX_PUBLICATION_FILE);if(name.endsWith(".pending"))pending.add(file);else{var value=decodePublication(read(file,MAX_PUBLICATION_FILE));if(!id.equals(value.request().transferId()))throw new IOException("Source publication filename changed");committed.put(id,value);}}}
        // Committed phases are validated before any unpublished bytes can be discarded.
        for(var file:pending){UUID id=UUID.fromString(file.getFileName().toString().substring(0,36));byte[] encoded=Files.readAllBytes(file);if(encoded.length>MAX_PUBLICATION_FILE)throw new IOException("Source publication sidecar exceeds bound");boolean complete=encoded.length>=32;byte[] body=complete?Arrays.copyOf(encoded,encoded.length-32):new byte[0];if(complete)complete=java.security.MessageDigest.isEqual(digest(body),Arrays.copyOfRange(encoded,body.length,encoded.length));
            if(!complete){removal.delete(file);removal.force(root);continue;}
            var next=decodePublication(body);if(!id.equals(next.request().transferId()))throw new IOException("Pending source publication identity changed");var before=committed.get(id);requirePublicationTransition(before,next);SessionJournalFiles.force(file);Files.move(file,path(id,"publication"),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);SessionJournalFiles.force(root);committed.put(id,next);
        }
    }
    /** Complete checksummed recovery promises are inventoried before any namespace publication. */
    private void recoverRecoveryReservations()throws IOException {
        inventory(null,null);
        try(var files=Files.newDirectoryStream(root,"*.recovery-reserved.pending")){for(var pending:files){
            var name=pending.getFileName().toString();var canonical=pending.resolveSibling(name.substring(0,name.length()-".pending".length()));
            byte[] promised=recoveryReservation(pending);
            if(exists(canonical) && !Arrays.equals(promised,recoveryReservation(canonical)))throw new IOException("Recovery reservation sidecar conflicts with original promise");
            SessionJournalFiles.force(pending);
            if(exists(canonical)){SessionJournalFiles.force(canonical);removal.delete(pending);}
            else Files.move(pending,canonical,StandardCopyOption.ATOMIC_MOVE);
            SessionJournalFiles.force(canonical);removal.force(root);
        }}
        boolean retained=false;try(var files=Files.newDirectoryStream(root,"*.recovery-reserved")){for(var canonical:files){SessionJournalFiles.force(canonical);retained=true;}}
        if(retained)SessionJournalFiles.force(root);
    }
    private byte[] recoveryReservation(Path file)throws IOException {
        regular(file,40);byte[] value=read(file,40);
        if(value.length!=8 || ByteBuffer.wrap(value).getLong()!=imageLimits.maxImageBytes())throw new IOException("Recovery image reservation changed");return value;
    }
    private static void requirePublicationTransition(Publication before,Publication next)throws IOException {
        if(before==null){if(next.phase()!=PublicationPhase.CAPTURING)throw new IOException("Terminal source sidecar lacks original capture authority");return;}
        if(Arrays.equals(encodePublication(before),encodePublication(next)))return;
        if(before.phase()!=PublicationPhase.CAPTURING || next.phase()==PublicationPhase.CAPTURING || !Arrays.equals(request(PREPARE,before.request()),request(PREPARE,next.request())) || !before.cut().equals(next.cut()) || !before.owner().equals(next.owner()) || !Arrays.equals(before.configurationDigest(),next.configurationDigest()) || !Arrays.equals(before.captureDigest(),next.captureDigest()) || before.maximumImageBytes()!=next.maximumImageBytes())throw new IOException("Pending source publication changed committed authority");
    }
    private record Reservation(Request request,long maximumImageBytes) {}
    private static final int MAX_RESERVATION_FILE=MAX_REQUEST_BYTES+44;
    private Reservation reservation(UUID id)throws IOException{var bytes=read(path(id,"reserved"),MAX_RESERVATION_FILE);try{var in=ByteBuffer.wrap(bytes);long maximum=in.getLong();int length=in.getInt();if(maximum<1 || maximum>imageLimits.maxImageBytes() || length<1 || length!=in.remaining())throw new IOException("Invalid source disk reservation");byte[] requestBytes=new byte[length];in.get(requestBytes);var request=request(requestBytes);if(!request.transferId().equals(id))throw new IOException("Source reservation identity changed");return new Reservation(request,maximum);}catch(IllegalArgumentException malformed){throw new IOException("Invalid source disk reservation",malformed);}}
    private byte[] reservationBytes(Request request){byte[] encoded=request(PREPARE,request);return ByteBuffer.allocate(12+encoded.length).putLong(imageLimits.maxImageBytes()).putInt(encoded.length).put(encoded).array();}
    /** Durable inventory admission precedes source pin acquisition and image materialization. */
    public synchronized void reserve(Request request)throws IOException {
        requireOpen();requireNotObsolete(request);UUID id=request.transferId();if(exists(path(id,"recovery-reserved")))throw new AdmissionRejected("Source transfer collides with topology recovery anchor");if(exists(path(id,"released")) || exists(path(id,"released.pending")))throw new AdmissionRejected("Source transfer already has durable or pending release authority");
        var file=path(id,"reserved");if(exists(file)){var prior=reservation(id);if(!Arrays.equals(request(PREPARE,request),request(PREPARE,prior.request())))throw new AdmissionRejected("Original source reservation request changed");SessionJournalFiles.force(file);SessionJournalFiles.force(root);inventory(null,null);return;}
        var usage=inventoryUsage();long imageCharge=Math.multiplyExact(2,imageLimits.maxImageBytes()),metadataCharge=Math.max(remainingCapturePromise(request,false),Math.max(RELEASE_METADATA_PROMISE,request.purpose()==Purpose.CURRENT_LEAF?obsoleteMetadataPromise(request):0))+3L*(44L+encodedRequestBytes(request));
        if(usage.count()>=maximumArtifacts || publicationCount()>=MAX_RELEASED || imageCharge>maximumImageBytes-usage.imageBytes() || metadataCharge>maximumMetadataBytes-usage.metadataBytes())throw new AdmissionRejected("Durable source image/metadata reservation exhausted");
        writeMetadata(file,reservationBytes(request),MAX_RESERVATION_FILE);inventory(null,null);
    }
    /** Shares the existing forced image/scratch/metadata quota with source transfers. */
    synchronized Path reserveTopologyRecovery(UUID id)throws IOException {
        requireOpen();var reservation=path(id,"recovery-reserved");
        if(pendingObsolete.containsKey(id) || exists(path(id,"obsolete")) || exists(path(id,"obsolete.pending")) || exists(path(id,"reserved")) || exists(path(id,"released")) || exists(path(id,"released.pending")) || exists(path(id,"publication")))throw new IOException("Recovery anchor collides with source transfer");
        if(!exists(reservation)) {
            var usage=inventoryUsage();long imageCharge=Math.multiplyExact(2,imageLimits.maxImageBytes());
            if(usage.count()>=maximumArtifacts || imageCharge>maximumImageBytes-usage.imageBytes() || 3L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES>maximumMetadataBytes-usage.metadataBytes())throw new AdmissionRejected("Topology recovery image/metadata quota exhausted");
            writeMetadata(reservation,ByteBuffer.allocate(8).putLong(imageLimits.maxImageBytes()).array(),40);
        }
        inventory(null,null);var directory=path(id,"recovery");Files.createDirectories(directory);SessionJournalFiles.force(directory);SessionJournalFiles.force(root);return directory;
    }
    synchronized Path topologyRecoveryDirectory(UUID id)throws IOException {
        requireOpen();if(!exists(path(id,"recovery-reserved")) || !Files.isDirectory(path(id,"recovery"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Topology recovery anchor has no forced reservation");inventory(null,null);return path(id,"recovery");
    }
    public synchronized long reservedImageBytes()throws IOException{requireOpen();return inventoryUsage().imageBytes();}
    private record Capture(Request request,byte[] lower,byte[] upper,ApplyPosition cut,DistributedSchema schema,ReplicaDescriptor owner,ReplicatedRangeStore.Configuration configuration,Map<String,byte[]> rows,byte[] origin,NativeTopologyFreeze metadataFreeze) {}
    public synchronized void capture(Request request,TopologyChange.Definition definition,ReplicatedRangeStore.SnapshotPin pin,DistributedSchema schema,ReplicaDescriptor owner)throws Exception {
        request.header().requireDefinition(definition);
        if(!request.relocation())throw new IOException("Definition capture requires relocation purpose");
        capture(request,pin,schema,owner,definition.sourceDirectory(),definition.parents().getFirst());
    }
    public synchronized void capture(Request request,ReplicatedRangeStore.SnapshotPin pin,DistributedSchema schema,ReplicaDescriptor owner)throws Exception {
        if(request.relocation())throw new IOException("Relocation capture requires immutable definition");
        capture(request,pin,schema,owner,request.sourceDirectory(),request.range());
    }
    private void capture(Request request,ReplicatedRangeStore.SnapshotPin pin,DistributedSchema schema,ReplicaDescriptor owner,RangeDirectory directory,RangeDescriptor parent)throws Exception {
        requireNotObsolete(request);var reserved=reservation(request.transferId());requireRequestIo(request,reserved.request());
        if(!pin.groupId().equals(parent.groupId()) || pin.rangeEpoch()!=parent.rangeEpoch() || !Arrays.equals(pin.interval().lowerInclusive(),parent.lowerInclusive()) || !Arrays.equals(pin.interval().upperExclusive(),parent.upperExclusive()) || !pin.interval().domainId().equals(directory.domainId()) || !pin.interval().columns().equals(directory.columns()) || !Arrays.equals(pin.schemaDigest(),schema.digest()))throw new IOException("Source capture pin changed original range/schema");
        var rows=new TreeMap<String,byte[]>();var metadataFreeze=new NativeTopologyFreeze[1];pin.visitState((key,value)->{if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value.clone());if(request.metadataOnly() && metadataFreezeKey(pin.groupId(),key)){if(metadataFreeze[0]!=null)throw new IllegalArgumentException("Duplicate capture metadata FREEZE");metadataFreeze[0]=NativeTopologyFreeze.decode(value);}});
        var origin=request.metadataOnly()?null:pin.topologyImageOrigin();var capture=new Capture(request,pin.interval().lowerInclusive(),pin.interval().upperExclusive(),pin.position(),schema,owner,pin.configuration(),NativeTopologyLearnerSchema.copy(rows),origin==null?new byte[32]:digest(origin.encode()),metadataFreeze[0]);byte[] encoded=encodeCapture(capture);var prior=publication(request);if(prior!=null&&prior.phase()==PublicationPhase.CAPTURING&&!prior.cut().equals(pin.position()))throw new OriginalCutUnavailable();beginPublication(request,capture.cut(),capture.owner(),capture.configuration(),digest(encoded));var file=path(request.transferId(),"capture");
        if(exists(file)){if(!Arrays.equals(read(file,MAX_METADATA_FILE),encoded)){var original=publication(request);if(original!=null&&original.phase()==PublicationPhase.CAPTURING&&!pin.position().equals(original.cut()))throw new OriginalCutUnavailable();throw new IOException("Original source capture cut/configuration/schema changed");}SessionJournalFiles.force(file);SessionJournalFiles.force(root);}else{writeMetadata(file,encoded,MAX_METADATA_FILE);}
        captureForceObserver.forced(file);
    }
    private static byte[] encodeCapture(Capture capture)throws IOException {
        var request=capture.request();var cfg=capture.configuration();var cut=capture.cut();
        if(cut==null || cut.term()<1 || cfg==null || cfg.term()>cut.term() || cfg.index()>cut.index() || cfg.index()==cut.index()&&cfg.term()!=cut.term() || !capture.schema().clusterId().equals(request.clusterId()) || !capture.schema().databaseId().equals(request.databaseId()) || !capture.schema().domainId().equals(request.domainId()) || capture.schema().schemaEpoch()!=(request.sourceDirectory()==null?request.header().schemaEpoch():request.sourceDirectory().schemaEpoch()) || !Arrays.equals(capture.schema().digest(),request.schemaDigest()) || !request.sourceOwner(capture.owner()) || capture.owner().role()!=ReplicaDescriptor.Role.VOTER || capture.origin().length!=32)throw new IOException("Capture intent differs from source owner/schema/native prefix");
        requireMetadataFreeze(request,capture.metadataFreeze(),cut,capture.lower(),capture.upper());
        var buffer=new ByteArrayOutputStream();var out=new DataOutputStream(buffer);out.writeInt(0x53434150);blob(out,request(PREPARE,capture.request()));nullable(out,capture.lower());nullable(out,capture.upper());out.writeLong(capture.cut().term());out.writeLong(capture.cut().index());blob(out,DistributedSchemaCodec.encode(capture.schema()));var owner=capture.owner();uuid(out,owner.nodeId());uuid(out,owner.nodeIncarnation());uuid(out,owner.replicaId());out.writeUTF(owner.peerId());out.writeUTF(owner.address());out.writeByte(owner.role().ordinal());out.writeLong(cfg.term());out.writeLong(cfg.index());blob(out,cfg.data());out.write(capture.origin());blob(out,capture.metadataFreeze()==null?new byte[0]:capture.metadataFreeze().encode());out.writeInt(capture.rows().size());for(var row:new TreeMap<>(capture.rows()).entrySet()){out.writeUTF(row.getKey());blob(out,row.getValue());if(buffer.size()>MAX_REPLY_BYTES)throw new IOException("Source capture intent exceeds bound");}out.flush();if(buffer.size()>MAX_REPLY_BYTES)throw new IOException("Source capture intent exceeds bound");return buffer.toByteArray();
    }
    private Capture capture(UUID id)throws IOException {
        var result=decodeCapture(id,read(path(id,"capture"),MAX_METADATA_FILE));SessionJournalFiles.force(path(id,"capture"));SessionJournalFiles.force(root);return result;
    }
    private static Capture decodeCapture(UUID id,byte[] bytes)throws IOException{try{var in=new DataInputStream(new ByteArrayInputStream(bytes));if(in.readInt()!=0x53434150)throw new IOException("Invalid source capture intent");var request=request(blob(in,MAX_REQUEST_BYTES));byte[] lower=nullable(in),upper=nullable(in);var cut=new ApplyPosition(in.readLong(),in.readLong());var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES));UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in);String peer=in.readUTF(),address=in.readUTF();int role=in.readUnsignedByte();if(role>=ReplicaDescriptor.Role.values().length)throw new IOException("Invalid source capture role");var owner=new ReplicaDescriptor(node,incarnation,replica,peer,address,ReplicaDescriptor.Role.values()[role]);var cfg=new ReplicatedRangeStore.Configuration(in.readLong(),in.readLong(),blob(in,65536));byte[] origin=in.readNBytes(32);byte[] freezeBytes=blob(in,NativeTopologyFreeze.MAX_BYTES);var metadataFreeze=freezeBytes.length==0?null:NativeTopologyFreeze.decode(freezeBytes);int count=in.readInt();if(count<0 || count>NativeTopologyLearnerSchema.MAX_ROWS)throw new IOException("Invalid source capture history count");var rows=new TreeMap<String,byte[]>();long total=0;for(int n=0;n<count;n++){String key=in.readUTF();byte[] value=blob(in,NativeTopologyLearnerSchema.MAX_BYTES);if((total+=value.length)>NativeTopologyLearnerSchema.MAX_BYTES || rows.put(key,value)!=null)throw new IOException("Invalid source capture history");}var result=new Capture(request,lower,upper,cut,schema,owner,cfg,NativeTopologyLearnerSchema.copy(rows),origin,metadataFreeze);if(!request.transferId().equals(id) || in.available()!=0 || !Arrays.equals(bytes,encodeCapture(result)))throw new IOException("Source capture intent changed");return result;}catch(IllegalArgumentException malformed){throw new IOException("Invalid source capture intent",malformed);}
    }
    private static byte[] configurationDigest(ReplicatedRangeStore.Configuration cfg){return digest(ByteBuffer.allocate(16+cfg.data().length).putLong(cfg.term()).putLong(cfg.index()).put(cfg.data()).array());}
    private int publicationCount()throws IOException{
        var identities=new HashSet<UUID>(pendingObsolete.keySet());
        try(var files=Files.newDirectoryStream(root)){for(var file:files){String name=file.getFileName().toString();if(name.endsWith(".publication") || name.endsWith(".publication.pending") || name.endsWith(".reserved") || name.endsWith(".reserved.pending") || name.endsWith(".obsolete") || name.endsWith(".obsolete.pending") || name.endsWith(".released") || name.endsWith(".released.pending")){int dot=name.indexOf('.');if(dot!=36)throw new IOException("Invalid source identity credit filename");try{identities.add(UUID.fromString(name.substring(0,dot)));}catch(IllegalArgumentException malformed){throw new IOException("Invalid source identity credit",malformed);}if(identities.size()>MAX_RELEASED)throw new IOException("Source terminal identity budget exhausted");}}}return identities.size();
    }
    /** Includes main/pending/decoded terminal metadata and both settlement sidecars. */
    private static long obsoleteMetadataPromise(Request original){
        long maximumRequest=ScopedTransactionRpc.HEADER_BYTES+40L+encodedRequestBytes(original)+NativeRecoverySupersession.MAX_SCOPE_BYTES;
        return 3L*(33L+maximumRequest+32)+128+RELEASE_BODY_BYTES+32;
    }
    private static long remainingCapturePromise(Request request,boolean captured){
        long prepared=maximumPreparedBytes(request);
        // Capture persists only origin digest32; META Prepared also forbids a full origin.
        // Beyond request/schema/CFG/owner/history/freeze, capture needs 2*MAX_BOUND+96
        // bytes and <=39 extra bytes per schema row (UTF hex keys, raw keys <=33).
        // The retained Prepared baseline provides 2*MAX_BOUND+237+32*MAX_COLUMNS
        // bytes and 82 per row, so subtracting only the full origin remains conservative.
        long capture=request.metadataOnly()?prepared:prepared-NativeTopologyImageOrigin.MAX_BYTES;
        return 3L*(prepared+32)+(captured?0:3L*(capture+32))+6L*maximumPublicationBytes(request)+256;
    }
    public synchronized PublicationPhase publicationPhase(Request request)throws IOException{var publication=publication(request);return publication==null?null:publication.phase();}
    public synchronized Abandoned abandoned(Request request)throws IOException{var publication=publication(request);if(publication==null||publication.phase()!=PublicationPhase.ABANDONED)return null;cleanupAbandoned(publication);return abandoned(publication);}
    private static Abandoned abandoned(Publication publication){return new Abandoned(publication.request(),publication.successor(),publication.cut(),publication.owner(),publication.configurationDigest());}
    public synchronized Abandoned abandon(Request request,ReplicatedRangeStore.Configuration current,ApplyPosition completed)throws IOException{requireIdle(request.transferId());
        var original=publication(request);if(original==null||original.phase()==PublicationPhase.READY)throw new IOException("Only positively unpublished CAPTURING source may be abandoned");if(original.phase()==PublicationPhase.ABANDONED){cleanupAbandoned(original);return abandoned(original);}requireNotObsolete(request);
        var capture=exists(path(request.transferId(),"capture"))?capture(request.transferId()):null;if((capture==null?!Arrays.equals(configurationDigest(current),original.configurationDigest()):!Arrays.equals(current.data(),capture.configuration().data())) || !RatisRelocationOwnerRpc.covers(completed,original.cut()))throw new IOException("Source abandonment lost original qualified configuration/prefix");
        UUID successor;do{successor=UUID.randomUUID();}while(successor.equals(request.transferId())||successor.equals(new UUID(0,0)));var terminal=new Publication(PublicationPhase.ABANDONED,request,original.cut(),original.owner(),original.configurationDigest(),original.captureDigest(),new byte[32],successor,original.maximumImageBytes());writePublication(terminal);cleanupAbandoned(terminal);return abandoned(terminal);
    }
    private void beginPublication(Request request,ApplyPosition cut,ReplicaDescriptor owner,ReplicatedRangeStore.Configuration cfg,byte[] captureDigest)throws IOException{
        var original=publication(request);if(original!=null){if(original.phase()==PublicationPhase.ABANDONED)throw new AbandonedException(abandoned(original));if(!original.cut().equals(cut)||!Arrays.equals(original.captureDigest(),captureDigest))throw new IOException("Source publication changed original capture");return;}
        writePublication(new Publication(PublicationPhase.CAPTURING,request,cut,owner,configurationDigest(cfg),captureDigest,new byte[32],new UUID(0,0),reservation(request.transferId()).maximumImageBytes()));
    }
    private synchronized void ready(Prepared prepared)throws IOException{
        var original=publication(prepared.request());if(original==null){beginPublication(prepared.request(),prepared.image().position(),prepared.sourceOwner(),prepared.sourceCfg(),digest(reply(PREPARE,prepared)));original=publication(prepared.request());}
        if(original.phase()==PublicationPhase.ABANDONED)throw new AbandonedException(abandoned(original));byte[] readyDigest=digest(reply(PREPARE,prepared));if(original.phase()==PublicationPhase.READY){if(!Arrays.equals(original.preparedDigest(),readyDigest))throw new IOException("READY source identity changed");return;}
        writePublication(new Publication(PublicationPhase.READY,original.request(),original.cut(),original.owner(),original.configurationDigest(),original.captureDigest(),readyDigest,new UUID(0,0),original.maximumImageBytes()));
    }
    public synchronized void requireReady(Prepared prepared)throws IOException{requireNotObsolete(prepared.request());var publication=publication(prepared.request());if(publication==null||publication.phase()!=PublicationPhase.READY||!Arrays.equals(publication.preparedDigest(),digest(reply(PREPARE,prepared))))throw new IOException("Source Prepared lacks forced READY publication");}
    private Publication publication(Request request)throws IOException{var file=path(request.transferId(),"publication");if(!exists(file))return null;var value=decodePublication(read(file,MAX_PUBLICATION_FILE));requireRequestIo(request,value.request());SessionJournalFiles.force(file);SessionJournalFiles.force(root);return value;}
    private static byte[] encodePublication(Publication value)throws IOException{var buffer=new ByteArrayOutputStream();var out=new DataOutputStream(buffer);out.writeInt(0x53505542);out.writeByte(value.phase().ordinal());blob(out,request(PREPARE,value.request()));out.writeLong(value.cut().term());out.writeLong(value.cut().index());var owner=value.owner();uuid(out,owner.nodeId());uuid(out,owner.nodeIncarnation());uuid(out,owner.replicaId());out.writeUTF(owner.peerId());out.writeUTF(owner.address());out.writeByte(owner.role().ordinal());out.write(value.configurationDigest());out.write(value.captureDigest());out.write(value.preparedDigest());uuid(out,value.successor());out.writeLong(value.maximumImageBytes());out.flush();if(buffer.size()+32>MAX_PUBLICATION_FILE)throw new IOException("Source publication exceeds bound");return buffer.toByteArray();}
    private Publication decodePublication(byte[] bytes)throws IOException{try{var in=new DataInputStream(new ByteArrayInputStream(bytes));if(in.readInt()!=0x53505542)throw new IOException("Invalid source publication");int phase=in.readUnsignedByte();if(phase>=PublicationPhase.values().length)throw new IOException("Invalid source publication phase");var request=request(blob(in,MAX_REQUEST_BYTES));var cut=new ApplyPosition(in.readLong(),in.readLong());UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in);String peer=in.readUTF(),address=in.readUTF();int role=in.readUnsignedByte();if(role>=ReplicaDescriptor.Role.values().length)throw new IOException("Invalid source publication owner");var owner=new ReplicaDescriptor(node,incarnation,replica,peer,address,ReplicaDescriptor.Role.values()[role]);byte[] cfg=in.readNBytes(32),capture=in.readNBytes(32),prepared=in.readNBytes(32);UUID successor=uuid(in);long maximum=in.readLong();var value=new Publication(PublicationPhase.values()[phase],request,cut,owner,cfg,capture,prepared,successor,maximum);if(cut.term()<1||maximum<1||maximum>imageLimits.maxImageBytes()||!request.sourceOwner(owner)||in.available()!=0||!Arrays.equals(bytes,encodePublication(value)))throw new IOException("Invalid source publication binding");if(value.phase()==PublicationPhase.ABANDONED)new Abandoned(request,successor,cut,owner,cfg);else if(!successor.equals(new UUID(0,0)))throw new IOException("Nonterminal source publication carries successor");return value;}catch(IllegalArgumentException malformed){throw new IOException("Invalid source publication",malformed);}}
    private void writePublication(Publication value)throws IOException{writeMetadata(path(value.request().transferId(),"publication"),encodePublication(value),MAX_PUBLICATION_FILE);publicationForceObserver.forced(value.phase());}
    private void cleanupAbandoned(Publication publication)throws IOException{
        UUID id=publication.request().transferId();var settled=path(id,"abandoned-settled");byte[] expected=digest(encodePublication(publication));if(exists(settled)){if(!Arrays.equals(read(settled,64),expected))throw new IOException("Abandoned settlement changed");SessionJournalFiles.force(settled);SessionJournalFiles.force(root);return;}
        var sourceDir=root.resolve(id+".source");if(Files.exists(sourceDir,LinkOption.NOFOLLOW_LINKS)){directory(sourceDir);try(var files=Files.newDirectoryStream(sourceDir)){int count=0;for(var file:files){if(++count>4)throw new IOException("Abandoned source directory exceeds bound");regular(file,imageLimits.maxImageBytes());removal.delete(file);}}removal.delete(sourceDir);}
        for(var suffix:List.of("image.pending","image","prepared.pending","prepared","capture.pending","capture","reserved.pending","reserved")){var file=path(id,suffix);if(exists(file))removal.delete(file);}removal.force(root);writeMetadata(settled,expected,64);
    }
    private static void nullable(DataOutputStream out,byte[] bound)throws IOException{if(bound==null)out.writeInt(-1);else{if(bound.length>MAX_BOUND_BYTES)throw new IOException("Source capture bound exceeds limit");blob(out,bound);}}
    private static byte[] nullable(DataInputStream in)throws IOException{int length=in.readInt();if(length==-1)return null;if(length<0 || length>MAX_BOUND_BYTES || length>in.available())throw new IOException("Invalid source capture bound");return in.readNBytes(length);}
    private static void blob(DataOutputStream out,byte[] bytes)throws IOException{out.writeInt(bytes.length);out.write(bytes);}private static byte[] blob(DataInputStream in,int maximum)throws IOException{int size=in.readInt();if(size<0 || size>maximum || size>in.available())throw new IOException("Invalid source capture field");return in.readNBytes(size);}private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    private synchronized void writeMetadata(Path file,byte[] body,int maximum)throws IOException{SessionJournalFiles.write(file,body,maximum);}
    private final Set<UUID> bulkOwners=new HashSet<>();
    private final ThreadLocal<NativeRangeImage.IoAdmission> imageAdmission=ThreadLocal.withInitial(()->NativeRangeImage.IoAdmission.UNLIMITED);
    private AutoCloseable ownBulk(UUID id,NativeRangeImage.IoAdmission admission)throws IOException{
        Objects.requireNonNull(admission);synchronized(this){requireOpen();if(bulkOwners.size()>=maximumArtifacts || !bulkOwners.add(id))throw new IOException("Source artifact bulk owner is active");}
        imageAdmission.set(Objects.requireNonNull(admission));
        return ()->{imageAdmission.remove();synchronized(this){bulkOwners.remove(id);}};
    }
    private void requireIdle(UUID id)throws IOException{if(bulkOwners.contains(id))throw new IOException("Actual source artifact validation/publication is active");}
    /** Full image validation is owned per transfer; no rate callback executes under the artifact monitor. */
    public Saved lookupAdmitted(Request request,NativeRangeImage.IoAdmission admission)throws IOException{
        try(var owner=ownBulk(request.transferId(),admission)){return lookupOwned(request);}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Source lookup owner failed",failure);}
    }
    public Saved retainAdmitted(Prepared prepared,SnapshotInfo snapshot,NativeRangeImage.IoAdmission admission)throws IOException{
        try(var owner=ownBulk(prepared.request().transferId(),admission)){return retainOwned(prepared,snapshot);}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Source publication owner failed",failure);}
    }
    public synchronized Saved lookup(Request request)throws IOException {requireIdle(request.transferId());return lookupOwned(request);}
    private Saved lookupOwned(Request request)throws IOException {
        requireOpen();UUID id=request.transferId();var published=publication(request);if(published!=null&&published.phase()==PublicationPhase.ABANDONED){cleanupAbandoned(published);throw new AbandonedException(abandoned(published));}requireNotObsolete(request);
        if(exists(path(id,"released"))){var receipt=receipt(id);requireReceipt(request,receipt);cleanup(receipt);throw new IOException("Source transfer was durably released");}
        if(!exists(path(id,"capture")) && exists(path(id,"capture.pending"))){var pending=path(id,"capture.pending");var original=decodeCapture(id,read(pending,MAX_METADATA_FILE));requireRequestIo(request,original.request());SessionJournalFiles.force(pending);Files.move(pending,path(id,"capture"),StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(path(id,"capture"));SessionJournalFiles.force(root);}
        if(!exists(path(id,"prepared")) && exists(path(id,"prepared.pending"))){
            var pending=path(id,"prepared.pending");var prepared=decodeMetadata(id,read(pending,MAX_METADATA_FILE));requireRequestIo(request,prepared.request());inventory(null,null);synchronized(this){Files.move(pending,path(id,"prepared"),StandardCopyOption.ATOMIC_MOVE);}SessionJournalFiles.force(path(id,"prepared"));SessionJournalFiles.force(root);
        }
        if(!exists(path(id,"prepared")) && exists(path(id,"capture"))){var original=capture(id);requireRequestIo(request,original.request());var file=sourcePath(id,original.cut());if(exists(file)){var image=inspect(file,request.metadataOnly());byte[] origin=image.topologyOrigin()==null?new byte[32]:digest(image.topologyOrigin().encode());if(!image.position().equals(original.cut()) || !Arrays.equals(image.interval().lowerInclusive(),original.lower()) || !Arrays.equals(image.interval().upperExclusive(),original.upper()) || !Arrays.equals(origin,original.origin()))throw new IOException("Original capture image/provenance changed");var prepared=new Prepared(request,image,original.schema(),original.owner(),original.configuration(),original.rows(),original.metadataFreeze());retainOwned(prepared,snapshot(file,prepared));}}
        if(!exists(path(id,"prepared"))){if(published!=null&&published.phase()==PublicationPhase.READY)throw new IOException("READY source lost original metadata; abandonment forbidden");if(exists(path(id,"image")) || exists(path(id,"image.pending")) || exists(path(id,"prepared.pending")))throw new IOException("Unbound or uncertain source artifact cannot be recaptured");return null;}
        var prepared=metadata(id);requireRequestIo(request,prepared.request());inventory(null,null);cleanMetadataPending(path(id,"prepared"),reply(PREPARE,prepared));
        Path image=path(id,"image");if(!exists(image)){
            Path source=sourcePath(id,prepared.image().position());if(!exists(source))throw new IOException("Forced source intent has no original image; exact repair required");
            validateImage(prepared,source);publishImage(source,image,prepared);
        }
        validateImage(prepared,image);ready(prepared);var saved=new Saved(prepared,image,snapshot(image,prepared));return new Saved(prepared,image,restoreOriginal(saved));
    }
    public synchronized Saved retain(Prepared prepared,SnapshotInfo snapshot)throws IOException {requireIdle(prepared.request().transferId());return retainOwned(prepared,snapshot);}
    private Saved retainOwned(Prepared prepared,SnapshotInfo snapshot)throws IOException {
        requireOpen();Objects.requireNonNull(prepared);Objects.requireNonNull(snapshot);reserve(prepared.request());UUID id=prepared.request().transferId();
        if(exists(path(id,"released"))){requireReceipt(prepared.request(),receipt(id));throw new IOException("Released source transfer cannot be retained again");}
        if(snapshot.getFiles().size()!=1 || snapshot.getTerm()!=prepared.image().position().term() || snapshot.getIndex()!=prepared.image().position().index())throw new IOException("Source snapshot identity changed");
        Path source=snapshot.getFiles().getFirst().getPath().toAbsolutePath().normalize();
        regular(source,prepared.image().bytes());validateImage(prepared,source);MD5Hash md5=MD5Hash.newInstance(NativeRangeImage.md5Admitted(source,imageAdmission.get()));var declared=snapshot.getFiles().getFirst().getFileDigest();if(declared!=null && !declared.equals(md5))throw new IOException("Source snapshot MD5 changed");
        byte[] encoded=reply(PREPARE,prepared);inventory(prepared,encoded);
        Path metadata=path(id,"prepared");if(exists(metadata)){byte[] prior=read(metadata,MAX_METADATA_FILE);if(!Arrays.equals(prior,encoded))throw new IOException("Original forced source metadata changed");}
        else {
            if(exists(path(id,"image")) || exists(path(id,"image.pending")))throw new IOException("Foreign image already uses transfer identity");
            cleanMetadataPending(metadata,encoded);synchronized(this){writeMetadata(metadata,encoded,MAX_METADATA_FILE);}
        }
        cleanMetadataPending(metadata,encoded);var original=sourcePath(id,prepared.image().position());ensureOriginalDirectory(id);publishImage(source,original,prepared);publishImage(original,path(id,"image"),prepared);validateImage(prepared,path(id,"image"));
        ready(prepared);return new Saved(prepared,path(id,"image"),new SingleFileSnapshotInfo(new FileInfo(original,md5),snapshot.getTerm(),snapshot.getIndex()));
    }
    private Path sourcePath(UUID id,ApplyPosition cut){return root.resolve(id+".source").resolve(SimpleStateMachineStorage.getSnapshotFileName(cut.term(),cut.index()));}
    private synchronized void ensureOriginalDirectory(UUID id)throws IOException {
        var owned=root.resolve(id+".source");directory(root);
        if(!Files.exists(owned,LinkOption.NOFOLLOW_LINKS))Files.createDirectory(owned);
        directory(owned);SessionJournalFiles.force(owned);SessionJournalFiles.force(root);
    }
    public synchronized Path originalImagePath(Request request,ApplyPosition cut)throws IOException {
        requireNotObsolete(request);var original=capture(request.transferId());requireRequestIo(request,original.request());if(!cut.equals(original.cut()))throw new IOException("Source original cut changed");var file=sourcePath(request.transferId(),cut);ensureOriginalDirectory(request.transferId());return file;
    }
    private SnapshotInfo restoreOriginal(Saved saved)throws IOException {
        requireOpen();var prepared=saved.prepared();requireNotObsolete(prepared.request());requirePrepared(metadata(prepared.request().transferId()),prepared);if(exists(path(prepared.request().transferId(),"released")))throw new IOException("Released source image cannot be restored");
        Path target=sourcePath(prepared.request().transferId(),prepared.image().position());ensureOriginalDirectory(prepared.request().transferId());publishImage(saved.image(),target,prepared);return snapshot(target,prepared);
    }
    /** Re-establishes ACK for an existing durable terminal only; never creates fresh authority. */
    public boolean retryObsolete(RatisRecoveryObsoleteRpc.Request request)throws IOException {
        UUID id=request.original().transferId();
        try(var owner=ownBulk(id,NativeRangeImage.IoAdmission.UNLIMITED)){
            Path canonical=path(id,"obsolete"),pending=path(id,"obsolete.pending");Obsolete value;
            if(exists(canonical)){value=obsoleteRecord(canonical,id);requireObsoleteRequest(request,value);}
            else if(exists(pending)){
                regular(pending,MAX_OBSOLETE_FILE);byte[] encoded=Files.readAllBytes(pending);
                if(encoded.length<32 || !MessageDigest.isEqual(digest(Arrays.copyOf(encoded,encoded.length-32)),Arrays.copyOfRange(encoded,encoded.length-32,encoded.length)))return false;
                value=obsoleteRecord(pending,id);requireObsoleteRequest(request,value);
                inventoryUsage(null,null,value);SessionJournalFiles.force(pending);Files.move(pending,canonical,StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(root);
            }else return false;
            synchronized(this){var warm=pendingObsolete.get(id);if(warm!=null)requireObsoleteRequest(request,warm);}
            if(exists(pending)){byte[] body=encodeObsolete(value);requirePrefixBytes(pending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());}
            obsoleteForceObserver.boundary(false);SessionJournalFiles.force(canonical);SessionJournalFiles.force(root);
            if(exists(pending)){removal.delete(pending);removal.force(root);}obsoleteForceObserver.boundary(true);
            synchronized(this){pendingObsolete.remove(id);}return true;
        }catch(IOException failed){throw failed;}catch(Exception failed){throw new IOException("Source obsolete durable retry failed",failed);}
    }
    /** The qualified caller keeps its actual source lease until this force acknowledges. No files are deleted. */
    public void forceObsolete(RatisRecoveryObsoleteRpc.Request request)throws IOException {
        try(var owner=ownBulk(request.original().transferId(),NativeRangeImage.IoAdmission.UNLIMITED)){forceObsoleteOwned(request);}
        catch(IOException failed){throw failed;}catch(Exception failed){throw new IOException("Source obsolete force failed",failed);}
    }
    /** Qualified caller has drained its actual source lease; settlement acknowledges forced deletion too. */
    public void obsolete(RatisRecoveryObsoleteRpc.Request request)throws IOException {
        try(var owner=ownBulk(request.original().transferId(),NativeRangeImage.IoAdmission.UNLIMITED)){var value=forceObsoleteOwned(request);cleanupObsolete(value);}
        catch(IOException failed){throw failed;}catch(Exception failed){throw new IOException("Source obsolete cleanup failed",failed);}
    }
    /** Exact settled terminal lookup only; absence grants no authority. */
    public boolean isObsolete(RatisRecoveryObsoleteRpc.Request request)throws IOException {
        try(var owner=ownBulk(request.original().transferId(),NativeRangeImage.IoAdmission.UNLIMITED)){
            UUID id=request.original().transferId();if(!exists(path(id,"obsolete")))return false;var value=obsoleteRecord(path(id,"obsolete"),id);requireObsoleteRequest(request,value);if(!exists(path(id,"obsolete-settled")))return false;requireObsoleteSettled(value);return true;
        }catch(IOException failed){throw failed;}catch(Exception failed){throw new IOException("Source obsolete lookup failed",failed);}
    }
    private Obsolete forceObsoleteOwned(RatisRecoveryObsoleteRpc.Request request)throws IOException {
        UUID id=request.original().transferId();Obsolete value;requireNoInstalledRelease(request.original());
        synchronized(this){
            requireOpen();
            if(exists(path(id,"recovery-reserved")) || exists(path(id,"recovery-reserved.pending")) || exists(path(id,"recovery")))throw new AdmissionRejected("Obsolete source identity collides with recovery anchor");
            value=pendingObsolete.get(id);
            if(exists(path(id,"obsolete"))){var saved=obsoleteRecord(path(id,"obsolete"),id);if(value!=null&&!Arrays.equals(encodeObsolete(value),encodeObsolete(saved)))throw new IOException("Obsolete force changed original captured authority");value=saved;}
            if(value==null && exists(path(id,"obsolete.pending"))){try{value=obsoleteRecord(path(id,"obsolete.pending"),id);}catch(IOException partial){regular(path(id,"obsolete.pending"),MAX_OBSOLETE_FILE);}}
            if(value==null){
                long reserved=0;ApplyPosition cut=null;var original=request.original();
                if(exists(path(id,"reserved"))){var reservation=reservation(id);requireObsoleteOriginal(original,reservation.request());reserved=reservation.maximumImageBytes();}
                if(exists(path(id,"capture"))){var captured=decodeCapture(id,read(path(id,"capture"),MAX_METADATA_FILE));requireObsoleteOriginal(original,captured.request());cut=captured.cut();}
                if(exists(path(id,"prepared"))){var prepared=decodeMetadata(id,read(path(id,"prepared"),MAX_METADATA_FILE));requireObsoleteOriginal(original,prepared.request());if(cut!=null&&!cut.equals(prepared.image().position()))throw new IOException("Obsolete source capture/prepared cut changed");cut=prepared.image().position();}
                var publication=exists(path(id,"publication"))?decodePublication(read(path(id,"publication"),MAX_PUBLICATION_FILE)):null;if(publication!=null)requireObsoleteOriginal(original,publication.request());if(publication!=null){if(cut!=null&&!cut.equals(publication.cut()))throw new IOException("Obsolete source publication cut changed");cut=publication.cut();if(!exists(path(id,"abandoned-settled")))reserved=Math.max(reserved,publication.maximumImageBytes());}
                value=new Obsolete(request,reserved,cut);
                inventoryUsage(null,null,value);
            }
            requireObsoleteRequest(request,value);pendingObsolete.put(id,value);
        }
        byte[] body=encodeObsolete(value);Path canonical=path(id,"obsolete"),pending=path(id,"obsolete.pending");
        if(exists(canonical)){if(!Arrays.equals(read(canonical,MAX_OBSOLETE_FILE),body))throw new IOException("Obsolete terminal changed");SessionJournalFiles.force(canonical);SessionJournalFiles.force(root);}
        else {if(exists(pending)){requirePrefixBytes(pending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());removal.delete(pending);removal.force(root);}obsoleteForceObserver.boundary(false);writeMetadata(canonical,body,MAX_OBSOLETE_FILE);obsoleteForceObserver.boundary(true);}
        synchronized(this){pendingObsolete.remove(id);}return value;
    }
    private void requireNotObsolete(Request request)throws IOException {
        UUID id=request.transferId();var retained=pendingObsolete.get(id);
        if(retained!=null){requireRequestIo(request,retained.request().original());throw new AdmissionRejected("Source CURRENT capture has pending obsolete disposition");}
        if(exists(path(id,"obsolete"))){var value=obsoleteRecord(path(id,"obsolete"),id);requireRequestIo(request,value.request().original());throw new AdmissionRejected("Source CURRENT capture is durably obsolete");}
        if(exists(path(id,"obsolete.pending")))throw new AdmissionRejected("Source CURRENT capture has uncertain obsolete disposition");
    }
    private byte[] encodeObsolete(Obsolete value){byte[] request=RatisRecoveryObsoleteRpc.request(value.request());var out=ByteBuffer.allocate(17+request.length+(value.cut()==null?0:16)).putInt(OBSOLETE_MAGIC).putInt(request.length).put(request).putLong(value.reservedImageBytes()).put((byte)(value.cut()==null?0:1));if(value.cut()!=null)out.putLong(value.cut().term()).putLong(value.cut().index());return out.array();}
    private Obsolete obsoleteRecord(Path file,UUID id)throws IOException {try{var bytes=read(file,MAX_OBSOLETE_FILE);var in=ByteBuffer.wrap(bytes);if(in.remaining()<17 || in.getInt()!=OBSOLETE_MAGIC)throw new IOException("Invalid obsolete tombstone");int size=in.getInt();if(size<1 || size>RatisRecoveryObsoleteRpc.MAX_REQUEST_BYTES || size>in.remaining()-9)throw new IOException("Invalid obsolete request length");byte[] request=new byte[size];in.get(request);var decoded=RatisRecoveryObsoleteRpc.decode(request);long reserved=in.getLong();int present=in.get()&255;if(reserved<0 || reserved>imageLimits.maxImageBytes() || present>1 || in.remaining()!=(present==0?0:16) || !decoded.original().transferId().equals(id))throw new IOException("Invalid obsolete reservation/identity");var cut=present==0?null:new ApplyPosition(in.getLong(),in.getLong());if(cut!=null&&cut.term()<1)throw new IOException("Invalid obsolete captured cut");var result=new Obsolete(decoded,reserved,cut);if(!Arrays.equals(bytes,encodeObsolete(result)))throw new IOException("Noncanonical obsolete tombstone");return result;}catch(IllegalArgumentException|java.nio.BufferUnderflowException bad){throw new IOException("Invalid obsolete tombstone",bad);}}
    private static void requireObsoleteOriginal(Request expected,Request actual)throws AdmissionRejected {if(!Arrays.equals(request(PREPARE,expected),request(PREPARE,actual)))throw new AdmissionRejected("Obsolete request conflicts with a valid original source namespace");}
    private static void requireObsoleteRequest(RatisRecoveryObsoleteRpc.Request request,Obsolete value)throws IOException {if(!request.equals(value.request()))throw new AdmissionRejected("Obsolete terminal requires exact original request/fence");}
    private void requireObsoleteSettled(Obsolete value)throws IOException {if(!Arrays.equals(read(path(value.request().original().transferId(),"obsolete-settled"),64),digest(encodeObsolete(value))))throw new IOException("Obsolete settlement changed");SessionJournalFiles.force(path(value.request().original().transferId(),"obsolete-settled"));SessionJournalFiles.force(root);}
    private void cleanupObsolete(Obsolete value)throws IOException {
        UUID id=value.request().original().transferId();var settled=path(id,"obsolete-settled");if(exists(settled)){requireObsoleteSettled(value);return;}
        var source=root.resolve(id+".source");if(exists(source)){directory(source);if(value.cut()==null)throw new IOException("Obsolete source directory lacks captured cut");String expected=sourcePath(id,value.cut()).getFileName().toString();int count=0;try(var files=Files.newDirectoryStream(source)){for(var file:files){if(++count>4)throw new IOException("Obsolete source directory exceeds bound");var name=file.getFileName().toString();if(!name.equals(expected)&&!name.equals(expected+".pending")&&!name.matches("[.]native-range-building-.*[.]tmp"))throw new IOException("Foreign obsolete source build file");regular(file,imageLimits.maxImageBytes());removal.delete(file);}}removal.force(source);removal.delete(source);}
        for(var suffix:List.of("image.pending","image","released.pending","prepared.pending","prepared","capture.pending","capture","reserved.pending","reserved")){var file=path(id,suffix);if(exists(file)){regular(file,suffix.startsWith("image")?imageLimits.maxImageBytes():MAX_METADATA_FILE);removal.delete(file);}}
        removal.force(root);writeMetadata(settled,digest(encodeObsolete(value)),64);
    }
    /** Retry an already forced release only. This never creates a receipt or authorizes an unknown transfer. */
    public boolean isReleased(Request request)throws IOException {try(var owner=ownBulk(request.transferId(),NativeRangeImage.IoAdmission.UNLIMITED)){return isReleasedOwned(request);}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}}
    private boolean isReleasedOwned(Request request)throws IOException {
        requireOpen();requireNotObsolete(request);UUID id=request.transferId();
        if(!exists(path(id,"released"))){var pending=completeReleasePending(id);if(pending==null)return false;requireReceipt(request,pending);var expected=expectedReleaseReceipt(request,true);if(!Arrays.equals(encode(pending),encode(expected)))throw new IOException("Pending RELEASE changed retained image authority");SessionJournalFiles.force(path(id,"released.pending"));Files.move(path(id,"released.pending"),path(id,"released"),StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(root);}
        var receipt=receipt(id);requireReceipt(request,receipt);cleanup(receipt);return true;
    }
    public void released(Request request)throws IOException {try(var owner=ownBulk(request.transferId(),NativeRangeImage.IoAdmission.UNLIMITED)){releasedOwned(request);}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}}
    private void releasedOwned(Request request)throws IOException {
        requireOpen();requireNotObsolete(request);UUID id=request.transferId();Receipt receipt;
        if(exists(path(id,"released"))){receipt=receipt(id);requireReceipt(request,receipt);}
        else {
            var prepared=metadata(id);requireRequestIo(request,prepared.request());if(exists(path(id,"image")))validateImage(prepared,path(id,"image"));
            receipt=expectedReleaseReceipt(request,false);inventoryUsage(null,null,null,receipt);int releases=0;try(var entries=Files.newDirectoryStream(root,"*.released")){for(var ignored:entries)if(++releases>=MAX_RELEASED)throw new IOException("Source release receipt quota exhausted");}
            if(Files.exists(path(id,"released.pending"),LinkOption.NOFOLLOW_LINKS))regular(path(id,"released.pending"),RELEASE_BODY_BYTES+32);
            Path releasePending=path(id,"released.pending");if(exists(releasePending)){byte[] body=encode(receipt);requirePrefixBytes(releasePending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());Files.delete(releasePending);SessionJournalFiles.force(root);}
            writeMetadata(path(id,"released"),encode(receipt),RELEASE_BODY_BYTES+32);
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
            else {var partial=inspect(pending,receipt.metadataOnly());if(!Arrays.equals(partial.sha256(),receipt.imageDigest()) || partial.bytes()!=receipt.imageBytes())throw new IOException("Uncertain staging image cannot be reclaimed");}
        }
        Path metadataPending=path(receipt.id(),"prepared.pending");
        if(exists(metadataPending)){if(metadataBody==null){metadataBody=read(metadataPending,MAX_METADATA_FILE);if(!Arrays.equals(digest(metadataBody),receipt.metadataDigest()))throw new IOException("Uncertain released metadata staging");}requirePrefixBytes(metadataPending,ByteBuffer.allocate(metadataBody.length+32).put(metadataBody).put(digest(metadataBody)).array());}
        if(exists(image)){regular(image,receipt.imageBytes());var manifest=inspect(image,receipt.metadataOnly());if(manifest.bytes()!=receipt.imageBytes() || !Arrays.equals(manifest.sha256(),receipt.imageDigest()))throw new IOException("Released source image changed");removal.delete(image);}
        if(exists(pending)){regular(pending,receipt.imageBytes());removal.delete(pending);}
        if(exists(metadataPending))removal.delete(metadataPending);
        var source=sourcePath(receipt.id(),receipt.cut());var sourceDir=source.getParent();if(Files.exists(sourceDir,LinkOption.NOFOLLOW_LINKS)){
            if(!Files.isDirectory(sourceDir,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source original namespace changed");
            try(var files=Files.newDirectoryStream(sourceDir)){for(var file:files){var name=file.getFileName().toString();if(!name.equals(source.getFileName().toString()) && !name.equals(source.getFileName()+".pending"))throw new IOException("Unsettled source build prevents cleanup");regular(file,receipt.imageBytes());if(name.endsWith(".pending")){if(!exists(source))throw new IOException("Partial original source image lacks settled original");requirePrefix(file,source);}else{var imageManifest=inspect(file,receipt.metadataOnly());if(!Arrays.equals(imageManifest.sha256(),receipt.imageDigest()) || !imageManifest.position().equals(receipt.cut()))throw new IOException("Source original changed before cleanup");}removal.delete(file);}}
            removal.delete(sourceDir);
        }
        if(exists(metadata))removal.delete(metadata);for(var suffix:List.of("capture.pending","capture","reserved.pending","reserved")){var file=path(receipt.id(),suffix);if(exists(file))removal.delete(file);}removal.force(root);
        writeMetadata(path(receipt.id(),"settled"),digest(encode(receipt)),64);
    }
    private record Usage(int count,long imageBytes,long metadataBytes) {}
    private synchronized long metadataUsage()throws IOException{return inventoryUsage().metadataBytes();}
    private synchronized void inventory(Prepared candidate,byte[] candidateBytes)throws IOException{inventoryUsage(candidate,candidateBytes);}
    private Usage inventoryUsage()throws IOException{return inventoryUsage(null,null);}
    private Usage inventoryUsage(Prepared candidate,byte[] candidateBytes)throws IOException {return inventoryUsage(candidate,candidateBytes,null);}
    private Usage inventoryUsage(Prepared candidate,byte[] candidateBytes,Obsolete proposedObsolete)throws IOException {return inventoryUsage(candidate,candidateBytes,proposedObsolete,null);}
    private Usage inventoryUsage(Prepared candidate,byte[] candidateBytes,Obsolete proposedObsolete,Receipt proposedRelease)throws IOException {
        publicationCount();
        var recoveries=new HashSet<UUID>();var recoveryDirectories=new HashSet<UUID>();var reservations=new HashMap<UUID,Reservation>();var prepared=new HashMap<UUID,Prepared>();var captures=new HashMap<UUID,Capture>();var receipts=new HashMap<UUID,Receipt>();var settled=new HashSet<UUID>();var abandonedSettled=new HashSet<UUID>();var publications=new HashMap<UUID,Publication>();var imageIds=new HashSet<UUID>();var sourceIds=new HashSet<UUID>();var obsoletes=new HashMap<UUID,Obsolete>();var obsoleteIds=new HashSet<UUID>();var obsoleteSettled=new HashSet<UUID>();var obsoleteBytes=new HashMap<UUID,Long>();var releasePendingBytes=new HashMap<UUID,Long>();long metadata=0;int files=0;
        try(var entries=Files.newDirectoryStream(root)){for(var file:entries){if(++files>7*MAX_RELEASED+maximumArtifacts*12+1)throw new IOException("Source artifact directory quota exceeded");String name=file.getFileName().toString();if(name.equals("artifacts.lock")){regular(file,0);continue;}int dot=name.indexOf('.');if(dot!=36)throw new IOException("Foreign source artifact path");UUID id;try{id=UUID.fromString(name.substring(0,dot));}catch(IllegalArgumentException invalid){throw new IOException("Foreign source artifact identity",invalid);}if(!id.toString().equals(name.substring(0,dot)))throw new IOException("Noncanonical source artifact identity");String suffix=name.substring(dot+1);
            switch(suffix){
                case "recovery-reserved","recovery-reserved.pending"->{recoveryReservation(file);if(recoveries.add(id))metadata=Math.addExact(metadata,3L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES);}
                case "recovery"->{if(!Files.isDirectory(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid recovery anchor directory");recoveryDirectories.add(id);}

                case "source"->{if(!Files.isDirectory(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid source original directory");sourceIds.add(id);}
                case "publication"->{regular(file,MAX_PUBLICATION_FILE);var publication=decodePublication(read(file,MAX_PUBLICATION_FILE));if(!publication.request().transferId().equals(id))throw new IOException("Source publication filename changed");publications.put(id,publication);metadata=Math.addExact(metadata,3L*Files.size(file));}
                case "abandoned-settled"->{regular(file,64);abandonedSettled.add(id);metadata=Math.addExact(metadata,Files.size(file));}
                case "publication.pending"->{regular(file,MAX_PUBLICATION_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));throw new IOException("Uncertain source publication requires exact recovery");}
                case "abandoned-settled.pending"->{regular(file,64);metadata=Math.addExact(metadata,Files.size(file));}
                case "reserved"->{regular(file,MAX_RESERVATION_FILE);reservations.put(id,reservation(id));metadata=Math.addExact(metadata,3L*Files.size(file));}
                case "capture"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source capture metadata quota exhausted before decode");captures.put(id,capture(id));}
                case "prepared"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source metadata quota exhausted before decode");prepared.put(id,metadata(id));}
                case "image","image.pending"->{regular(file,imageLimits.maxImageBytes());if(suffix.equals("image.pending")&&exists(path(id,"image")))throw new IOException("Uncertain duplicate source image staging");imageIds.add(id);}
                case "obsolete","obsolete.pending"->{regular(file,MAX_OBSOLETE_FILE);obsoleteIds.add(id);obsoleteBytes.merge(id,Files.size(file),Math::max);Obsolete value=null;try{value=obsoleteRecord(file,id);}catch(IOException partial){if(suffix.equals("obsolete"))throw partial;}if(value!=null){var previous=obsoletes.putIfAbsent(id,value);if(previous!=null&&!Arrays.equals(encodeObsolete(previous),encodeObsolete(value)))throw new IOException("Conflicting obsolete terminal sidecars");}}
                case "obsolete-settled"->{regular(file,64);obsoleteSettled.add(id);}
                case "obsolete-settled.pending"->{regular(file,64);if(!exists(path(id,"obsolete")))throw new IOException("Obsolete settlement staging lacks terminal authority");}
                case "released"->{regular(file,RELEASE_BODY_BYTES+32);receipts.put(id,receipt(id));metadata=Math.addExact(metadata,RELEASE_METADATA_PROMISE);}
                case "settled"->{regular(file,64);settled.add(id);}
                case "prepared.pending"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source metadata staging quota exhausted");if(!exists(path(id,"prepared")))prepared.put(id,decodeMetadata(id,read(file,MAX_METADATA_FILE)));}
                case "capture.pending"->{regular(file,MAX_METADATA_FILE);metadata=Math.addExact(metadata,3L*Files.size(file));if(metadata>maximumMetadataBytes)throw new IOException("Source capture staging quota exhausted");if(!exists(path(id,"capture")))captures.put(id,decodeCapture(id,read(file,MAX_METADATA_FILE)));}
                case "reserved.pending"->throw new IOException("Uncertain disk reservation requires exact force repair");
                case "released.pending"->{regular(file,RELEASE_BODY_BYTES+32);long bytes=Files.size(file);releasePendingBytes.put(id,bytes);metadata=Math.addExact(metadata,bytes);}
                case "settled.pending"->{regular(file,64);if(!exists(path(id,"released")))throw new IOException("RELEASE settlement staging lacks canonical receipt");}
                default->throw new IOException("Uncertain source artifact file");
            }
        }}
        if(proposedRelease!=null){var before=receipts.putIfAbsent(proposedRelease.id(),proposedRelease);if(before!=null&&!Arrays.equals(encode(before),encode(proposedRelease)))throw new IOException("RELEASE candidate changed original receipt");if(before==null)metadata=Math.addExact(metadata,RELEASE_METADATA_PROMISE);}
        for(var entry:releasePendingBytes.entrySet())if(receipts.containsKey(entry.getKey()))metadata=Math.subtractExact(metadata,entry.getValue()); // The canonical/candidate RELEASE promise already covers its sidecar.
        for(var entry:pendingObsolete.entrySet()){obsoleteIds.add(entry.getKey());obsoleteBytes.merge(entry.getKey(),(long)encodeObsolete(entry.getValue()).length+32,Math::max);var before=obsoletes.putIfAbsent(entry.getKey(),entry.getValue());if(before!=null&&!Arrays.equals(encodeObsolete(before),encodeObsolete(entry.getValue())))throw new IOException("Warm obsolete terminal changed");}
        if(proposedObsolete!=null){UUID id=proposedObsolete.request().original().transferId();obsoleteIds.add(id);obsoleteBytes.merge(id,(long)encodeObsolete(proposedObsolete).length+32,Math::max);var before=obsoletes.putIfAbsent(id,proposedObsolete);if(before!=null&&!Arrays.equals(encodeObsolete(before),encodeObsolete(proposedObsolete)))throw new AdmissionRejected("Obsolete candidate changed original terminal");}
        for(long bytes:obsoleteBytes.values())metadata=Math.addExact(metadata,3L*bytes+128);
        var terminalIds=new HashSet<UUID>(obsoleteIds);terminalIds.addAll(publications.keySet());terminalIds.addAll(reservations.keySet());terminalIds.addAll(receipts.keySet());terminalIds.addAll(releasePendingBytes.keySet());if(terminalIds.size()>MAX_RELEASED)throw new IOException("Source terminal identity budget exhausted");
        if(candidate!=null&&!prepared.containsKey(candidate.request().transferId())){prepared.put(candidate.request().transferId(),candidate);metadata=Math.addExact(metadata,3L*(candidateBytes.length+32));}
        long images=0;var retained=new HashSet<UUID>();
        for(var entry:reservations.entrySet()){
            UUID id=entry.getKey();var reservation=entry.getValue();var value=prepared.get(id);var capture=captures.get(id);var receipt=receipts.get(id);
            if(value!=null){requireRequestIo(reservation.request(),value.request());if(value.image().bytes()>reservation.maximumImageBytes())throw new IOException("Source image exceeded durable promise");}
            if(capture!=null)requireRequestIo(reservation.request(),capture.request());if(receipt!=null)requireReceipt(reservation.request(),receipt);
            if(settled.contains(id))throw new IOException("Settled source transfer retained an unfinished disk promise");retained.add(id);images=Math.addExact(images,2L*reservation.maximumImageBytes());
            if(!obsoletes.containsKey(id) && receipt==null){long completion=value==null?remainingCapturePromise(reservation.request(),capture!=null):0;long obsolete=reservation.request().purpose()==Purpose.CURRENT_LEAF?obsoleteMetadataPromise(reservation.request()):0;long future=Math.max(completion,Math.max(RELEASE_METADATA_PROMISE,obsolete));future=Math.max(0,future-releasePendingBytes.getOrDefault(id,0L));metadata=Math.addExact(metadata,future);}
        }
        for(var entry:receipts.entrySet()){
            UUID id=entry.getKey();if(settled.contains(id)){if(!Arrays.equals(read(path(id,"settled"),64),digest(encode(entry.getValue()))))throw new IOException("Settled source release changed");SessionJournalFiles.force(path(id,"settled"));SessionJournalFiles.force(root);if(prepared.containsKey(id)||captures.containsKey(id)||imageIds.contains(id)||sourceIds.contains(id))throw new IOException("Settled source transfer retained owned artifacts");}
            else if(!retained.contains(id)){retained.add(id);images=Math.addExact(images,2L*entry.getValue().reservedImageBytes());}
        }
        for(var entry:obsoletes.entrySet()){
            UUID id=entry.getKey();var value=entry.getValue();var original=value.request().original();
            if(receipts.containsKey(id) || settled.contains(id) || recoveries.contains(id))throw new IOException("Obsolete disposition conflicts with installed release/recovery");
            if(reservations.containsKey(id))requireRequestIo(original,reservations.get(id).request());if(captures.containsKey(id))requireRequestIo(original,captures.get(id).request());if(prepared.containsKey(id))requireRequestIo(original,prepared.get(id).request());if(publications.containsKey(id))requireRequestIo(original,publications.get(id).request());
            if(obsoleteSettled.contains(id)){requireObsoleteSettled(value);if(reservations.containsKey(id)||prepared.containsKey(id)||captures.containsKey(id)||sourceIds.contains(id)||imageIds.contains(id))throw new IOException("Settled obsolete source retained owned artifacts");}
            else if(value.reservedImageBytes()>0 && retained.add(id))images=Math.addExact(images,2L*value.reservedImageBytes());
        }
        for(var id:obsoleteSettled)if(!obsoletes.containsKey(id) || !exists(path(id,"obsolete")))throw new IOException("Obsolete settlement lacks forced terminal authority");
        for(var entry:publications.entrySet()){
            var id=entry.getKey();var publication=entry.getValue();if(abandonedSettled.contains(id)){if(publication.phase()!=PublicationPhase.ABANDONED||!Arrays.equals(read(path(id,"abandoned-settled"),64),digest(encodePublication(publication))))throw new IOException("Abandoned source settlement changed");SessionJournalFiles.force(path(id,"abandoned-settled"));SessionJournalFiles.force(root);if(reservations.containsKey(id)||prepared.containsKey(id)||captures.containsKey(id)||sourceIds.contains(id)||imageIds.contains(id))throw new IOException("Abandoned settled transfer retained resources");}
            else if(publication.phase()==PublicationPhase.ABANDONED&&!obsoleteSettled.contains(id)&&!retained.contains(id)){retained.add(id);images=Math.addExact(images,2L*publication.maximumImageBytes());}
            if(prepared.containsKey(id)&&publication.phase()==PublicationPhase.READY&&!Arrays.equals(publication.preparedDigest(),digest(reply(PREPARE,prepared.get(id)))))throw new IOException("READY source metadata changed");
        }
        for(var id:abandonedSettled)if(!publications.containsKey(id))throw new IOException("Abandoned settlement lacks positive publication");
        for(var id:prepared.keySet())if(!reservations.containsKey(id)&&!receipts.containsKey(id))throw new IOException("Source image lacks durable disk reservation");for(var id:captures.keySet())if(!reservations.containsKey(id)&&!receipts.containsKey(id))throw new IOException("Source capture lacks durable disk reservation");for(var id:settled)if(!receipts.containsKey(id))throw new IOException("Source settled marker lacks release authority");
        for(var id:imageIds){var value=prepared.get(id);if(value==null&&!receipts.containsKey(id))throw new IOException("Image lacks forced original source metadata");long expected=value==null?receipts.get(id).imageBytes():value.image().bytes();if(exists(path(id,"image"))&&Files.size(path(id,"image"))!=expected || exists(path(id,"image.pending"))&&Files.size(path(id,"image.pending"))>expected)throw new IOException("Owned source image exceeds bound");}
        for(var id:sourceIds){var value=prepared.get(id);var captured=captures.get(id);var receipt=receipts.get(id);ApplyPosition cut=value!=null?value.image().position():captured!=null?captured.cut():receipt!=null?receipt.cut():publications.containsKey(id)?publications.get(id).cut():null;if(cut==null)throw new IOException("Original source directory lacks forced cut");var expected=sourcePath(id,cut).getFileName().toString();int count=0;try(var filesInSource=Files.newDirectoryStream(sourcePath(id,cut).getParent())){for(var file:filesInSource){if(++count>4)throw new IOException("Source original file count exceeded");var name=file.getFileName().toString();if(!name.equals(expected)&&!name.equals(expected+".pending")&&!name.matches("[.]native-range-building-.*[.]tmp"))throw new IOException("Foreign source original file");regular(file,imageLimits.maxImageBytes());}}}
        for(var id:recoveryDirectories)if(!recoveries.contains(id))throw new IOException("Recovery image lacks durable reservation");
        for(var id:recoveries){if(!retained.add(id))throw new IOException("Recovery/source reservation identity overlaps");images=Math.addExact(images,2L*imageLimits.maxImageBytes());if(recoveryDirectories.contains(id))RatisTopologyRecoveryAnchors.validateDirectory(path(id,"recovery"),imageLimits);}
        if(obsoleteIds.size()>MAX_RELEASED||publications.size()>MAX_RELEASED||receipts.size()>MAX_RELEASED||retained.size()>maximumArtifacts||images>maximumImageBytes||metadata>maximumMetadataBytes)throw new IOException("Source durable image/metadata quota exhausted");return new Usage(retained.size(),images,metadata);
    }
    private void publishImage(Path source,Path target,Prepared prepared)throws IOException {
        if(exists(target)){validateImage(prepared,target);SessionJournalFiles.force(target);SessionJournalFiles.force(target.getParent());return;}
        Path pending=target.resolveSibling(target.getFileName()+".pending");
        if(exists(pending)){regular(pending,prepared.image().bytes());requirePrefix(pending,source);Files.delete(pending);SessionJournalFiles.force(pending.getParent());}
        try{Files.createLink(pending,source);}catch(UnsupportedOperationException|IOException unavailable){if(exists(pending))throw new IOException("Uncertain source hardlink publication",unavailable);try(var input=Files.newInputStream(source);var output=Files.newOutputStream(pending,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){var buffer=new byte[65536];while(true){imageAdmission.get().beforeBytes(buffer.length);int count=input.read(buffer);if(count==-1)break;imageAdmission.get().beforeBytes(count);output.write(buffer,0,count);}}}
        validateImage(prepared,pending);SessionJournalFiles.force(pending);Files.move(pending,target,StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(target.getParent());
    }
    private NativeRangeImage.Manifest inspect(Path file,boolean metadataOnly)throws IOException {return inspect(file,metadataOnly,null);}
    private NativeRangeImage.Manifest inspect(Path file,boolean metadataOnly,NativeTopologyFreeze expectedFreeze)throws IOException {
        var admitted=new NativeRangeImage.IoAdmission() {
            public void beforeBytes(int bytes)throws IOException{imageAdmission.get().beforeBytes(bytes);}
            public AutoCloseable reserveMetadata(int bytes)throws IOException{return metadataAdmission.reserveMetadata(bytes);}
        };
        var freeze=new NativeTopologyFreeze[1];var image=metadataOnly?NativeRangeImage.visitMetadataAdmitted(file,imageLimits,(kind,column,key,value)->{if(expectedFreeze!=null && kind==NativeRangeImage.Section.STATE && metadataFreezeKey(expectedFreeze.groupId(),key)){if(freeze[0]!=null)throw new IOException("Duplicate metadata image FREEZE");freeze[0]=NativeTopologyFreeze.decode(value);}},admitted):NativeRangeImage.inspectAdmitted(file,imageLimits,admitted);
        if(expectedFreeze!=null && !expectedFreeze.equals(freeze[0]))throw new IOException("Prepared metadata FREEZE differs from actual image STATE");return image;
    }
    private void validateImage(Prepared prepared,Path file)throws IOException {
        regular(file,prepared.image().bytes());if(Files.size(file)!=prepared.image().bytes())throw new IOException("Source image length changed");var actual=inspect(file,prepared.request().metadataOnly(),prepared.metadataFreeze());var expected=prepared.image();
        if(!actual.owner().equals(expected.owner()) || !actual.interval().domainId().equals(expected.interval().domainId()) || !actual.interval().columns().equals(expected.interval().columns()) || !Arrays.equals(actual.interval().lowerInclusive(),expected.interval().lowerInclusive()) || !Arrays.equals(actual.interval().upperExclusive(),expected.interval().upperExclusive())
                || !actual.position().equals(expected.position()) || !Arrays.equals(actual.schemaDigest(),expected.schemaDigest()) || !actual.deterministicMergeColumns().equals(expected.deterministicMergeColumns()) || actual.rows()!=expected.rows() || actual.bytes()!=expected.bytes() || !Arrays.equals(actual.sha256(),expected.sha256())
                || !Arrays.equals(actual.topologyOrigin()==null?null:actual.topologyOrigin().encode(),expected.topologyOrigin()==null?null:expected.topologyOrigin().encode()))throw new IOException("Source image checksum/cut/schema/geometry changed");
    }
    private Prepared metadata(UUID id)throws IOException {var result=decodeMetadata(id,read(path(id,"prepared"),MAX_METADATA_FILE));SessionJournalFiles.force(path(id,"prepared"));SessionJournalFiles.force(root);return result;}
    private static Prepared decodeMetadata(UUID id,byte[] bytes)throws IOException {try{var request=requestFromReply(bytes);var prepared=reply(bytes,PREPARE,request);if(!request.transferId().equals(id))throw new IOException("Source metadata filename identity changed");return prepared;}catch(IllegalArgumentException malformed){throw new IOException("Invalid forced source metadata",malformed);}}
    private static Request requestFromReply(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=PREPARE)throw new IllegalArgumentException("Expected source PREPARE metadata");var in=ByteBuffer.wrap(packet.body());if(!in.hasRemaining() || in.get()!=1)throw new IllegalArgumentException("Expected READY source metadata");if(in.remaining()<4)throw new IllegalArgumentException("Truncated source intent");int size=in.getInt();if(size<0 || size>MAX_REQUEST_BYTES || size>in.remaining())throw new IllegalArgumentException("Invalid source intent length");byte[] body=new byte[size];in.get(body);return request(ScopedTransactionRpc.frame(PREPARE,body));}
    private Receipt receipt(UUID id)throws IOException {var value=decodeReceipt(read(path(id,"released"),RELEASE_BODY_BYTES+32),id);SessionJournalFiles.force(path(id,"released"));SessionJournalFiles.force(root);return value;}
    private Receipt decodeReceipt(byte[] bytes,UUID id)throws IOException {
        if(bytes.length!=RELEASE_BODY_BYTES)throw new IOException("Invalid source release receipt size");
        try{var in=ByteBuffer.wrap(bytes);if(in.getInt()!=RELEASE_MAGIC)throw new IOException("Invalid source release receipt header");UUID actual=new UUID(in.getLong(),in.getLong());byte[] request=new byte[32],metadata=new byte[32],image=new byte[32];in.get(request).get(metadata).get(image);long size=in.getLong(),reserved=in.getLong();var cut=new ApplyPosition(in.getLong(),in.getLong());int kind=in.get()&255;
            if(!actual.equals(id) || size<1 || size>imageLimits.maxImageBytes() || reserved<size || reserved>imageLimits.maxImageBytes() || cut.term()<1 || kind>1)throw new IOException("Invalid source release receipt identity/image bound");var value=new Receipt(id,request,metadata,image,size,reserved,cut,kind==1);if(!Arrays.equals(bytes,encode(value)))throw new IOException("Noncanonical RELEASE receipt");return value;
        }catch(IllegalArgumentException malformed){throw new IOException("Invalid RELEASE receipt",malformed);}
    }
    private Receipt completeReleasePending(UUID id)throws IOException {
        var pending=path(id,"released.pending");if(!exists(pending))return null;regular(pending,RELEASE_BODY_BYTES+32);var encoded=Files.readAllBytes(pending);
        if(encoded.length!=RELEASE_BODY_BYTES+32 || !MessageDigest.isEqual(digest(Arrays.copyOf(encoded,RELEASE_BODY_BYTES)),Arrays.copyOfRange(encoded,RELEASE_BODY_BYTES,encoded.length)))return null;
        return decodeReceipt(Arrays.copyOf(encoded,RELEASE_BODY_BYTES),id);
    }
    private Receipt expectedReleaseReceipt(Request request,boolean requireImage)throws IOException {
        UUID id=request.transferId();var prepared=decodeMetadata(id,read(path(id,"prepared"),MAX_METADATA_FILE));requireObsoleteOriginal(request,prepared.request());var reservation=reservation(id);requireObsoleteOriginal(request,reservation.request());
        if(requireImage){Path image=path(id,"image");if(!exists(image))image=sourcePath(id,prepared.image().position());if(!exists(image))throw new IOException("Pending RELEASE lacks retained original image");validateImage(prepared,image);}
        return new Receipt(id,digest(request(RELEASE,request)),digest(reply(PREPARE,prepared)),prepared.image().sha256(),prepared.image().bytes(),reservation.maximumImageBytes(),prepared.image().position(),request.metadataOnly());
    }
    private void requireNoInstalledRelease(Request request)throws IOException {
        UUID id=request.transferId();if(exists(path(id,"released"))){decodeReceipt(read(path(id,"released"),RELEASE_BODY_BYTES+32),id);throw new AdmissionRejected("Installed RELEASE is definitive; OBSOLETE cannot replace it");}
        if(exists(path(id,"settled")))throw new IOException("RELEASE settlement lost its canonical receipt");
        var pending=path(id,"released.pending");if(!exists(pending))return;
        var completed=completeReleasePending(id);var expected=expectedReleaseReceipt(request,completed!=null);
        if(completed!=null){if(!Arrays.equals(encode(completed),encode(expected)))throw new IOException("Complete pending RELEASE changed original authority");throw new AdmissionRejected("Complete installed RELEASE requires exact local RELEASE settlement");}
        byte[] body=encode(expected);requirePrefixBytes(pending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());
    }
    private static byte[] encode(Receipt value){return ByteBuffer.allocate(RELEASE_BODY_BYTES).putInt(RELEASE_MAGIC).putLong(value.id().getMostSignificantBits()).putLong(value.id().getLeastSignificantBits()).put(value.requestDigest()).put(value.metadataDigest()).put(value.imageDigest()).putLong(value.imageBytes()).putLong(value.reservedImageBytes()).putLong(value.cut().term()).putLong(value.cut().index()).put((byte)(value.metadataOnly()?1:0)).array();}
    private static void requireReceipt(Request request,Receipt receipt)throws IOException{if(!Arrays.equals(receipt.requestDigest(),digest(request(RELEASE,request))))throw new IOException("Released source request changed");}
    private static void requireRequestIo(Request expected,Request actual)throws IOException{try{requireRequest(expected,actual);}catch(IllegalArgumentException changed){throw new IOException("Original source request changed",changed);}}
    private static void requirePrepared(Prepared expected,Prepared actual)throws IOException{if(!Arrays.equals(reply(PREPARE,expected),reply(PREPARE,actual)))throw new IOException("Original source image metadata changed");}
    private synchronized void cleanMetadataPending(Path path,byte[] body)throws IOException{Path pending=path.resolveSibling(path.getFileName()+".pending");if(exists(pending)){regular(pending,MAX_METADATA_FILE);requirePrefixBytes(pending,ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).array());Files.delete(pending);SessionJournalFiles.force(path.getParent());}}
    private void requirePrefix(Path partial,Path source)throws IOException{try(var left=FileChannel.open(partial,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS);var right=FileChannel.open(source,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){if(left.size()>right.size())throw new IOException("Uncertain staging image length");var a=ByteBuffer.allocate(65536);var b=ByteBuffer.allocate(65536);long offset=0;while(offset<left.size()){int count=(int)Math.min(a.capacity(),left.size()-offset);a.clear().limit(count);b.clear().limit(count);imageAdmission.get().beforeBytes(count);fill(left,a);imageAdmission.get().beforeBytes(count);fill(right,b);if(Arrays.mismatch(a.array(),0,count,b.array(),0,count)!=-1)throw new IOException("Uncertain staging image changed");offset+=count;}}}
    private static void requirePrefixBytes(Path partial,byte[] source)throws IOException{try(var file=FileChannel.open(partial,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){if(file.size()>source.length)throw new IOException("Uncertain metadata staging length");var bytes=ByteBuffer.allocate((int)file.size());fill(file,bytes);if(Arrays.mismatch(bytes.array(),0,bytes.capacity(),source,0,bytes.capacity())!=-1)throw new IOException("Uncertain metadata staging changed");}}
    private static void fill(FileChannel file,ByteBuffer bytes)throws IOException{while(bytes.hasRemaining())if(file.read(bytes)<0)throw new IOException("Truncated source artifact");}
    private static byte[] read(Path file,int maximum)throws IOException{regular(file,maximum);Path pending=file.resolveSibling(file.getFileName()+".pending");if(exists(pending))regular(pending,maximum);return SessionJournalFiles.read(file,maximum);}
    private SnapshotInfo snapshot(Path path,Prepared prepared)throws IOException{return new SingleFileSnapshotInfo(new FileInfo(path,MD5Hash.newInstance(NativeRangeImage.md5Admitted(path,imageAdmission.get()))),prepared.image().position().term(),prepared.image().position().index());}
    private static String name(Prepared prepared){return SimpleStateMachineStorage.getSnapshotFileName(prepared.image().position().term(),prepared.image().position().index());}
    private Path path(UUID id,String suffix){return root.resolve(id+"."+suffix);}
    private static boolean exists(Path path){return Files.exists(path,LinkOption.NOFOLLOW_LINKS);}
    private static void directory(Path path)throws IOException{for(Path part=path.toAbsolutePath().normalize();part!=null;part=part.getParent())if(Files.isSymbolicLink(part))throw new IOException("Source artifact directory crosses a symlink");if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source artifact directory is absent");}
    private static void regular(Path path,long maximum)throws IOException{directory(path.getParent());if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)>maximum)throw new IOException("Source artifact is foreign/symlink/oversized");}
    private static byte[] digest(byte[] body){try{return MessageDigest.getInstance("SHA-256").digest(body);}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private void requireOpen()throws IOException{if(closed || !lock.isValid())throw new IOException("Source artifact owner closed/fenced");}
    @Override public synchronized void close()throws IOException{if(closed)return;if(!bulkOwners.isEmpty())throw new IOException("Actual source artifact work remains active");closed=true;try{lock.release();}finally{lockChannel.close();}}
}
