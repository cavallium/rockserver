package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/** Forced canonical metadata and promise ceilings. These are not filesystem or LSM-space reservations. */
final class RatisMetadataClaims {
    static final long POOL_BYTES=(144L<<20)+185;
    private static final int MAX_CLAIMS=DistributedStoragePool.MAX_RETAINED_METADATA_OWNERS*RatisNodeResourceBudget.MAX_POOLS,RECORD_BYTES=73,MAX_BYTES=40+MAX_CLAIMS*RECORD_BYTES;
    static final long JOURNAL_BYTES=2L*MAX_BYTES;
    record Claim(UUID pool,UUID group,RatisNodePlan.Role role,long bytes,byte[] policy) {
        Claim {Objects.requireNonNull(pool);Objects.requireNonNull(group);Objects.requireNonNull(role);if(bytes<1 || bytes>RatisNodeResourceBudget.MAX_BYTES || policy.length!=32)throw new IllegalArgumentException("Invalid metadata claim");policy=policy.clone();}
        @Override public byte[] policy(){return policy.clone();}
        boolean same(Claim other){return pool.equals(other.pool)&&group.equals(other.group)&&role==other.role&&bytes==other.bytes&&Arrays.equals(policy,other.policy);}
    }
    @FunctionalInterface interface Writer {void write(Path path,byte[] bytes)throws IOException;}
    private final Path path;private final long nodeLimit;private final Map<UUID,Long> poolLimits;private final Writer writer;private Map<UUID,Claim> claims;
    RatisMetadataClaims(Path path,long nodeLimit,Map<UUID,Long> pools)throws IOException{this(path,nodeLimit,pools,(file,bytes)->SessionJournalFiles.write(file,bytes,MAX_BYTES));}
    RatisMetadataClaims(Path path,long nodeLimit,Map<UUID,Long> pools,Writer writer)throws IOException {
        this.path=path;this.nodeLimit=nodeLimit;poolLimits=Map.copyOf(pools);this.writer=writer;recoverPending();claims=load();validate(claims);forceExisting();
    }
    synchronized void reserve(Collection<Claim> requested)throws IOException {
        var current=load();validate(current);forceExisting();var next=new HashMap<>(current);
        for(var claim:requested){var previous=next.putIfAbsent(claim.group(),claim);if(previous!=null&&!previous.same(claim))throw new IOException("Retained metadata claim changed pool/role/effective policy");}
        validate(next);if(next.equals(current)&&Files.exists(path)){claims=current;return;}writer.write(path,encode(next));claims=Map.copyOf(next);
    }
    synchronized long bytes(){return total(claims);}
    synchronized int groups(){return claims.size();}
    private long total(Map<UUID,Claim> values){long total=JOURNAL_BYTES+Math.multiplyExact(POOL_BYTES,poolLimits.size());for(var claim:values.values())total=Math.addExact(total,claim.bytes());return total;}
    private void validate(Map<UUID,Claim> values)throws IOException {
        if(values.size()>MAX_CLAIMS || total(values)>nodeLimit)throw new IOException("Node durable metadata ceiling exhausted");var sums=new HashMap<UUID,Long>();for(var id:poolLimits.keySet())sums.put(id,POOL_BYTES);
        for(var claim:values.values()){if(!sums.containsKey(claim.pool()))throw new IOException("Retained metadata claim belongs to an undeclared pool");sums.compute(claim.pool(),(id,bytes)->Math.addExact(bytes,claim.bytes()));}for(var entry:sums.entrySet())if(entry.getValue()>poolLimits.get(entry.getKey()))throw new IOException("Physical pool durable metadata ceiling exhausted");
    }
    private Map<UUID,Claim> load()throws IOException{if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS))return Map.of();if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid owned metadata claims file");return decode(SessionJournalFiles.read(path,MAX_BYTES));}
    private void forceExisting()throws IOException{if(Files.exists(path)){SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());}}
    private void recoverPending()throws IOException {
        var previous=load();validate(previous);forceExisting();var pending=path.resolveSibling(path.getFileName()+".pending");
        if(!Files.exists(pending,LinkOption.NOFOLLOW_LINKS))return;
        if(!Files.isRegularFile(pending,LinkOption.NOFOLLOW_LINKS) || Files.size(pending)>MAX_BYTES)throw new IOException("Invalid metadata claim sidecar");
        final byte[] encoded;try(var channel=java.nio.channels.FileChannel.open(pending,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){
            long size=channel.size();if(size>MAX_BYTES)throw new IOException("Metadata sidecar grew beyond bound");encoded=new byte[(int)size];var input=ByteBuffer.wrap(encoded);while(input.hasRemaining())if(channel.read(input)<0)throw new IOException("Metadata sidecar changed while reading");if(channel.size()!=size)throw new IOException("Metadata sidecar changed while reading");
        }
        byte[] bytes=encoded.length<32?null:Arrays.copyOf(encoded,encoded.length-32);
        if(bytes==null || !java.security.MessageDigest.isEqual(OperatorUpload.sha256(bytes),Arrays.copyOfRange(encoded,encoded.length-32,encoded.length))){
            // This bounded unpublished file cannot have authorized native effects. The committed ledger was validated first.
            Files.delete(pending);SessionJournalFiles.force(path.getParent());return;
        }
        var proposed=decode(bytes);for(var claim:previous.values())if(!claim.same(proposed.getOrDefault(claim.group(),claim)) || !proposed.containsKey(claim.group()))throw new IOException("Metadata sidecar changed retained claim");
        validate(proposed);SessionJournalFiles.force(pending);Files.move(pending,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);forceExisting();
    }
    private static byte[] encode(Map<UUID,Claim> values){var out=ByteBuffer.allocate(8+RECORD_BYTES*values.size());out.putInt(0x4d434c4d).putInt(values.size());values.values().stream().sorted(Comparator.comparing(Claim::group)).forEach(claim->{uuid(out,claim.pool());uuid(out,claim.group());out.put((byte)claim.role().ordinal()).putLong(claim.bytes()).put(claim.policy);});return out.array();}
    private static Map<UUID,Claim> decode(byte[] bytes)throws IOException {try{var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x4d434c4d)throw new IOException("Invalid metadata claims");int count=in.getInt();if(count<0 || count>MAX_CLAIMS || in.remaining()!=count*RECORD_BYTES)throw new IOException("Invalid metadata claim count");var values=new HashMap<UUID,Claim>();for(int n=0;n<count;n++){UUID pool=uuid(in),group=uuid(in);int role=in.get()&255;long charge=in.getLong();byte[] policy=new byte[32];in.get(policy);if(role>=RatisNodePlan.Role.values().length || values.put(group,new Claim(pool,group,RatisNodePlan.Role.values()[role],charge,policy))!=null)throw new IOException("Invalid metadata claim identity");}if(!Arrays.equals(bytes,encode(values)))throw new IOException("Noncanonical metadata claims");return Map.copyOf(values);}catch(IllegalArgumentException|java.nio.BufferUnderflowException invalid){throw new IOException("Invalid metadata claims",invalid);}}
    private static void uuid(ByteBuffer buffer,UUID id){buffer.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());}private static UUID uuid(ByteBuffer buffer){return new UUID(buffer.getLong(),buffer.getLong());}
    static Claim claim(UUID pool,UUID group,RatisNodePlan.Role role,RatisRangeStateMachine.RetentionLimits retention,RatisSchemaReplicaService.Limits operators){
        long receiptCount=Math.addExact(retention.dataCount(),retention.maintenanceCount()),receiptBytes=Math.addExact(retention.dataBytes(),retention.maintenanceBytes());long bytes=receiptBytes+61L*receiptCount+53L*ReplicatedRangeStore.MAX_SESSION_ACK_NAMESPACES+202058;
        switch(role){case TRANSACTION_HOME->bytes+=32L<<20;case SNAPSHOT_HOME->bytes+=16L<<20;case CATALOGUE->bytes+=CatalogueHistory.MAX_BYTES+64L*(16+RangeDirectoryCodec.MAX_ENCODED_BYTES)+16+SessionEpochLedger.MAX_BYTES+2L*((64L<<20)+13)+operators.maximumDiskBytes()+66405L*operators.maximumUploads()+33;case DATA->{long manifest=RatisRangeImportRecord.MAX_MANIFEST_BYTES;bytes+=(128L<<20)+97+132L*33+294+(16L<<20)+64L*33+2L*(17+NativeTopologyLearnerImagePermission.MAX_BYTES)+130;bytes+=17+manifest+Math.max(17+manifest+24,17+88+17+168)+25+4096L*249;bytes+=4L*17+NativeTopologyFreeze.MAX_BYTES+NativeTopologyBirth.BYTES+NativeTopologyActivation.MAX_BYTES+32;}}
        var policy=ByteBuffer.allocate(53).put((byte)role.ordinal()).putLong(retention.dataCount()).putLong(retention.dataBytes()).putLong(retention.maintenanceCount()).putLong(retention.maintenanceBytes()).putInt(role==RatisNodePlan.Role.CATALOGUE?operators.maximumUploads():0).putLong(role==RatisNodePlan.Role.CATALOGUE?operators.maximumDiskBytes():0).putLong(bytes).array();return new Claim(pool,group,role,bytes,OperatorUpload.sha256(policy));
    }
}
