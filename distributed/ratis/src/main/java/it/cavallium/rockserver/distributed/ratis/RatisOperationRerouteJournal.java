package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Forced immutable original input; this owner performs no remote work or replacement capture. */
public final class RatisOperationRerouteJournal implements AutoCloseable {
    private static final int MAGIC=0x524f524a;
    public static final int MAX_BYTES=4+32+8+5*4+2*RangeDirectoryCodec.MAX_ENCODED_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES+OperationRerouteOrigin.MAX_BYTES+RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES+32;
    @FunctionalInterface interface Persistence { void write(Path path,byte[] body,int maximum)throws IOException; }
    private final Path path;
    private final byte[] binding,body;
    private final RangeDirectory source,target;
    private final DistributedCommand command;
    private final OperationRerouteOrigin origin;
    private final RatisTopologyOperationOutcomeRpc.Reply absence;
    private final Duration ttl;
    private final FileChannel channel;
    private final FileLock lock;
    private boolean closed;
    public static RatisOperationRerouteJournal create(Path path,byte[] binding,RangeDirectory source,DistributedCommand command,RangeDirectory target,OperationRerouteOrigin origin,RatisTopologyOperationOutcomeRpc.Reply absence,Duration ttl)throws IOException {
        return create(path,binding,source,command,target,origin,absence,ttl,SessionJournalFiles::write);
    }
    static RatisOperationRerouteJournal create(Path path,byte[] binding,RangeDirectory source,DistributedCommand command,RangeDirectory target,OperationRerouteOrigin origin,RatisTopologyOperationOutcomeRpc.Reply absence,Duration ttl,Persistence io)throws IOException {
        return new RatisOperationRerouteJournal(path,binding,encode(source,command,target,origin,absence,ttl,binding),Objects.requireNonNull(io));
    }
    public static RatisOperationRerouteJournal reopen(Path path,byte[] binding)throws IOException {return new RatisOperationRerouteJournal(path,binding,null,null);}
    private RatisOperationRerouteJournal(Path file,byte[] expected,byte[] requested,Persistence io)throws IOException {
        path=Objects.requireNonNull(file).toAbsolutePath().normalize();requireDigest(expected);binding=expected.clone();
        if(path.getParent()==null || !Files.isDirectory(path.getParent(),LinkOption.NOFOLLOW_LINKS))throw new IOException("Reroute journal parent is absent");
        var lockPath=path.resolveSibling(path.getFileName()+".lock");
        if(Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS) && (!Files.isRegularFile(lockPath,LinkOption.NOFOLLOW_LINKS) || Files.size(lockPath)!=0))throw new IOException("Invalid reroute journal lock");
        channel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try {
            try{acquired=channel.tryLock();}catch(OverlappingFileLockException owned){throw new IOException("Reroute journal already owned",owned);}
            if(acquired==null)throw new IOException("Reroute journal already owned");lock=acquired;
            var pending=path.resolveSibling(path.getFileName()+".pending");
            boolean create=requested!=null;
            if(create && Files.exists(pending,LinkOption.NOFOLLOW_LINKS))throw new IOException("Unpublished reroute sidecar requires inspection");
            if(create==Files.exists(path,LinkOption.NOFOLLOW_LINKS))throw new IOException(create ? "Reroute journal already exists" : "Reroute journal absent");
            body=create ? requested : read(path);
            var in=ByteBuffer.wrap(body);if(in.getInt()!=MAGIC)throw new IOException("Invalid reroute journal tag");byte[] stored=new byte[32];in.get(stored);
            if(!Arrays.equals(stored,binding))throw new IOException("Reroute journal binding changed");ttl=Duration.ofMillis(in.getLong());
            source=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));command=DistributedCommandCodec.decode(blob(in,DistributedCommandCodec.MAX_ENCODED_BYTES));target=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));origin=OperationRerouteOrigin.decode(blob(in,OperationRerouteOrigin.MAX_BYTES));
            absence=RatisTopologyOperationOutcomeRpc.decodeReply(blob(in,RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES),request(source,command,origin));
            if(in.hasRemaining() || !Arrays.equals(body,encode(source,command,target,origin,absence,ttl,binding)))throw new IOException("Noncanonical reroute journal");
            if(create)io.write(path,body,MAX_BYTES);else {
                SessionJournalFiles.force(path);
                if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)) {
                    if(!Arrays.equals(body,read(pending)))throw new IOException("Unpublished reroute sidecar differs from durable input");
                    Files.delete(pending);
                }
                SessionJournalFiles.force(path.getParent());
            }
        }catch(IOException|RuntimeException|Error failure){try{if(acquired!=null)acquired.release();}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{channel.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}if(failure instanceof RuntimeException invalid)throw new IOException("Invalid reroute journal",invalid);throw failure;}
    }
    private static byte[] encode(RangeDirectory source,DistributedCommand command,RangeDirectory target,OperationRerouteOrigin origin,RatisTopologyOperationOutcomeRpc.Reply absence,Duration ttl,byte[] binding) {
        requireDigest(binding);Objects.requireNonNull(source);Objects.requireNonNull(command);Objects.requireNonNull(target);Objects.requireNonNull(origin);Objects.requireNonNull(absence);Objects.requireNonNull(ttl);
        var route=source.ranges().stream().filter(r->r.groupId().equals(command.groupId())).findFirst().orElseThrow();var lineage=origin.source();
        if(ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))
                || command.kind()!=DistributedCommand.Kind.DATA && command.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED
                || !source.clusterId().equals(origin.clusterId()) || !source.databaseId().equals(origin.databaseId()) || !source.domainId().equals(origin.domainId())
                || !source.clusterId().equals(target.clusterId()) || !source.databaseId().equals(target.databaseId()) || !source.domainId().equals(target.domainId())
                || source.schemaEpoch()!=target.schemaEpoch() || !Arrays.equals(source.schemaDigest(),target.schemaDigest()) || !source.columns().equals(target.columns()) || source.routingPrefixBytes()!=target.routingPrefixBytes() || target.generation()<=source.generation()
                || !route.rangeId().equals(lineage.rangeId()) || !route.groupId().equals(lineage.groupId()) || route.rangeEpoch()!=lineage.rangeEpoch() || route.rangeEpoch()!=command.rangeEpoch()
                || !Arrays.equals(source.schemaDigest(),command.schemaDigest()) || !command.operationId().equals(origin.operation()) || !Arrays.equals(command.receiptDigest(),origin.originalReceiptDigest()) || !Arrays.equals(target.payloadDigest(),origin.targetDirectoryDigest())
                || absence.freeze().header().schemaEpoch()!=source.schemaEpoch() || !Arrays.equals(absence.freeze().header().schemaDigest(),source.schemaDigest())
                || !absence.request().equals(request(source,command,origin)) || absence.outcome()!=RatisTopologyOperationOutcomeRpc.Outcome.ABSENT || !absence.cut().equals(origin.absenceCut()) || !Arrays.equals(OperatorUpload.sha256(absence.freeze().encode()),origin.absenceCommitment()))throw new IllegalArgumentException("Reroute input binding changed");
        for(var mutation:command.mutations()) {
            if(mutation.kind()==Mutation.Kind.DELETE_RANGE) {
                var ranges=source.intersect(mutation.columnId(),mutation.key(),mutation.value(),false);
                if(ranges.size()!=1 || !ranges.getFirst().equals(route))throw new IllegalArgumentException("Original mutation crosses its frozen source range");
            }else if(!source.route(mutation.columnId(),mutation.key()).equals(route))throw new IllegalArgumentException("Original mutation crosses its frozen source range");
        }
        for(var predicate:command.predicates())if(!source.route(predicate.columnId(),predicate.key()).equals(route))throw new IllegalArgumentException("Original predicate crosses its frozen source range");
        if(command.acceptance()!=null && !command.acceptance().scope().participants(source,1).equals(List.of(route)))throw new IllegalArgumentException("Original acceptance crosses its frozen source range");
        if(command.acceptance()!=null || !command.mutations().isEmpty() || !command.predicates().isEmpty()) {
            if(origin.targetParticipantCount()!=RatisOperationRerouteAdmissionRpc.targetParticipantCount(command,target) || !Arrays.equals(origin.planDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(command,target)))throw new IllegalArgumentException("Reroute target input binding changed");
        }
        // ACK-only input is retained; admission still requires original-parent ACK authority.
        byte[][] images={RangeDirectoryCodec.encode(source),DistributedCommandCodec.encode(command),RangeDirectoryCodec.encode(target),origin.encode(),RatisTopologyOperationOutcomeRpc.reply(absence)};int size=44;for(var image:images)size=Math.addExact(size,4+image.length);
        if(size+32>MAX_BYTES)throw new IllegalArgumentException("Reroute journal exceeds bound");var out=ByteBuffer.allocate(size).putInt(MAGIC).put(binding).putLong(ttl.toMillis());for(var image:images)out.putInt(image.length).put(image);return out.array();
    }
    private static RatisTopologyOperationOutcomeRpc.Request request(RangeDirectory source,DistributedCommand command,OperationRerouteOrigin origin){return new RatisTopologyOperationOutcomeRpc.Request(source.clusterId(),source.databaseId(),source.domainId(),origin.source(),command.operationId(),command.receiptDigest());}
    private static byte[] blob(ByteBuffer in,int maximum){int length=in.getInt();if(length<1 || length>maximum || length>in.remaining())throw new IllegalArgumentException("Invalid reroute image size");byte[] bytes=new byte[length];in.get(bytes);return bytes;}
    private static void requireDigest(byte[] value){if(value==null || value.length!=32)throw new IllegalArgumentException("Reroute binding requires 32 bytes");}
    private static byte[] read(Path path)throws IOException {
        try(var file=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            long size=file.size();if(size<44+5*4+32 || size>MAX_BYTES)throw new IOException("Invalid reroute journal size");var bytes=ByteBuffer.allocate((int)size);while(bytes.hasRemaining())if(file.read(bytes)<0)throw new IOException("Truncated reroute journal");if(file.size()!=size)throw new IOException("Reroute journal changed during read");
            byte[] encoded=bytes.array(),body=Arrays.copyOf(encoded,encoded.length-32);if(!java.security.MessageDigest.isEqual(OperatorUpload.sha256(body),Arrays.copyOfRange(encoded,body.length,encoded.length)))throw new IOException("Reroute journal checksum mismatch");return body;
        }
    }
    public RangeDirectory sourceDirectory(){return source;}public DistributedCommand originalCommand(){return command;}public RangeDirectory targetDirectory(){return target;}public OperationRerouteOrigin origin(){return origin;}public RatisTopologyOperationOutcomeRpc.Reply absence(){return absence;}public Duration captureTtl(){return ttl;}
    public Path atomicPath(){return path.resolveSibling(path.getFileName()+".atomic");}
    public RatisRoutedClient.PreparedCommand original(RatisRoutedClient routed){var route=source.ranges().stream().filter(r->r.groupId().equals(command.groupId())).findFirst().orElseThrow();return routed.restoreSessionCommand(source,command,route);}
    public void requireInput(RangeDirectory source,DistributedCommand command,RangeDirectory target,OperationRerouteOrigin origin,RatisTopologyOperationOutcomeRpc.Reply absence,Duration ttl)throws IOException {if(!Arrays.equals(body,encode(source,command,target,origin,absence,ttl,binding)))throw new IOException("Reroute original input changed");}
    @Override public synchronized void close()throws IOException {if(closed && !channel.isOpen())return;closed=true;try{if(lock.isValid())lock.release();}finally{channel.close();}}
}
