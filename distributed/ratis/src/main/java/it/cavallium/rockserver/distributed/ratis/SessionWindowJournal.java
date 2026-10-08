package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Exclusive bounded durable session window. Caller serializes journal transitions and retains ownership of concurrent RPCs. */
final class SessionWindowJournal implements AutoCloseable {
    static final int MAX_BYTES=64*1024*1024;
    static final long MAX_WINDOW_BYTES=16L*1024*1024;
    private static final int MAGIC=0x53574a4c, VERSION=1, HEADER_BYTES=121;
    record Saved(SessionEpochLedger.Grant grant,RatisSessionClient.Limits limits,RatisSessionClient.WindowState state) {}
    private final Path path;
    private final RatisRoutedClient routed;
    private final byte[] identity;
    private final FileChannel channel;
    private final FileLock lock;
    private Saved saved;
    private boolean closed,fenced;

    static SessionWindowJournal open(Path path,RatisRoutedClient routed,boolean create) throws IOException {
        return new SessionWindowJournal(path,routed,create);
    }
    private SessionWindowJournal(Path path,RatisRoutedClient routed,boolean create) throws IOException {
        this.path=path.toAbsolutePath(); this.routed=Objects.requireNonNull(routed); identity=routed.sessionIdentityDigest();
        if (!Files.isDirectory(this.path.getParent())) throw new IOException("Session window journal parent is absent");
        channel=FileChannel.open(this.path.resolveSibling(this.path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Session window journal already owned",owned); }
            if (acquired==null) throw new IOException("Session window journal already owned"); lock=acquired;
            if (create==Files.exists(this.path)) throw new IOException(create ? "Session window journal already exists" : "Session window journal is absent");
            if (!create) {
                byte[] body=SessionJournalFiles.read(this.path,MAX_BYTES); saved=decode(body);
                SessionJournalFiles.force(this.path); SessionJournalFiles.force(this.path.getParent());
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    synchronized Saved read() throws IOException {
        requireOpen(); if (saved==null) throw new IOException("Session window journal has not been initialized"); return saved;
    }
    synchronized void write(SessionEpochLedger.Grant grant,RatisSessionClient.Limits limits,RatisSessionClient.WindowState state) throws IOException {
        requireOpen();
        try {
            var next=new Saved(Objects.requireNonNull(grant),Objects.requireNonNull(limits),Objects.requireNonNull(state));
            if (saved!=null && (!saved.grant().equals(grant) || !saved.limits().equals(limits))) throw new IOException("Session window grant/limits changed");
            int size=encodedSize(next);
            var restored=RatisSessionClient.restore(routed,grant,limits,state).snapshot();
            var canonical=new Saved(grant,limits,restored);
            if (encodedSize(canonical)!=size) throw new IOException("Session window changed during canonical restoration");
            byte[] body=encode(canonical,size);
            SessionJournalFiles.write(path,body,MAX_BYTES); saved=canonical;
        } catch (IOException failure) { fenced=true; throw failure; }
        catch (RuntimeException invalid) {
            fenced=true; throw new IOException("Invalid bounded session window state",invalid);
        }
    }
    private void requireOpen() throws IOException {
        if (closed || fenced || !lock.isValid()) throw new IOException("Session window journal is closed/fenced; reopen it");
    }
    @Override public synchronized void close() throws IOException {
        if (closed && !channel.isOpen()) return; closed=true;
        IOException failure=null;
        try { if (lock.isValid()) lock.release(); } catch (IOException cleanup) { failure=cleanup; }
        try { channel.close(); } catch (IOException cleanup) { if (failure==null) failure=cleanup; else failure.addSuppressed(cleanup); }
        if (failure!=null) throw failure;
    }
    private Saved decode(byte[] body) throws IOException {
        try {
            if (body.length<HEADER_BYTES) throw new IOException("Truncated session window journal");
            var input=ByteBuffer.wrap(body);
            if (input.getInt()!=MAGIC || input.getInt()!=VERSION) throw new IOException("Unsupported session window journal");
            byte[] bound=new byte[32]; input.get(bound); if (!Arrays.equals(bound,identity)) throw new IOException("Wrong session window journal identity");
            var grant=new SessionEpochLedger.Grant(input.getLong(),uuid(input),input.getLong(),input.getLong());
            var limits=new RatisSessionClient.Limits(input.getInt(),input.getLong(),input.getLong()); validateLimits(limits);
            long sequence=input.getLong(), acknowledged=input.getLong(); boolean exhausted=flag(input); int count=input.getInt();
            if (count<0 || count>limits.outstanding() || (long)count*70>input.remaining()) throw new IOException("Invalid session window entry count");
            var entries=new ArrayList<RatisSessionClient.WindowEntry>(count);
            for (int i=0;i<count;i++) {
                byte[] encoded=blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false);
                var command=DistributedCommandCodec.decode(encoded);
                if (!Arrays.equals(encoded,DistributedCommandCodec.encode(command))) throw new IOException("Noncanonical session window command");
                var descriptor=route(input); var prepared=routed.restoreSessionCommand(command,descriptor);
                byte[] resultBytes=blob(input,CommandResult.MAX_BYTES,true);
                var result=resultBytes==null ? null : CommandResult.decode(resultBytes);
                if (result!=null && !Arrays.equals(resultBytes,result.encode())) throw new IOException("Noncanonical session window result");
                entries.add(new RatisSessionClient.WindowEntry(prepared,result,flag(input)));
            }
            if (input.hasRemaining()) throw new IOException("Trailing session window journal data");
            var state=new RatisSessionClient.WindowState(sequence,acknowledged,exhausted,List.copyOf(entries));
            var canonical=new Saved(grant,limits,RatisSessionClient.restore(routed,grant,limits,state).snapshot());
            int size=encodedSize(canonical);
            if (!Arrays.equals(body,encode(canonical,size))) throw new IOException("Noncanonical session window journal");
            return canonical;
        } catch (RuntimeException invalid) { throw new IOException("Invalid session window journal",invalid); }
    }
    private static int encodedSize(Saved saved) throws IOException {
        validateLimits(saved.limits()); var entries=saved.state().entries();
        if (entries.size()>saved.limits().outstanding()) throw new IOException("Session window entry count exceeds bound");
        long size=HEADER_BYTES;
        for (var entry:entries) {
            int command=DistributedCommandCodec.encodedSize(entry.prepared().command());
            if (command<1 || command>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IOException("Session command blob exceeds bound");
            long result=entry.result()==null ? 0 : resultSize(entry.result());
            size+=4L+command+routeSize(entry.prepared().descriptor())+4+result+1;
            if (size>MAX_BYTES-32) throw new IOException("Session window journal exceeds aggregate byte bound");
        }
        return (int)size;
    }
    private static void validateLimits(RatisSessionClient.Limits limits) throws IOException {
        if (limits.outstanding()<1 || limits.outstanding()>4096 || limits.commandBytes()<1 || limits.commandBytes()>MAX_WINDOW_BYTES
                || limits.resultBytes()<8 || limits.resultBytes()>MAX_WINDOW_BYTES) throw new IOException("Durable session byte/count limits exceed bound");
    }
    private static long resultSize(CommandResult result) throws IOException {
        long size=8;
        for (int i=0;i<result.valueCount();i++) { byte[] value=result.value(i); size+=4L+(value==null ? 0 : value.length); }
        if (size>CommandResult.MAX_BYTES) throw new IOException("Session result blob exceeds bound"); return size;
    }
    private static long routeSize(RangeDescriptor route) throws IOException {
        if (route.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IOException("Session route is inactive");
        byte[] lower=route.lowerInclusive(), upper=route.upperExclusive();
        long size=60L+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length);
        for (var replica:route.replicas()) size+=57L+ascii(replica.peerId(),128).length+ascii(replica.address(),256).length;
        return size;
    }
    private byte[] encode(Saved saved,int size) {
        var output=ByteBuffer.allocate(size).putInt(MAGIC).putInt(VERSION).put(identity); var grant=saved.grant(); var limits=saved.limits(); var state=saved.state();
        output.putLong(grant.epoch()); uuid(output,grant.sessionId()); output.putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis());
        output.putInt(limits.outstanding()).putLong(limits.commandBytes()).putLong(limits.resultBytes());
        output.putLong(state.nextSequence()).putLong(state.acknowledgedThrough()).put((byte)(state.exhausted() ? 1 : 0)).putInt(state.entries().size());
        for (var entry:state.entries()) {
            blob(output,DistributedCommandCodec.encode(entry.prepared().command())); route(output,entry.prepared().descriptor());
            blob(output,entry.result()==null ? null : entry.result().encode()); output.put((byte)(entry.consumed() ? 1 : 0));
        }
        return output.array();
    }
    private static void route(ByteBuffer output,RangeDescriptor route) {
        uuid(output,route.rangeId()); uuid(output,route.groupId()); output.putLong(route.rangeEpoch()).putLong(route.configurationVersion());
        blob(output,route.lowerInclusive()); blob(output,route.upperExclusive()); output.putInt(route.replicas().size());
        for (var replica:route.replicas()) {
            uuid(output,replica.nodeId()); uuid(output,replica.nodeIncarnation()); uuid(output,replica.replicaId());
            blob(output,ascii(replica.peerId(),128)); blob(output,ascii(replica.address(),256));
            output.put((byte)(replica.role()==ReplicaDescriptor.Role.VOTER ? 0 : 1));
        }
    }
    private static RangeDescriptor route(ByteBuffer input) throws IOException {
        require(input,48); UUID range=uuid(input), group=uuid(input); long epoch=input.getLong(), configuration=input.getLong();
        byte[] lower=blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES,true), upper=blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES,true);
        require(input,4); int count=input.getInt();
        if (count<1 || count>RangeDirectoryCodec.MAX_REPLICAS || (long)count*59>input.remaining()) throw new IOException("Invalid session route replica count");
        var replicas=new ArrayList<ReplicaDescriptor>(count);
        for (int i=0;i<count;i++) {
            require(input,48); UUID node=uuid(input), incarnation=uuid(input), replica=uuid(input);
            String peer=ascii(blob(input,128,false),128), address=ascii(blob(input,256,false),256); boolean listener=flag(input);
            replicas.add(new ReplicaDescriptor(node,incarnation,replica,peer,address,listener ? ReplicaDescriptor.Role.LISTENER : ReplicaDescriptor.Role.VOTER));
        }
        return new RangeDescriptor(range,group,epoch,configuration,lower,upper,RangeDescriptor.TopologyState.ACTIVE,replicas);
    }
    private static byte[] ascii(String value,int maximum) {
        if (value==null || value.isEmpty() || value.length()>maximum) throw new IllegalArgumentException("Invalid session route string length");
        byte[] bytes=value.getBytes(StandardCharsets.US_ASCII);
        for (int i=0;i<value.length();i++) if (value.charAt(i)>127) throw new IllegalArgumentException("Non-ASCII session route string");
        return bytes;
    }
    private static String ascii(byte[] bytes,int maximum) throws IOException {
        if (bytes.length<1 || bytes.length>maximum) throw new IOException("Invalid session route string size");
        for (byte value:bytes) if (value<0) throw new IOException("Non-ASCII session route string");
        return new String(bytes,StandardCharsets.US_ASCII);
    }
    private static byte[] blob(ByteBuffer input,int maximum,boolean nullable) throws IOException {
        require(input,4); int size=input.getInt(); if (size== -1 && nullable) return null;
        if (size<0 || size>maximum || size>input.remaining()) throw new IOException("Invalid session window blob size");
        byte[] bytes=new byte[size]; input.get(bytes); return bytes;
    }
    private static void blob(ByteBuffer output,byte[] bytes) { output.putInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.put(bytes); }
    private static boolean flag(ByteBuffer input) throws IOException { require(input,1); int flag=input.get() & 255; if (flag>1) throw new IOException("Invalid session window flag"); return flag==1; }
    private static void require(ByteBuffer input,int bytes) throws IOException { if (input.remaining()<bytes) throw new IOException("Truncated session window journal"); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
}
