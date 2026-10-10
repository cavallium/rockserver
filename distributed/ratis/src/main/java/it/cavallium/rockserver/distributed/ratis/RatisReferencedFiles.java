package it.cavallium.rockserver.distributed.ratis;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Group-owned forced immutable payloads. Borrowed executor; observers never own actual publication. */
public final class RatisReferencedFiles implements StateMachine.DataApi,AutoCloseable {
    public record Limits(int maxReferences,int maxStreams,int maxPendingOperations,long maxFileBytes,long maxContentBytes) {
        public Limits { if(maxReferences<1 || maxReferences>4096 || maxStreams<1 || maxPendingOperations<1 || maxFileBytes<1 || maxFileBytes>Integer.MAX_VALUE || maxContentBytes<maxFileBytes) throw new IllegalArgumentException("Invalid referenced file limits"); }
        public static Limits defaults() { return new Limits(4096,8,32,32L*1024*1024,256L*1024*1024); }
    }
    enum PublishStage { FILE_FORCED,CONTENT_PUBLISHED,DIRECTORY_FORCED,REFERENCE_FORCED }
    @FunctionalInterface interface PublicationObserver { void at(PublishStage stage) throws IOException; }
    private final Path directory,content,incoming,journal; private final UUID group; private final Limits limits; private final Executor executor; private final FileChannel lockChannel; private final FileLock lock;
    private final TreeMap<Long,RatisReferencedFile> references=new TreeMap<>(); private final TreeMap<Long,CompletableFuture<?>> actual=new TreeMap<>();
    private final Map<String,Integer> readers=new HashMap<>(); private final Map<String,Long> readerBytes=new HashMap<>(); private final Map<Long,RatisReferencedFile> declared=new HashMap<>(); private int streamReservations;
    private record Queued(RatisReferencedFile reference,CompletableFuture<RatisReferencedFile> future) {}
    private final TreeMap<Long,Queued> queued=new TreeMap<>(); private long truncateIndex; private CompletableFuture<Void> truncateActual,truncateReady; private final Set<Stream> streams=Collections.newSetFromMap(new IdentityHashMap<>()); private final Set<CompletableFuture<?>> operations=Collections.newSetFromMap(new IdentityHashMap<>());
    private final Object publication=new Object(); private volatile PublicationObserver observer=stage -> {}; private boolean closed,truncating,fenced; private long reservedBytes;
    public RatisReferencedFiles(Path directory,UUID group,Limits limits,Executor boundedIo) throws IOException {
        this.directory=directory.toAbsolutePath().normalize(); this.group=Objects.requireNonNull(group); this.limits=Objects.requireNonNull(limits); executor=Objects.requireNonNull(boundedIo); content=this.directory.resolve("content"); incoming=this.directory.resolve("incoming"); journal=this.directory.resolve("references");
        Files.createDirectories(content); Files.createDirectories(incoming); SessionJournalFiles.force(this.directory); SessionJournalFiles.force(this.directory.getParent());
        lockChannel=FileChannel.open(this.directory.resolve("owner.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try { try { acquired=lockChannel.tryLock(); } catch(OverlappingFileLockException conflict) { throw new IOException("Referenced files already owned",conflict); } if(acquired==null) throw new IOException("Referenced files already owned"); lock=acquired; recover(); }
        catch(IOException|RuntimeException|Error failure) { if(acquired!=null) try { acquired.release(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } try { lockChannel.close(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    public UUID groupId() { return group; }
    public void requireGroup(UUID expected) { if(!group.equals(expected)) throw new IllegalArgumentException("Referenced file owner belongs to another group"); }
    void publicationObserver(PublicationObserver observer) { this.observer=Objects.requireNonNull(observer); }
    private void requireOpen() throws IOException { if(closed || fenced) throw new IOException("Referenced file owner unavailable; reopen it"); }
    private Path file(byte[] sha) { return content.resolve(HexFormat.of().formatHex(sha)+".data"); }
    private long usedBytes() { var unique=new HashMap<>(readerBytes); references.values().forEach(ref -> unique.put(HexFormat.of().formatHex(ref.sha256()),ref.length())); declared.values().forEach(ref -> unique.put(HexFormat.of().formatHex(ref.sha256()),ref.length())); queued.values().forEach(value -> unique.put(HexFormat.of().formatHex(value.reference().sha256()),value.reference().length())); return unique.values().stream().mapToLong(Long::longValue).sum(); }
    private void validate(RatisReferencedFile.Request request) throws IOException { requireGroup(request.groupId()); if(request.length()>limits.maxFileBytes()) throw new IOException("Referenced payload exceeds file bound"); }
    private RatisReferencedFile.Request request(LogEntryProto entry) throws IOException {
        if(!entry.hasStateMachineLogEntry()) return null; var value=entry.getStateMachineLogEntry(); byte[] metadata=value.getLogData().toByteArray();
        boolean recognized=metadata.length>=4 && ByteBuffer.wrap(metadata).getInt()==RatisReferencedFile.Request.MAGIC;
        if(!recognized) { if(!value.getStateMachineEntry().getStateMachineData().isEmpty() || value.getStateMachineEntry().getLogEntryProtoSerializedSize()!=0) throw new IOException("Unknown supplied/stripped state-machine payload"); return null; }
        try { var request=RatisReferencedFile.Request.decode(metadata); validate(request); if(!ClientId.valueOf(value.getClientId()).getUuid().equals(request.clientId()) || value.getCallId()!=request.callId()) throw new IOException("Referenced request identity differs from actual entry"); return request; }
        catch(IllegalArgumentException bad) { throw new IOException("Malformed referenced request",bad); }
    }
    private RatisReferencedFile reference(LogEntryProto entry,RatisReferencedFile.Request request) { return new RatisReferencedFile(group,entry.getTerm(),entry.getIndex(),request.clientId(),request.callId(),request.length(),request.sha256()); }
    public synchronized RatisReferencedFile reference(long term,long index) throws IOException { requireOpen(); var result=references.get(index); if(result==null || result.term()!=term) throw new IOException("Exact referenced file unavailable"); return result; }
    /** Cheap deterministic log-envelope validation; publication remains owned by DataApi I/O. */
    RatisReferencedFile.Request validateEntry(LogEntryProto entry) throws IOException { return request(entry); }
    RatisReferencedFile publishedReference(long term,long index,RatisReferencedFile.Request expected) throws IOException {
        var result=reference(term,index); if(!result.request().equals(expected))throw new IOException("Published reference differs from committed envelope"); return result;
    }
    @FunctionalInterface private interface Io<T> { T run() throws Exception; }
    private <T> CompletableFuture<T> submit(CompletableFuture<T> future,Io<T> work) { return submit(future,work,false); }
    private <T> CompletableFuture<T> submit(CompletableFuture<T> future,Io<T> work,boolean cleanup) {
        IOException admission=null;
        synchronized(this) { try { if(cleanup) { if(closed) throw new IOException("Referenced owner closed"); } else requireOpen(); if(operations.size()>=limits.maxPendingOperations()+(cleanup ? limits.maxStreams() : 0)) throw new IOException("Referenced I/O capacity exhausted"); operations.add(future); } catch(IOException failure) { admission=failure; } }
        if(admission!=null) { future.completeExceptionally(admission); return future.copy(); }
        try { executor.execute(() -> { T value=null; Throwable failure=null; try { value=work.run(); } catch(Throwable error) { failure=error; } synchronized(this) { operations.remove(future); } if(failure==null) future.complete(value); else future.completeExceptionally(failure); }); }
        catch(RuntimeException rejected) { synchronized(this) { operations.remove(future); } future.completeExceptionally(rejected); } return future.copy();
    }
    private record Registration(CompletableFuture<RatisReferencedFile> future,boolean created,boolean stored,CompletableFuture<Void> ready) {}
    private synchronized Registration register(RatisReferencedFile reference,long streamCredit) throws IOException {
        requireOpen();
        if(truncating && reference.index()>=truncateIndex) {
            var previous=queued.get(reference.index());
            if(previous!=null) { if(!previous.reference().equals(reference)) throw new IOException("Queued replacement identity changed"); return new Registration(previous.future(),false,false,truncateReady); }
            if(queued.size()+operations.size()>=limits.maxPendingOperations() || references.headMap(truncateIndex,false).size()+queued.size()>=limits.maxReferences()) throw new IOException("Queued replacement capacity exhausted");
            boolean shared=references.values().stream().anyMatch(ref -> Arrays.equals(ref.sha256(),reference.sha256())) || declared.values().stream().anyMatch(ref -> Arrays.equals(ref.sha256(),reference.sha256())) || queued.values().stream().anyMatch(value -> Arrays.equals(value.reference().sha256(),reference.sha256())) || readerBytes.containsKey(HexFormat.of().formatHex(reference.sha256()));
            if((shared ? 0 : reference.length())>limits.maxContentBytes()-usedBytes()-reservedBytes+streamCredit) throw new IOException("Queued content capacity exhausted");
            var promise=new CompletableFuture<RatisReferencedFile>(); queued.put(reference.index(),new Queued(reference,promise)); return new Registration(promise,true,false,truncateReady);
        }
        var old=references.get(reference.index()); var promise=declared.get(reference.index());
        if(old!=null && !old.equals(reference) || promise!=null && !promise.equals(reference)) throw new IOException("Log reference identity changed without truncation");
        var previous=actual.get(reference.index());
        if(previous!=null) { @SuppressWarnings("unchecked") var typed=(CompletableFuture<RatisReferencedFile>)previous; return new Registration(typed,false,old!=null,CompletableFuture.completedFuture(null)); }
        if(old==null && references.size()+declared.keySet().stream().filter(index -> !references.containsKey(index)).count()>=limits.maxReferences()) throw new IOException("Referenced index capacity exhausted");
        boolean shared=references.values().stream().anyMatch(ref -> Arrays.equals(ref.sha256(),reference.sha256())) || declared.values().stream().anyMatch(ref -> Arrays.equals(ref.sha256(),reference.sha256())) || readerBytes.containsKey(HexFormat.of().formatHex(reference.sha256()));
        if((shared ? 0 : reference.length())>limits.maxContentBytes()-usedBytes()-reservedBytes+streamCredit) throw new IOException("Referenced content capacity exhausted");
        var future=new CompletableFuture<RatisReferencedFile>(); actual.put(reference.index(),future); declared.put(reference.index(),reference); return new Registration(future,true,old!=null,CompletableFuture.completedFuture(null));
    }
    private <T> CompletableFuture<T> submitAfter(CompletableFuture<Void> ready,CompletableFuture<T> future,Io<T> work) {
        ready.whenComplete((ignored,error) -> { if(error!=null) future.completeExceptionally(error); else submit(future,work); }); return future.copy();
    }
    private RatisReferencedFile publish(RatisReferencedFile reference,Path temporary) throws IOException {
        boolean moved=false;
        try {
            verify(temporary,reference); try(var data=FileChannel.open(temporary,StandardOpenOption.WRITE)) { data.force(true); } observer.at(PublishStage.FILE_FORCED);
            synchronized(publication) {
                Path target=file(reference.sha256()); if(Files.exists(target)) verify(target,reference); else Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE);
                moved=true; observer.at(PublishStage.CONTENT_PUBLISHED); SessionJournalFiles.force(content); observer.at(PublishStage.DIRECTORY_FORCED);
                TreeMap<Long,RatisReferencedFile> next; synchronized(this) { next=new TreeMap<>(references); next.put(reference.index(),reference); }
                force(next); synchronized(this) { references.clear(); references.putAll(next); } observer.at(PublishStage.REFERENCE_FORCED);
            }
            return reference;
        } catch(IOException|RuntimeException failure) { if(moved) synchronized(this) { fenced=true; } throw failure; }
        finally { Files.deleteIfExists(temporary); }
    }
    private CompletableFuture<?> verifyStored(CompletableFuture<RatisReferencedFile> future,RatisReferencedFile reference) throws IOException {
        var reader=pin(reference); future.whenComplete((value,error) -> { try { reader.close(); } catch(IOException cleanup) { synchronized(RatisReferencedFiles.this) { fenced=true; } } });
        return submit(future,() -> { try(reader) { reader.read(); return reference; } });
    }
    @Override public CompletableFuture<?> write(LogEntryProto entry) {
        CompletableFuture<RatisReferencedFile> future=null;
        try {
            var request=request(entry); if(request==null) return CompletableFuture.completedFuture(null); var reference=reference(entry,request);
            var registration=register(reference,0); future=registration.future(); if(!registration.created()) return future.copy();
            if(registration.stored()) return verifyStored(future,reference);
            var payload=entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData();
            if(payload.size()!=request.length()) throw new IOException("Missing/truncated referenced payload");
            return submitAfter(registration.ready(),future,() -> { Path temp=Files.createTempFile(incoming,"write-",".data"); try { try(var data=FileChannel.open(temp,StandardOpenOption.WRITE)) { var buffer=payload.asReadOnlyByteBuffer(); while(buffer.hasRemaining()) data.write(buffer); } return publish(reference,temp); } finally { Files.deleteIfExists(temp); } });
        } catch(Exception failure) {
            if(future!=null) future.completeExceptionally(failure);
            else { var failed=new CompletableFuture<RatisReferencedFile>(); failed.completeExceptionally(failure); synchronized(this) { if(actual.size()<limits.maxReferences()) actual.putIfAbsent(entry.getIndex(),failed); } future=failed; }
            return future.copy();
        }
    }
    @Override public CompletableFuture<StateMachine.DataStream> stream(RaftClientRequest request) {
        try {
            var expected=RatisReferencedFile.Request.decode(request.getMessage().getContent().toByteArray()); validate(expected); requireGroup(request.getRaftGroupId().getUuid()); if(!request.getClientId().getUuid().equals(expected.clientId()) || request.getCallId()!=expected.callId()) throw new IOException("Stream request identity changed");
            synchronized(this) { requireOpen(); if(streamReservations>=limits.maxStreams() || expected.length()>limits.maxContentBytes()-usedBytes()-reservedBytes) throw new IOException("Referenced stream capacity exhausted"); streamReservations++; reservedBytes+=expected.length(); }
            var future=new CompletableFuture<StateMachine.DataStream>(); var started=new java.util.concurrent.atomic.AtomicBoolean(); var creationCanceled=new java.util.concurrent.atomic.AtomicBoolean();
            future.whenComplete((value,error) -> { if(!started.get()) synchronized(RatisReferencedFiles.this) { streamReservations--; reservedBytes-=expected.length(); } });
            var view=submit(future,() -> { started.set(true); try { var stream=new Stream(expected,creationCanceled); synchronized(this) { streams.add(stream); } return stream; } catch(Throwable failure) { synchronized(this) { streamReservations--; reservedBytes-=expected.length(); } throw failure; } });
            future.whenComplete((value,error) -> { if(view.isCancelled() && value instanceof Stream stream) stream.cleanUp(); });
            view.whenComplete((value,error) -> { if(view.isCancelled()) creationCanceled.set(true); if(view.isCancelled() && future.isDone() && !future.isCompletedExceptionally()) ((Stream)future.join()).cleanUp(); });
            return view;
        } catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    @Override public CompletableFuture<?> link(StateMachine.DataStream data,LogEntryProto entry) {
        try {
            var request=request(entry); if(!(data instanceof Stream stream) || stream.owner()!=this || request==null || !request.equals(stream.request)) throw new IOException("Stream/link identity mismatch"); var reference=reference(entry,request);
            synchronized(stream) {
                var old=stream.linkReference; if(old!=null && !old.equals(reference)) throw new IOException("Stream was bound to another log index");
                stream.close(); if(stream.length!=request.length()) throw new IOException("Incomplete stream");
                var registration=register(reference,stream.charged ? stream.request.length() : 0);
                if(!registration.created()) {
                    stream.cleanupRequested=true;
                    registration.future().whenComplete((value,error) -> stream.cleanUp());
                    return registration.future().copy();
                }
                stream.linkReference=reference; stream.linked=true;
                synchronized(this) { if(stream.charged) { reservedBytes-=stream.request.length(); stream.charged=false; } }
                registration.future().whenComplete((value,error) -> { try { Files.deleteIfExists(stream.path); } catch(IOException cleanup) { synchronized(RatisReferencedFiles.this) { fenced=true; } } releaseStream(stream); });
                return registration.stored() ? verifyStored(registration.future(),reference) : submitAfter(registration.ready(),registration.future(),() -> publish(reference,stream.path));
            }
        } catch(Exception failure) { throw new IllegalArgumentException("Referenced streamed link rejected",failure); }
    }
    private void releaseStream(Stream stream) { synchronized(this) { if(streams.remove(stream)) { streamReservations--; if(stream.charged) { reservedBytes-=stream.request.length(); stream.charged=false; } } } }
    private final class Stream implements StateMachine.DataStream,StateMachine.DataChannel {
        final RatisReferencedFile.Request request; final Path path; final FileChannel channel; final java.util.concurrent.atomic.AtomicBoolean creationCanceled; long length; boolean open=true,linked,charged=true,cleanupRequested; RatisReferencedFile linkReference; CompletableFuture<Void> cleanupActual;
        Stream(RatisReferencedFile.Request request,java.util.concurrent.atomic.AtomicBoolean creationCanceled) throws IOException { this.request=request; this.creationCanceled=creationCanceled; path=Files.createTempFile(incoming,"stream-",".data"); channel=FileChannel.open(path,StandardOpenOption.WRITE); }
        RatisReferencedFiles owner() { return RatisReferencedFiles.this; }
        public StateMachine.DataChannel getDataChannel() { return this; }
        public Executor getExecutor() { return executor; }
        public synchronized int write(ByteBuffer buffer) throws IOException { if(!open || linked) throw new IOException("Stream is closed"); if(buffer.remaining()>request.length()-length) throw new IOException("Stream exceeds exact payload length"); int count=channel.write(buffer); length+=count; return count; }
        public synchronized void force(boolean metadata) throws IOException { if(!open) throw new IOException("Stream closed"); channel.force(metadata); }
        public synchronized boolean isOpen() { return open; }
        public synchronized void close() throws IOException { if(open) { try { channel.force(true); } finally { try { channel.close(); } finally { open=false; } } } }
        public CompletableFuture<?> cleanUp() {
            CompletableFuture<Void> done;
            synchronized(this) {
                if(linked) { synchronized(RatisReferencedFiles.this) { var pending=actual.get(linkReference.index()); if(pending==null) { var replacement=queued.get(linkReference.index()); pending=replacement==null ? null : replacement.future(); } return pending==null ? CompletableFuture.completedFuture(null) : pending.copy(); } }
                cleanupRequested=true; if(cleanupActual!=null && !cleanupActual.isCompletedExceptionally()) return cleanupActual.copy(); cleanupActual=new CompletableFuture<>(); done=cleanupActual;
            }
            return submit(done,() -> { synchronized(this) { try { if(open) channel.close(); } finally { open=false; } Files.deleteIfExists(path); } releaseStream(this); return null; },true);
        }
    }
    public final class Reader implements AutoCloseable {
        private final RatisReferencedFile reference; private boolean released;
        private Reader(RatisReferencedFile reference) { this.reference=reference; }
        /** Trusted local JNI bridge only; retain this pin until all native users of the immutable file return. */
        public synchronized Path immutablePath() throws IOException { if(released) throw new IOException("Referenced reader closed"); return file(reference.sha256()); }
        public synchronized ByteString read() throws IOException { if(released) throw new IOException("Referenced reader closed"); byte[] bytes=new byte[Math.toIntExact(reference.length())]; try(var channel=FileChannel.open(file(reference.sha256()),StandardOpenOption.READ)) { if(channel.size()!=reference.length()) throw new IOException("Referenced bytes changed"); var buffer=ByteBuffer.wrap(bytes); while(buffer.hasRemaining()) if(channel.read(buffer)<0) throw new EOFException("Referenced bytes shortened"); if(channel.size()!=reference.length()) throw new IOException("Referenced bytes changed"); } if(!MessageDigest.isEqual(hash(bytes),reference.sha256())) throw new IOException("Referenced bytes changed"); return ByteString.copyFrom(bytes); }
        public void close() throws IOException { synchronized(this) { if(released) return; released=true; } synchronized(RatisReferencedFiles.this) { String sha=HexFormat.of().formatHex(reference.sha256()); readers.compute(sha,(key,count) -> count==1 ? null : count-1); if(!readers.containsKey(sha)) readerBytes.remove(sha); } reclaim(); }
    }
    public synchronized Reader pin(RatisReferencedFile reference) throws IOException { requireOpen(); requireGroup(reference.groupId()); if(!reference.equals(references.get(reference.index()))) throw new IOException("Reference is not owned/live"); if(readers.values().stream().mapToInt(Integer::intValue).sum()>=limits.maxPendingOperations()) throw new IOException("Referenced reader capacity exhausted"); String sha=HexFormat.of().formatHex(reference.sha256()); readers.merge(sha,1,Integer::sum); readerBytes.put(sha,reference.length()); return new Reader(reference); }
    @Override public CompletableFuture<ByteString> read(LogEntryProto entry) {
        try { var request=request(entry); if(request==null) return CompletableFuture.completedFuture(ByteString.EMPTY); var reference=reference(entry,request); var reader=pin(reference); var future=new CompletableFuture<ByteString>(); future.whenComplete((value,error) -> { try { reader.close(); } catch(IOException cleanup) { synchronized(RatisReferencedFiles.this) { fenced=true; } } }); return submit(future,() -> { try(reader) { return reader.read(); } }); }
        catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    @Override public CompletableFuture<Void> flush(long index) { CompletableFuture<?>[] futures; synchronized(this) { var all=new ArrayList<CompletableFuture<?>>(); all.addAll(actual.headMap(index,true).values()); queued.headMap(index,true).values().forEach(value -> all.add(value.future())); futures=all.toArray(CompletableFuture[]::new); } return CompletableFuture.allOf(futures).copy(); }
    public CompletableFuture<Void> drain() {
        List<Stream> pendingCleanup; synchronized(this) { pendingCleanup=streams.stream().filter(stream -> (stream.cleanupRequested || stream.creationCanceled.get()) && !stream.linked).toList(); }
        var cleanup=new ArrayList<CompletableFuture<?>>(); for(var stream:pendingCleanup) cleanup.add(stream.cleanUp());
        var waiting=new ArrayList<CompletableFuture<?>>(); synchronized(this) { waiting.addAll(operations); waiting.addAll(actual.values()); queued.values().forEach(value -> waiting.add(value.future())); if(truncateActual!=null) waiting.add(truncateActual); }
        // I/O return is separate from publication success; flush retains the original failed result.
        var returned=waiting.stream().map(future -> future.handle((value,error) -> null)).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(returned).thenCompose(ignored -> {
            for(var future:cleanup) if(future.isCompletedExceptionally()) return CompletableFuture.failedFuture(new IOException("Protected cleanup rejected; retry drain when I/O capacity is available"));
            boolean more,unreleased;
            synchronized(this) { more=!operations.isEmpty() || !queued.isEmpty() || truncateActual!=null; unreleased=streams.stream().anyMatch(stream -> (stream.cleanupRequested || stream.creationCanceled.get()) && !stream.linked); }
            if(more) return drain();
            if(unreleased) return CompletableFuture.failedFuture(new IOException("Stream cleanup remains owned; retry drain"));
            return CompletableFuture.completedFuture(null);
        }).copy();
    }
    @Override public CompletableFuture<Void> truncate(long index) {
        var future=new CompletableFuture<Void>(); var ready=new CompletableFuture<Void>(); CompletableFuture<?>[] pending;
        synchronized(this) { try { requireOpen(); if(index<0 || truncating) throw new IOException("Invalid/active truncate"); truncating=true; truncateIndex=index; truncateActual=future; truncateReady=ready; pending=actual.tailMap(index,true).values().toArray(CompletableFuture[]::new); } catch(IOException failure) { return CompletableFuture.failedFuture(failure); } }
        // This completion runs even when bounded admission or executor rejection prevents the body from starting.
        future.whenComplete((ignored,error) -> {
            List<Queued> waiting;
            synchronized(RatisReferencedFiles.this) {
                waiting=List.copyOf(queued.values()); queued.clear();
                for(var value:waiting) { actual.put(value.reference().index(),value.future()); declared.put(value.reference().index(),value.reference()); }
                truncating=false; truncateActual=null; truncateReady=null;
            }
            if(error!=null) ready.completeExceptionally(error); else ready.complete(null);
        });
        CompletableFuture.allOf(pending).handle((ignored,error) -> null).whenComplete((ignored,error) -> submit(future,() -> {
            synchronized(publication) { TreeMap<Long,RatisReferencedFile> next; synchronized(this) { next=new TreeMap<>(references.headMap(index,false)); } force(next); synchronized(this) { references.clear(); references.putAll(next); actual.tailMap(index,true).clear(); declared.keySet().removeIf(value -> value>=index); } reclaim(); }
            return null;
        })); return ready.copy();
    }
    @Override public long transferTo(Message message,java.nio.channels.WritableByteChannel destination) throws IOException { throw new IOException("Referenced payload transfer protocol is not configured"); }
    private void force(Map<Long,RatisReferencedFile> refs) throws IOException { var out=ByteBuffer.allocate(24+RatisReferencedFile.BYTES*refs.size()).putInt(0x52465247).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).putInt(refs.size()); refs.values().forEach(ref -> out.put(ref.encode())); try { SessionJournalFiles.write(journal,out.array(),24+100*limits.maxReferences()+32); } catch(IOException failure) { synchronized(this) { fenced=true; } throw failure; } }
    private void recover() throws IOException {
        if(Files.exists(journal)) { byte[] body=SessionJournalFiles.read(journal,24+100*limits.maxReferences()+32); try { var in=ByteBuffer.wrap(body); if(in.getInt()!=0x52465247) throw new IOException("Invalid reference journal"); requireGroup(new UUID(in.getLong(),in.getLong())); int count=in.getInt(); if(count<0 || count>limits.maxReferences() || in.remaining()!=count*100L) throw new IOException("Invalid reference count"); long last=-1; for(int i=0;i<count;i++) { byte[] encoded=new byte[100]; in.get(encoded); var ref=RatisReferencedFile.decode(encoded); requireGroup(ref.groupId()); if(ref.index()<=last || ref.length()>limits.maxFileBytes()) throw new IOException("Noncanonical reference journal"); references.put(ref.index(),ref); last=ref.index(); verify(file(ref.sha256()),ref); } if(usedBytes()>limits.maxContentBytes()) throw new IOException("Recovered content exceeds capacity"); } catch(RuntimeException malformed) { throw new IOException("Invalid reference journal",malformed); } try(var file=FileChannel.open(journal,StandardOpenOption.READ)) { file.force(true); } SessionJournalFiles.force(directory); }
        try(var files=Files.newDirectoryStream(incoming)) { for(var file:files) Files.delete(file); } reclaim();
    }
    private void reclaim() throws IOException { synchronized(publication) { Set<String> live; synchronized(this) { live=new HashSet<>(readers.keySet()); references.values().forEach(ref -> live.add(HexFormat.of().formatHex(ref.sha256()))); declared.values().forEach(ref -> live.add(HexFormat.of().formatHex(ref.sha256()))); queued.values().forEach(value -> live.add(HexFormat.of().formatHex(value.reference().sha256()))); } boolean changed=false; try(var files=Files.newDirectoryStream(content,"*.data")) { for(var file:files) { String name=file.getFileName().toString(); if(!live.contains(name.substring(0,name.length()-5))) { Files.delete(file); changed=true; } } } if(changed) SessionJournalFiles.force(content); } }
    private void verify(Path path,RatisReferencedFile reference) throws IOException { if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)!=reference.length()) throw new IOException("Referenced bytes missing/changed"); try(var stream=Files.newInputStream(path)) { var digest=sha(); byte[] buffer=new byte[64*1024]; int count; while((count=stream.read(buffer))>=0) digest.update(buffer,0,count); if(!MessageDigest.isEqual(digest.digest(),reference.sha256())) throw new IOException("Referenced SHA mismatch"); } }
    private static MessageDigest sha() { try { return MessageDigest.getInstance("SHA-256"); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    private static byte[] hash(byte[] bytes) { return sha().digest(bytes); }
    @Override public void close() throws IOException { synchronized(this) { if(closed) return; if(truncating || !queued.isEmpty() || actual.values().stream().anyMatch(future -> !future.isDone()) || !operations.isEmpty() || streamReservations!=0 || !streams.isEmpty() || !readers.isEmpty()) throw new IOException("Referenced actual I/O/stream/reader ownership retained"); closed=true; } lock.release(); lockChannel.close(); }
}
