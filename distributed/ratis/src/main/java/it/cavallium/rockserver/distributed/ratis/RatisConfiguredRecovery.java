package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** One node-owned immutable recovery attempt. CONTROL only requests work; its borrowed executor owns IO. */
final class RatisConfiguredRecovery implements AutoCloseable {
    enum Phase { PREPARE, REGISTER, TRANSFER, PROBE, RELEASE, HEAD, COMPLETE, DONE }
    interface Host {
        CompletableFuture<Prepared> prepare(Request request);
        CompletableFuture<byte[]> register(Prepared prepared);
        CompletableFuture<Prepared> transfer(Request request);
        CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request);
        CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request);
        /** False means no completion force was attempted; a new source head may be selected. */
        CompletableFuture<Boolean> complete(Prepared prepared,RatisSourceHeadRpc.Head frozenHead);
        CompletableFuture<Void> release(Prepared prepared);
    }
    interface Metadata extends RatisProofQueryService.ResourceLease,RatisTopologySourceSnapshotClient.ReplyAdmission {
        void requireRawBytes(int bytes)throws IOException;
        @Override default void beforeReply(int bytes)throws IOException{requireRawBytes(bytes);}
    }
    @FunctionalInterface interface Scope {void require(Request request)throws IOException;}
    private record State(Request initial,Request request,Phase phase,Prepared prepared,byte[] registration,
                         RatisSourceHeadRpc.Head head,Abandoned abandoned) {}
    private static final int MAX_WAITERS=16;
    private final Path file;private final byte[] binding;private final Executor executor;private final Host host;
    private final RatisJournalAccess files;private final Metadata metadata;
    private final FileChannel channel;private final FileLock lock;
    private final CompletableFuture<Void> stopped=new CompletableFuture<>();
    private State state,dirty;private Phase phase;private CompletableFuture<Phase> actual;private int waiters;
    private boolean stopping,closed;

    /** Metadata and journal replacement capacity must be reserved before this constructor reads or copies state. */
    RatisConfiguredRecovery(Path directory,byte[] binding,Request initial,Host host,Executor executor,
                            RatisJournalAccess files,Metadata metadata,Scope scope)throws IOException {
        this.binding=Objects.requireNonNull(binding).clone();this.host=Objects.requireNonNull(host);this.executor=Objects.requireNonNull(executor);this.files=Objects.requireNonNull(files);this.metadata=Objects.requireNonNull(metadata);
        if(binding.length!=32 || initial!=null && initial.relocation()){metadata.close();throw new IllegalArgumentException("Invalid configured recovery binding/purpose");}
        var root=directory.toAbsolutePath().normalize();if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS)){metadata.close();throw new IOException("Recovery journal requires an owned existing directory");}
        file=root.resolve("recovery");Path lockFile=root.resolve("recovery.lock");try{channel=FileChannel.open(lockFile,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);}catch(IOException failed){metadata.close();throw failed;}FileLock acquired=null;
        try {
            try{acquired=channel.tryLock();}catch(OverlappingFileLockException busy){throw new IOException("Recovery journal already owned",busy);}if(acquired==null)throw new IOException("Recovery journal already owned");lock=acquired;
            files.register(file,RatisSchemaClientJournal.MAX_BYTES);
            int count=0;try(var entries=Files.newDirectoryStream(root)){for(var entry:entries){if(++count>3 || !Set.of("recovery","recovery.pending","recovery.lock").contains(entry.getFileName().toString()) || !Files.isRegularFile(entry,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign object in recovery journal");}}
            if(Files.exists(file,LinkOption.NOFOLLOW_LINKS)){long size=Files.size(file);if(size>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery journal exceeds bound");metadata.requireRawBytes(Math.toIntExact(size));state=decode(RatisSchemaClientJournal.readOwned(file,files));if(initial!=null){try{requireRequest(initial,state.initial());}catch(IllegalArgumentException changed){if(state.phase()!=Phase.DONE || !initial.metadataOnly() || !state.initial().metadataOnly() || initial.transferId().equals(state.initial().transferId()))throw changed;requireRequest(state.initial().successor(initial.transferId()),initial);state=new State(initial,initial,Phase.PREPARE,null,null,null,null);}}}
            else if(initial==null){var unpublished=root.resolve("recovery.pending");if(!Files.isRegularFile(unpublished,LinkOption.NOFOLLOW_LINKS) || Files.size(unpublished)>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery has no retained original request");metadata.requireRawBytes(Math.toIntExact(Files.size(unpublished)));state=decode(RatisSchemaClientJournal.readOwned(unpublished,files));if(state.phase()!=Phase.PREPARE || state.abandoned()!=null)throw new IOException("Missing canonical recovery journal after host dispatch");requireRequest(state.initial(),state.request());}
            else state=new State(initial,initial,Phase.PREPARE,null,null,null,null);
            scope.require(state.initial());
            // A pending replacement never authorized another host action. The forced main state,
            // or the undispatched initial request, remains the exact retry authority.
            var pending=root.resolve("recovery.pending");if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)){if(Files.size(pending)>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery sidecar exceeds bound");Files.delete(pending);SessionJournalFiles.force(root);}
            persist(state);phase=state.phase();channel.force(true);SessionJournalFiles.force(root);
        }catch(IOException|RuntimeException|Error failed){try{if(acquired!=null)acquired.release();}catch(IOException cleanup){failed.addSuppressed(cleanup);}try{channel.close();}catch(IOException cleanup){failed.addSuppressed(cleanup);}metadata.close();throw failed;}
    }
    synchronized Phase phase(){return phase;}
    synchronized Request initial(){return state.initial();}
    /** Timer use allocates no observer while actual IO remains in flight. */
    void advance(){requestStep(false);}
    CompletableFuture<Phase> step(){return requestStep(true);}
    private CompletableFuture<Phase> requestStep(boolean observe) {
        final CompletableFuture<Phase> pending;boolean start=false;
        synchronized(this){
            if(stopping)return CompletableFuture.failedFuture(new IOException("Configured recovery stopped"));
            if(actual==null && phase==Phase.DONE)return CompletableFuture.completedFuture(phase);
            if(actual==null){actual=new CompletableFuture<>();waiters=0;start=true;}
            if(observe && waiters>=MAX_WAITERS)return CompletableFuture.failedFuture(new RejectedExecutionException("Recovery observation bound exhausted"));
            if(observe)waiters++;pending=actual;
        }
        if(start)post(()->start(pending),pending);
        return observe?pending.copy():pending;
    }
    private void post(Runnable job,CompletableFuture<Phase> pending){try{executor.execute(job);}catch(RuntimeException rejected){finish(pending,rejected);}}
    private void start(CompletableFuture<Phase> pending) {
        final State current,unsaved;
        boolean stop;synchronized(this){current=state;unsaved=dirty;stop=stopping && unsaved==null;}
        if(stop){finish(pending,new CancellationException("Recovery stopped before dispatch"));return;}
        if(unsaved!=null){save(unsaved,pending);return;}
        try {
            CompletableFuture<State> next=switch(current.phase()) {
                case PREPARE -> host.prepare(current.request()).handle((prepared,failure)->{
                    var problem=unwrap(failure);if(problem instanceof AbandonedException terminal){var receipt=terminal.receipt();requireRequest(current.request(),receipt.original());return new State(current.initial(),receipt.successorRequest(),Phase.PREPARE,null,null,null,receipt);}
                    if(problem!=null)throw new CompletionException(problem);requireRequest(current.request(),prepared.request());return new State(current.initial(),current.request(),Phase.REGISTER,prepared,null,null,current.abandoned());
                });
                case REGISTER -> host.register(current.prepared()).thenApply(digest->{if(digest==null || digest.length!=32)throw new IllegalArgumentException("Missing exact native registration digest");return new State(current.initial(),current.request(),Phase.TRANSFER,current.prepared(),digest.clone(),null,current.abandoned());});
                case TRANSFER -> host.transfer(current.request()).thenApply(prepared->{if(!Arrays.equals(reply(PREPARE,prepared),reply(PREPARE,current.prepared())))throw new IllegalArgumentException("Transfer changed original prepared image");return new State(current.initial(),current.request(),Phase.PROBE,current.prepared(),current.registration(),null,current.abandoned());});
                case PROBE -> {var probe=new RatisRecoveryImageRpc.Request(current.request(),current.prepared().image().position(),current.prepared().image().sha256());yield host.probe(probe).thenApply(evidence->{evidence.require(probe);if(!evidence.imageInstalled())return current;if(!Arrays.equals(evidence.registrationDigest(),current.registration()))throw new IllegalArgumentException("Image receipt changed local registration");return new State(current.initial(),current.request(),Phase.RELEASE,current.prepared(),current.registration(),null,current.abandoned());});}
                case HEAD -> {var query=new RatisSourceHeadRpc.Request(current.request(),current.prepared().image().position());yield host.head(query).thenApply(head->{RatisSourceHeadRpc.reply(RatisSourceHeadRpc.reply(head),query);return new State(current.initial(),current.request(),Phase.COMPLETE,current.prepared(),current.registration(),head,current.abandoned());});}
                case COMPLETE -> host.complete(current.prepared(),current.head()).thenApply(done->new State(current.initial(),current.request(),done?Phase.DONE:Phase.HEAD,current.prepared(),current.registration(),done?current.head():null,current.abandoned()));
                case RELEASE -> host.release(current.prepared()).thenApply(ignored->new State(current.initial(),current.request(),Phase.HEAD,current.prepared(),current.registration(),null,current.abandoned()));
                case DONE -> CompletableFuture.completedFuture(current);
            };
            next.whenComplete((candidate,failure)->post(()->{if(failure!=null)finish(pending,unwrap(failure));else if(candidate==current)finish(pending,null);else save(candidate,pending);},pending));
        }catch(Throwable failed){finish(pending,failed);}
    }
    private void save(State candidate,CompletableFuture<Phase> pending) {
        synchronized(this){dirty=candidate;}
        try{persist(candidate);synchronized(this){state=candidate;phase=candidate.phase();dirty=null;}finish(pending,null);}
        catch(Throwable failed){finish(pending,failed);}
    }
    private void finish(CompletableFuture<Phase> pending,Throwable failure) {
        Phase result;boolean stop;
        synchronized(this){if(actual!=pending)return;actual=null;waiters=0;result=phase;stop=stopping;}
        if(failure==null)pending.complete(result);else pending.completeExceptionally(failure);
        if(stop)finishStop();
    }
    CompletableFuture<Void> stop(){boolean close;synchronized(this){stopping=true;close=actual==null;}if(close)finishStop();return stopped.copy();}
    private void finishStop(){
        synchronized(this){if(closed || actual!=null)return;closed=true;state=null;dirty=null;}
        IOException failure=null;try{lock.release();}catch(IOException failed){failure=failed;}try{channel.close();}catch(IOException failed){if(failure==null)failure=failed;else failure.addSuppressed(failed);}metadata.close();
        if(failure==null)stopped.complete(null);else stopped.completeExceptionally(failure);
    }
    @Override public void close()throws IOException {var done=stop();if(!done.isDone())throw new IOException("Recovery close waits for actual host and journal IO");try{done.join();}catch(CompletionException failed){throw new IOException("Recovery close failed",failed.getCause());}}
    private static Throwable unwrap(Throwable problem){while((problem instanceof CompletionException || problem instanceof ExecutionException) && problem.getCause()!=null)problem=problem.getCause();return problem;}
    private void persist(State value)throws IOException {
        validate(value);long size=40+field("binding",32)+field("initial",encodedRequestBytes(value.initial()))+field("request",encodedRequestBytes(value.request()))+field("phase",value.phase().name().length());
        if(value.prepared()!=null)size+=field("prepared",value.prepared().encodedSize());if(value.registration()!=null)size+=field("registration",32);if(value.head()!=null)size+=field("head",RatisSourceHeadRpc.encodedReplyBytes(value.head()));
        if(value.abandoned()!=null){var receipt=value.abandoned();size+=field("abandoned",73L+encodedRequestBytes(receipt.original())+57L+receipt.sourceOwner().peerId().length()+receipt.sourceOwner().address().length());}
        if(size>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery journal metadata bound exhausted");metadata.requireRawBytes(Math.toIntExact(size));var fields=encode(value);
        long actual=40;for(var entry:fields.entrySet())actual+=field(entry.getKey(),entry.getValue().length);if(actual!=size)throw new IOException("Recovery journal sizing differs from encoded fields");
        long original=Files.exists(file,LinkOption.NOFOLLOW_LINKS)?Files.size(file):0;var reservation=new RatisJournalAccess.Reservation(file,RatisSchemaClientJournal.MAX_BYTES,Math.addExact(original,size));
        files.reserve(reservation);RatisSchemaClientJournal.writeOwned(file,fields,files);files.fulfilled(reservation);
    }
    private static long field(String name,long bytes){return 6L+name.length()+bytes;}
    private Map<String,byte[]> encode(State value){
        var fields=new HashMap<String,byte[]>();fields.put("binding",binding);fields.put("initial",request(PREPARE,value.initial()));fields.put("request",request(PREPARE,value.request()));fields.put("phase",value.phase().name().getBytes(StandardCharsets.US_ASCII));
        if(value.prepared()!=null)fields.put("prepared",reply(PREPARE,value.prepared()));if(value.registration()!=null)fields.put("registration",value.registration());if(value.head()!=null)fields.put("head",RatisSourceHeadRpc.reply(value.head()));if(value.abandoned()!=null)fields.put("abandoned",reply(PREPARE,value.abandoned()));return fields;
    }
    private State decode(Map<String,byte[]> fields)throws IOException {
        try{if(!Arrays.equals(binding,fields.get("binding")) || !Set.of("binding","initial","request","phase","prepared","registration","head","abandoned").containsAll(fields.keySet()))throw new IllegalArgumentException("Recovery journal binding/fields differ");
            var initial=request(fields.get("initial"));var request=request(fields.get("request"));var phase=Phase.valueOf(new String(fields.get("phase"),StandardCharsets.US_ASCII));var prepared=fields.containsKey("prepared")?reply(fields.get("prepared"),PREPARE,request):null;
            var head=fields.containsKey("head")?RatisSourceHeadRpc.reply(fields.get("head"),new RatisSourceHeadRpc.Request(request,Objects.requireNonNull(prepared).image().position())):null;
            Abandoned abandoned=null;if(fields.containsKey("abandoned")){var packet=ScopedTransactionRpc.parse(fields.get("abandoned"),MAX_REPLY_BYTES);var bytes=java.nio.ByteBuffer.wrap(packet.body());if(bytes.get()!=2)throw new IllegalArgumentException("Expected abandonment receipt");int length=bytes.getInt();if(length<1 || length>MAX_REQUEST_BYTES || length>bytes.remaining())throw new IllegalArgumentException("Invalid abandonment scope length");byte[] body=new byte[length];bytes.get(body);var previous=request(ScopedTransactionRpc.frame(PREPARE,body));try{reply(fields.get("abandoned"),PREPARE,previous);throw new IllegalArgumentException("Expected terminal abandonment");}catch(AbandonedException terminal){abandoned=terminal.receipt();}}
            var state=new State(initial,request,phase,prepared,fields.get("registration"),head,abandoned);validate(state);return state;
        }catch(RuntimeException failed){throw new IOException("Invalid configured recovery journal",failed);}
    }
    private static void validate(State state)throws IOException {
        try{if(state.initial().relocation())throw new IllegalArgumentException("Wrong recovery purpose");requireRequest(state.initial().successor(state.request().transferId()),state.request());
            if(state.request().transferId().equals(state.initial().transferId())!=(state.abandoned()==null))throw new IllegalArgumentException("Recovery successor lacks abandonment");if(state.abandoned()!=null)requireRequest(state.abandoned().successorRequest(),state.request());
            boolean prepared=state.phase().ordinal()>=Phase.REGISTER.ordinal(),registered=state.phase().ordinal()>=Phase.TRANSFER.ordinal(),head=state.phase().ordinal()>=Phase.COMPLETE.ordinal();
            if(prepared!=(state.prepared()!=null) || registered!=(state.registration()!=null) || head!=(state.head()!=null) || registered && state.registration().length!=32)throw new IllegalArgumentException("Recovery phase lost required forced evidence");
            if(prepared)requireRequest(state.request(),state.prepared().request());if(head)RatisSourceHeadRpc.reply(RatisSourceHeadRpc.reply(state.head()),new RatisSourceHeadRpc.Request(state.request(),state.prepared().image().position()));
        }catch(RuntimeException failed){throw new IOException("Invalid recovery phase/evidence",failed);}
    }
}
