package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaRequests.*;

/** One bounded blocking controller lane, isolated from native DATA workers and observer cancellation. */
final class RatisSchemaJobBridge implements AutoCloseable {
    @FunctionalInterface interface ClientFactory { RatisSchemaClient create(RatisSchemaClient.Checkpoint checkpoint) throws IOException; }
    @FunctionalInterface interface Prepare { RatisSchemaClient.Handle create(RatisSchemaClient client,UUID id) throws Exception; }
    private final Path root,clientRoot;
    private final RWScheduler clock;
    private final ClientFactory factory;
    private final RatisSchemaClient.TrustedOperatorResolver resolver;
    private final int maximumRequests,maximumQueued;
    private final long maximumBytes;
    private final ExecutorService lane;
    private final ScheduledExecutorService timer;
    private final Map<UUID,Entry> entries=new LinkedHashMap<>();
    private RatisSchemaClient client;
    private Entry current;
    private int actual;
    private long bytes;
    private boolean stopping,closed;
    private volatile Thread worker;
    private static final class Entry {
        final UUID id; final Kind kind;
        volatile State state; volatile RatisSchemaClient.Phase phase; volatile Result result;
        volatile boolean cancelled; volatile long deadline;
        CompletableFuture<Result> observer;
        Entry(UUID id,Kind kind,State state) { this.id=id; this.kind=kind; this.state=state; }
        Status status() { return new Status(id,kind,state,phase,result); }
    }
    RatisSchemaJobBridge(Path root,Path clientRoot,RWScheduler clock,ClientFactory factory,RatisSchemaClient.TrustedOperatorResolver resolver,it.cavallium.rockserver.distributed.ratis.RatisRoutedClient routed,int requests,int queued,long maximumBytes) throws IOException {
        this.root=root; this.clientRoot=clientRoot; this.clock=clock; this.factory=factory; this.resolver=resolver; maximumRequests=requests; maximumQueued=queued; this.maximumBytes=maximumBytes;
        Files.createDirectories(root); Files.createDirectories(clientRoot);
        try (var files=Files.newDirectoryStream(root,"*.bridge")) {
            for (var file:files) { if (entries.size()>=requests) throw new IOException("Schema request inventory exceeds connection limits"); var e=decode(SessionJournalFiles.read(file,4096)); if (!file.getFileName().toString().equals(e.id+".bridge") || entries.putIfAbsent(e.id,e)!=null) throw new IOException("Invalid schema bridge identity"); if (e.state!=State.DONE && e.state!=State.ABORTED && e.state!=State.CONFLICT) e.state=Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS) ? State.PAUSED : State.NOT_STARTED; }
        }
        client=factory.create(this::checkpoint);
        try {
        var history=new ArrayList<Map.Entry<UUID,Long>>();
        for (var entry:entries.values()) if (Files.exists(clientPath(entry.id),LinkOption.NOFOLLOW_LINKS)) {
            var fields=RatisSchemaClientJournal.read(clientPath(entry.id)); var definition=fields.get("definition"); if (definition!=null) history.add(Map.entry(entry.id,it.cavallium.rockserver.core.common.distributed.SchemaChange.Definition.decode(definition).targetSchema().schemaEpoch()));
        }
        history.sort(Map.Entry.comparingByValue());
        long bootstrapEpoch=routed.directory().schemaEpoch();
        for (var entry:history) if (entry.getValue()>bootstrapEpoch) try (var handle=client.reopen(entry.getKey(),resolver)) { client.completed(handle); }
        history.sort(Map.Entry.<UUID,Long>comparingByValue().reversed());
        for (var entry:history) if (entry.getValue()<=routed.directory().schemaEpoch()) try (var handle=client.reopen(entry.getKey(),resolver)) { client.completed(handle); }
        } catch (Throwable failure) { try { client.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); } if (failure instanceof IOException io) throw io; throw new IOException("Schema history restoration failed",failure); }
        lane=Executors.newSingleThreadExecutor(task -> Thread.ofPlatform().daemon(true).name("ratis-schema-connection").unstarted(() -> { worker=Thread.currentThread(); task.run(); }));
        timer=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon(true).name("ratis-schema-observers").factory());
    }
    boolean isWorkerThread() { return worker==Thread.currentThread(); }
    private Path clientPath(UUID id) { return clientRoot.resolve(id+".request"); }
    private void checkpoint(Path file,Map<String,byte[]> fields) throws IOException {
        if (!Files.exists(file,LinkOption.NOFOLLOW_LINKS)) {
            var e=current;
            if (e==null || !file.equals(clientPath(e.id))) throw new IOException("Unowned initial schema checkpoint");
            if (e.cancelled || stopping || expired(e)) throw new CancellationException("Schema action did not reach its first durable checkpoint");
        }
        RatisSchemaClientJournal.write(file,fields);
    }
    RatisSchemaClient.TrustedOperatorResolver resolver() { return resolver; }
    CompletableFuture<Long> check(String name,byte[] hash,RequestContext context) {
        var result=new CompletableFuture<Long>(); long deadline=clock.resolveMonotonicDeadline(context);
        synchronized (this) { if (closed || stopping || actual>=maximumQueued) return CompletableFuture.failedFuture(new RejectedExecutionException("Schema read admission exhausted")); actual++; }
        lane.execute(() -> { try { if (deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0) throw new TimeoutException("Operator check deadline expired"); var value=client.checkOperator(name,hash); if (deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0) throw new TimeoutException("Operator check deadline expired"); result.complete(value); } catch (Throwable failure) { result.completeExceptionally(failure); } finally { synchronized (this) { actual--; } } });
        return result.copy();
    }
    synchronized List<Status> pending() { return entries.values().stream().filter(e -> e.state!=State.DONE && e.state!=State.ABORTED).map(Entry::status).toList(); }
    synchronized Status status(UUID id) { var e=entries.get(id); if (e==null) throw new IllegalArgumentException("Unknown schema request token"); return e.status(); }
    CompletableFuture<Result> submit(Kind kind,long charge,RequestContext context,Prepare prepare) {
        Entry e=new Entry(UUID.randomUUID(),kind,State.QUEUED);
        synchronized (this) { if (entries.size()>=maximumRequests) return CompletableFuture.failedFuture(new IOException("Durable schema request capacity exhausted")); entries.put(e.id,e); }
        return schedule(e,charge,context,prepare,false);
    }
    CompletableFuture<Result> resume(UUID id,RequestContext context,boolean abort) {
        final Entry e;
        synchronized (this) { e=entries.get(id); if (e==null) return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown schema request token")); if (e.result!=null && (e.state==State.DONE || e.state==State.ABORTED)) return CompletableFuture.completedFuture(e.result); if (e.state==State.NOT_STARTED) return CompletableFuture.failedFuture(new RatisSchemaRequestPendingException(e.status(),new IOException("No effects; a new action requires a new common call"))); }
        return schedule(e,65536,context,null,abort);
    }
    private CompletableFuture<Result> schedule(Entry e,long charge,RequestContext context,Prepare prepare,boolean abort) {
        final CompletableFuture<Result> view=new CompletableFuture<>();
        synchronized (this) {
            if (stopping || closed || e.observer!=null || actual>=maximumQueued || charge<0 || charge>maximumBytes-bytes) { e.state=prepare==null ? State.PAUSED : State.NOT_STARTED; view.completeExceptionally(new RatisSchemaRequestPendingException(e.status(),new RejectedExecutionException("Schema bridge admission exhausted/closing"))); return view; }
            e.deadline=clock.resolveMonotonicDeadline(Objects.requireNonNull(context)); e.cancelled=false; e.observer=view; actual++; bytes+=charge;
        }
        view.whenComplete((value,failure) -> { if (view.isCancelled()) e.cancelled=true; });
        ScheduledFuture<?> timeout=e.deadline==Long.MAX_VALUE ? null : timer.schedule(() -> view.completeExceptionally(new RatisSchemaRequestPendingException(e.status(),new TimeoutException("Original schema request deadline expired"))),Math.max(0,clock.remainingMonotonicDeadlineNanos(e.deadline)),TimeUnit.NANOSECONDS);
        lane.execute(() -> run(e,charge,prepare,abort,view,timeout)); return view;
    }
    private boolean expired(Entry e) { return e.deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(e.deadline)<=0; }
    private void run(Entry e,long charge,Prepare prepare,boolean abort,CompletableFuture<Result> view,ScheduledFuture<?> timeout) {
        RatisSchemaClient.Handle handle=null; Result outcome=null; Throwable problem=null;
        try {
            current=e; persist(e);
            if (e.cancelled || stopping || expired(e)) { e.state=Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS) ? State.PAUSED : State.NOT_STARTED; throw new CancellationException("Schema action paused before controller dispatch"); }
            handle=prepare==null ? client.reopen(e.id,resolver) : prepare.create(client,e.id);
            e.state=State.RUNNING; e.phase=handle.phase(); persist(e);
            for (int visits=0;visits<8192;visits++) {
                // A visit checkpoints its actual outcome before any observer deadline/cancellation decision.
                e.phase=handle.phase();
                if (e.phase==RatisSchemaClient.Phase.DONE || e.phase==RatisSchemaClient.Phase.ABORTED || e.phase==RatisSchemaClient.Phase.CONFLICT) break;
                if (e.cancelled || stopping || expired(e)) { e.state=State.PAUSED; throw new CancellationException("Original schema token retained"); }
                if (abort) { client.abort(handle); abort=false; } else client.advance(handle,1);
            }
            e.phase=handle.phase();
            if (e.phase==RatisSchemaClient.Phase.CONFLICT) { e.state=State.CONFLICT; throw new IOException("Schema request has a definitive conflict"); }
            if (e.phase!=RatisSchemaClient.Phase.DONE && e.phase!=RatisSchemaClient.Phase.ABORTED) { e.state=State.PAUSED; throw new IOException("Schema request visit budget exhausted"); }
            var result=client.completed(handle); if (result==null || result.cleanupPending()) throw new IOException("Schema cleanup has not durably completed");
            var uploaded=e.kind==Kind.UPLOAD ? client.uploaded(handle) : null;
            e.result=new Result(e.id,e.kind,uploaded==null ? result.columnId() : uploaded.operator().version(),result.changed(),result.aborted()); e.state=result.aborted() ? State.ABORTED : State.DONE; persist(e);
            if (expired(e) || e.cancelled) throw new CancellationException("Completed durable result retained for original token");
            outcome=e.result;
        } catch (Throwable failure) {
            if (e.state!=State.DONE && e.state!=State.ABORTED && e.state!=State.CONFLICT) e.state=Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS) ? State.PAUSED : State.NOT_STARTED;
            try { persist(e); } catch (Throwable checkpoint) { failure.addSuppressed(checkpoint); }
            problem=new RatisSchemaRequestPendingException(e.status(),failure);
            try { if (handle!=null) { handle.close(); handle=null; } client.close(); client=factory.create(this::checkpoint); } catch (Throwable reset) { failure.addSuppressed(reset); }
        } finally {
            try { if (handle!=null) handle.close(); } catch (IOException failure) { if (problem==null) problem=new RatisSchemaRequestPendingException(e.status(),failure); else problem.addSuppressed(failure); }
            current=null; if (timeout!=null) timeout.cancel(false);
            synchronized (this) { e.observer=null; actual--; bytes-=charge; }
        }
        if (problem==null) view.complete(outcome); else view.completeExceptionally(problem);
    }
    synchronized boolean busy() { return actual!=0; }
    synchronized void stop() { stopping=true; for (var e:entries.values()) if (e.observer!=null) e.cancelled=true; }
    @Override public void close() throws IOException { stop(); synchronized (this) { if (actual!=0) throw new IOException("Schema actual I/O remains active; ownership retained"); if (closed) return; } client.close(); lane.shutdown(); timer.shutdown(); synchronized (this) { closed=true; } }
    private void persist(Entry e) throws IOException {
        var out=new ByteArrayOutputStream(); try (var data=new DataOutputStream(out)) { data.writeLong(e.id.getMostSignificantBits()); data.writeLong(e.id.getLeastSignificantBits()); data.writeUTF(e.kind.name()); data.writeUTF(e.state.name()); data.writeUTF(e.phase==null ? "" : e.phase.name()); data.writeBoolean(e.result!=null); if (e.result!=null) { data.writeLong(e.result.value()); data.writeBoolean(e.result.changed()); data.writeBoolean(e.result.aborted()); } }
        SessionJournalFiles.write(root.resolve(e.id+".bridge"),out.toByteArray(),4096);
    }
    private static Entry decode(byte[] bytes) throws IOException {
        try (var in=new DataInputStream(new ByteArrayInputStream(bytes))) { var e=new Entry(new UUID(in.readLong(),in.readLong()),Kind.valueOf(in.readUTF()),State.valueOf(in.readUTF())); String phase=in.readUTF(); e.phase=phase.isEmpty() ? null : RatisSchemaClient.Phase.valueOf(phase); if (in.readBoolean()) e.result=new Result(e.id,e.kind,in.readLong(),in.readBoolean(),in.readBoolean()); if (in.read()!=-1) throw new IOException("Trailing schema bridge bytes"); return e; } catch (RuntimeException invalid) { throw new IOException("Invalid schema bridge record",invalid); }
    }
}
