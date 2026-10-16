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
    @FunctionalInterface interface ClientFactory { RatisSchemaClient create(UUID domain,RatisSchemaClient.Checkpoint checkpoint) throws IOException; }
    @FunctionalInterface interface Prepare { RatisSchemaClient.Handle create(RatisSchemaClient client,UUID id) throws Exception; }
    private final Path root,clientRoot;
    private final RWScheduler clock;
    private final ClientFactory factory;
    private final RatisSchemaClient.TrustedOperatorResolver resolver;
    private final RatisJournalAccess journalAccess;
    private final int maximumRequests,maximumQueued;
    private final long maximumBytes;
    private final ExecutorService lane;
    private final ScheduledExecutorService timer;
    private final Map<UUID,Entry> entries=new LinkedHashMap<>();
    private RatisSchemaClient client;private UUID clientDomain;private final Map<UUID,RatisRoutedClient> routed;
    private Entry current;
    private int actual;
    private long bytes;
    private boolean stopping,closed;
    private volatile Thread worker;
    private static final class Entry {
        final UUID id,domain; final Kind kind;
        volatile State state; volatile RatisSchemaClient.Phase phase; volatile Result result;
        private volatile Status terminalCandidate;
        private volatile boolean retained;
        volatile boolean cancelled; volatile long deadline;
        CompletableFuture<Result> observer;
        Entry(UUID id,UUID domain,Kind kind,State state) { this.id=id;this.domain=Objects.requireNonNull(domain); this.kind=kind; this.state=state; }
        synchronized Status status() { return new Status(id,kind,state,phase,result); }
    }
    RatisSchemaJobBridge(Path root,Path clientRoot,RWScheduler clock,ClientFactory factory,RatisSchemaClient.TrustedOperatorResolver resolver,Map<UUID,RatisRoutedClient> routed,int requests,int queued,long maximumBytes,RatisJournalAccess journalAccess) throws IOException {
        this.journalAccess=Objects.requireNonNull(journalAccess);
        this.routed=Map.copyOf(routed);if(routed.isEmpty())throw new IllegalArgumentException("Schema bridge requires declared domains");this.root=root; this.clientRoot=clientRoot; this.clock=clock; this.factory=factory; this.resolver=resolver; maximumRequests=requests; maximumQueued=queued; this.maximumBytes=maximumBytes;
        Files.createDirectories(root); Files.createDirectories(clientRoot);
        try (var files=Files.newDirectoryStream(root,"*.bridge")) {
            for (var file:files) { if (entries.size()>=requests) throw new IOException("Schema request inventory exceeds connection limits"); var e=decode(journalAccess.read(file,4096)); if (!routed.containsKey(e.domain) || !file.getFileName().toString().equals(e.id+".bridge") || entries.putIfAbsent(e.id,e)!=null) throw new IOException("Invalid schema bridge identity"); SessionJournalFiles.force(file);SessionJournalFiles.force(root);e.retained=true; if (e.state!=State.DONE && e.state!=State.ABORTED && e.state!=State.CONFLICT) e.state=e.terminalCandidate!=null || Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS) ? State.PAUSED : State.NOT_STARTED; }
        }
        try {
            for(var domain:routed.keySet()) {
                select(domain);var data=routed.get(domain);var history=new ArrayList<Map.Entry<UUID,Long>>();
                for(var entry:entries.values())if(entry.domain.equals(domain) && Files.exists(clientPath(entry.id),LinkOption.NOFOLLOW_LINKS)){var fields=RatisSchemaClientJournal.readOwned(clientPath(entry.id),journalAccess);var definition=fields.get("definition");if(definition!=null)history.add(Map.entry(entry.id,it.cavallium.rockserver.core.common.distributed.SchemaChange.Definition.decode(definition).targetSchema().schemaEpoch()));}
                history.sort(Map.Entry.comparingByValue());long bootstrapEpoch=data.directory().schemaEpoch();
                for(var entry:history)if(entry.getValue()>bootstrapEpoch)try(var handle=client.reopen(entry.getKey(),resolver)){client.completed(handle);}
                history.sort(Map.Entry.<UUID,Long>comparingByValue().reversed());
                for(var entry:history)if(entry.getValue()<=data.directory().schemaEpoch())try(var handle=client.reopen(entry.getKey(),resolver)){client.completed(handle);}
            }
        }catch(Throwable failure){try{if(client!=null)client.close();}catch(Throwable cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Schema history restoration failed",failure);}
        lane=Executors.newSingleThreadExecutor(task -> Thread.ofPlatform().daemon(true).name("ratis-schema-connection").unstarted(() -> { worker=Thread.currentThread(); task.run(); }));
        timer=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon(true).name("ratis-schema-observers").factory());
    }
    private void select(UUID domain)throws IOException {if(!routed.containsKey(domain))throw new IOException("Schema domain is not declared");if(domain.equals(clientDomain))return;if(client!=null)client.close();client=null;clientDomain=null;client=factory.create(domain,this::checkpoint);clientDomain=domain;}
    boolean isWorkerThread() { return worker==Thread.currentThread(); }
    private Path clientPath(UUID id) { return clientRoot.resolve(id+".request"); }
    private void checkpoint(Path file,Map<String,byte[]> fields) throws IOException {
        if (!Files.exists(file,LinkOption.NOFOLLOW_LINKS)) {
            var e=current;
            if (e==null || !file.equals(clientPath(e.id))) throw new IOException("Unowned initial schema checkpoint");
            if (e.cancelled || stopping || expired(e)) throw new CancellationException("Schema action did not reach its first durable checkpoint");
        }
        RatisSchemaClientJournal.writeOwned(file,fields,journalAccess);
    }
    RatisSchemaClient.TrustedOperatorResolver resolver() { return resolver; }
    CompletableFuture<Long> check(UUID domain,String name,byte[] hash,RequestContext context) {
        var result=new CompletableFuture<Long>(); long deadline=clock.resolveMonotonicDeadline(context);
        synchronized (this) { if (closed || stopping || actual>=maximumQueued) return CompletableFuture.failedFuture(new RejectedExecutionException("Schema read admission exhausted")); actual++; }
        lane.execute(() -> { try { if (deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0) throw new TimeoutException("Operator check deadline expired"); select(domain);var value=client.checkOperator(name,hash); if (deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0) throw new TimeoutException("Operator check deadline expired"); result.complete(value); } catch (Throwable failure) { result.completeExceptionally(failure); } finally { synchronized (this) { actual--; } } });
        return result.copy();
    }
    synchronized List<Status> pending() { return entries.values().stream().filter(e -> e.retained && e.state!=State.DONE && e.state!=State.ABORTED).map(Entry::status).toList(); }
    synchronized List<Status> inventory(){return entries.values().stream().filter(e->e.retained).map(Entry::status).toList();}
    synchronized Status status(UUID id) { var e=entries.get(Objects.requireNonNull(id)); if (e==null || !e.retained) throw unknown(id); return e.status(); }
    private static it.cavallium.rockserver.core.common.RocksDBException unknown(UUID id){return it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SCHEMA_OPERATION_NOT_FOUND,"Unknown retained schema operation "+id);}
    CompletableFuture<Result> submit(UUID domain,Kind kind,long charge,RequestContext context,Prepare prepare) {
        Entry e=new Entry(UUID.randomUUID(),domain,kind,State.QUEUED);
        synchronized (this) { if (entries.size()>=maximumRequests) return CompletableFuture.failedFuture(it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Durable schema request capacity exhausted")); entries.put(e.id,e); }
        return schedule(e,charge,context,prepare,false);
    }
    CompletableFuture<Result> resume(UUID id,RequestContext context,boolean abort) {
        final Entry e;
        synchronized (this) { e=entries.get(Objects.requireNonNull(id));if(e==null || !e.retained)return CompletableFuture.failedFuture(unknown(id));if(e.state==State.CONFLICT || e.state==State.NOT_STARTED && e.terminalCandidate==null && !abort)return CompletableFuture.failedFuture(it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SCHEMA_OPERATION_NOT_RESUMABLE,"Retained schema operation has no resumable controller: "+id)); }
        return schedule(e,65536,context,null,abort);
    }
    private CompletableFuture<Result> schedule(Entry e,long charge,RequestContext context,Prepare prepare,boolean abort) {
        final CompletableFuture<Result> view=new CompletableFuture<>();
        synchronized (this) {
            if (stopping || closed || e.observer!=null || actual>=maximumQueued || charge<0 || charge>maximumBytes-bytes) {
                if(prepare!=null){entries.remove(e.id,e);view.completeExceptionally(it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Schema bridge admission exhausted/closing"));}
                else view.completeExceptionally(new RatisSchemaRequestPendingException(e.status(),new RejectedExecutionException("Schema bridge admission exhausted/closing")));
                return view;
            }
            e.deadline=clock.resolveMonotonicDeadline(Objects.requireNonNull(context)); e.cancelled=false; e.observer=view; actual++; bytes+=charge;
        }
        view.whenComplete((value,failure) -> { if (view.isCancelled()) e.cancelled=true; });
        ScheduledFuture<?> timeout=e.deadline==Long.MAX_VALUE ? null : timer.schedule(() -> view.completeExceptionally(e.retained?new RatisSchemaRequestPendingException(e.status(),new TimeoutException("Original schema request deadline expired")):it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Schema request deadline expired before initial retention")),Math.max(0,clock.remainingMonotonicDeadlineNanos(e.deadline)),TimeUnit.NANOSECONDS);
        lane.execute(() -> run(e,charge,prepare,abort,view,timeout)); return view;
    }
    private boolean expired(Entry e) { return e.deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(e.deadline)<=0; }
    private void run(Entry e,long charge,Prepare prepare,boolean abort,CompletableFuture<Result> view,ScheduledFuture<?> timeout) {
        RatisSchemaClient.Handle handle=null; Result outcome=null; Throwable problem=null;boolean controller=false;
        try {
            current=e; persist(e);
            if (e.cancelled || stopping || expired(e)) { if(e.result==null && e.state!=State.CONFLICT)e.state=Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS) ? State.PAUSED : State.NOT_STARTED; throw new CancellationException("Schema action paused before controller dispatch"); }
            if(e.state==State.CONFLICT)throw new IOException("Schema request has a definitive conflict");
            if(e.result!=null && (e.state==State.DONE || e.state==State.ABORTED)) {
                outcome=e.result; // Reforce the original terminal row before acknowledging an exact retry.
            }else if(prepare==null && abort && !Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS)) {
                e.terminalCandidate=new Status(e.id,e.kind,State.ABORTED,null,new Result(e.id,e.kind,0,false,true));persist(e);outcome=e.result;
            }else {
            controller=true;select(e.domain);
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
            if (e.phase==RatisSchemaClient.Phase.CONFLICT) { e.terminalCandidate=new Status(e.id,e.kind,State.CONFLICT,e.phase,null);persist(e);throw new IOException("Schema request has a definitive conflict"); }
            if (e.phase!=RatisSchemaClient.Phase.DONE && e.phase!=RatisSchemaClient.Phase.ABORTED) { e.state=State.PAUSED; throw new IOException("Schema request visit budget exhausted"); }
            var result=client.completed(handle); if (result==null || result.cleanupPending()) throw new IOException("Schema cleanup has not durably completed");
            var uploaded=e.kind==Kind.UPLOAD ? client.uploaded(handle) : null;
            e.terminalCandidate=new Status(e.id,e.kind,result.aborted()?State.ABORTED:State.DONE,e.phase,new Result(e.id,e.kind,uploaded==null ? result.columnId() : uploaded.operator().version(),result.changed(),result.aborted()));persist(e);
            if (expired(e) || e.cancelled) throw new CancellationException("Completed durable result retained for original token");
            outcome=e.result;
            }
        } catch (Throwable failure) {
            if (e.state!=State.DONE && e.state!=State.ABORTED && e.state!=State.CONFLICT) e.state=e.terminalCandidate!=null || Files.exists(clientPath(e.id),LinkOption.NOFOLLOW_LINKS) ? State.PAUSED : State.NOT_STARTED;
            try { persist(e); } catch (Throwable checkpoint) { failure.addSuppressed(checkpoint); }
            problem=e.retained?new RatisSchemaRequestPendingException(e.status(),failure):it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN,"Schema request initial retention is unconfirmed",failure);
            try { if (handle!=null) { handle.close(); handle=null; } if(controller){if(client!=null)client.close(); client=factory.create(e.domain,this::checkpoint);clientDomain=e.domain;} } catch (Throwable reset) { failure.addSuppressed(reset); }
        } finally {
            try { if (handle!=null) handle.close(); } catch (IOException failure) { if (problem==null) problem=e.retained?new RatisSchemaRequestPendingException(e.status(),failure):it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN,"Schema request initial retention is unconfirmed",failure); else problem.addSuppressed(failure); }
            current=null; if (timeout!=null) timeout.cancel(false);
            synchronized (this) { e.observer=null; actual--; bytes-=charge; }
        }
        if (problem==null) view.complete(outcome); else view.completeExceptionally(problem);
    }
    synchronized boolean busy() { return actual!=0; }
    synchronized void stop() { stopping=true; for (var e:entries.values()) if (e.observer!=null) e.cancelled=true; }
    @Override public void close() throws IOException { stop(); synchronized (this) { if (actual!=0) throw new IOException("Schema actual I/O remains active; ownership retained"); if (closed) return; } client.close(); lane.shutdown(); timer.shutdown(); synchronized (this) { closed=true; } }
    private void persist(Entry e) throws IOException {
        var candidate=e.terminalCandidate;var status=candidate==null?e.status():candidate;
        var out=new ByteArrayOutputStream();try(var data=new DataOutputStream(out)){
            data.writeLong(e.id.getMostSignificantBits());data.writeLong(e.id.getLeastSignificantBits());data.writeLong(e.domain.getMostSignificantBits());data.writeLong(e.domain.getLeastSignificantBits());
            data.writeUTF(e.kind.name());data.writeUTF(status.state().name());data.writeUTF(status.phase()==null?"":status.phase().name());data.writeBoolean(status.result()!=null);
            if(status.result()!=null){data.writeLong(status.result().value());data.writeBoolean(status.result().changed());data.writeBoolean(status.result().aborted());}
        }
        journalAccess.write(root.resolve(e.id+".bridge"),out.toByteArray(),4096);
        synchronized(e){if(candidate!=null){e.phase=candidate.phase();e.result=candidate.result();e.state=candidate.state();e.terminalCandidate=null;}e.retained=true;}
    }
    private static Entry decode(byte[] bytes) throws IOException {
        try (var in=new DataInputStream(new ByteArrayInputStream(bytes))) { var e=new Entry(new UUID(in.readLong(),in.readLong()),new UUID(in.readLong(),in.readLong()),Kind.valueOf(in.readUTF()),State.valueOf(in.readUTF())); String phase=in.readUTF(); e.phase=phase.isEmpty() ? null : RatisSchemaClient.Phase.valueOf(phase); if (in.readBoolean()) e.result=new Result(e.id,e.kind,in.readLong(),in.readBoolean(),in.readBoolean()); if (in.read()!=-1) throw new IOException("Trailing schema bridge bytes"); if((e.state==State.DONE || e.state==State.ABORTED)!=(e.result!=null) || e.result!=null && (e.result.aborted()!=(e.state==State.ABORTED) || e.result.aborted() && e.result.changed()))throw new IOException("Inconsistent schema bridge outcome"); return e; } catch (RuntimeException invalid) { throw new IOException("Invalid schema bridge record",invalid); }
    }
}
