package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** One durable public atomic request per immutable complete generation. Observers never own work. */
final class RatisAtomicMutationOwner implements AutoCloseable {
    static final int MAX_BYTES=32*1024*1024;
    record Owned(CompletableFuture<Void> observer,CompletableFuture<Void> drained,Runnable cancel) {}
    private enum Phase { PREPARED, ALLOCATING, ALLOCATED, CAPTURING, STAGING, ACCEPTED, PUBLISHING, RESULT, REJECTING, DONE }
    private static final class Job {
        RemoteMutationToken token;List<AtomicMutation> mutations;List<AtomicPredicate> predicates;long ttlMillis,expires;
        Phase phase=Phase.PREPARED;boolean snapshotPublished;RatisTransactionClient.Identity identity;SnapshotFence fence;
        RatisTransactionPlan.Accepted accepted;CommandResult result;boolean acknowledged;
    }
    private final RatisTransactionBindings data;
    private final RatisSnapshotFences snapshotHomes;private final RatisTransactionStatuses homes;
    private final RatisTransactionClient transactions;
    private final RatisAtomicClient atomic;
    private final RatisRocksDBAdapter.AllocationSource allocations;
    private final RWScheduler scheduler;
    private final RatisRequestBudget budget;
    private final RatisRequestBudget.Owner ownerCredit;
    private final Path root,input,snapshot,child;
    private final FileChannel channel;private final FileLock lock;
    private final UUID ownerId;private final RatisRemoteMutationReceipts receipts;
    private RatisRequestBudget.Lease requestCredit;
    private RatisTransactionClient.Lease lease;private RatisTransactionClient.Cleanup cleanup;
    private RatisAtomicClient.Operation operation;
    private Job job;private boolean active,closed,fenced;

    RatisAtomicMutationOwner(RatisTransactionBindings data,RatisSnapshotFences snapshotHomes,RatisTransactionStatuses homes,
            RWScheduler scheduler,RatisRocksDBAdapter.AllocationSource allocations,Path root,RatisRequestBudget budget)throws IOException {
        this.data=data;this.snapshotHomes=snapshotHomes;this.homes=homes;this.scheduler=scheduler;this.allocations=allocations;this.budget=budget;this.root=root.toAbsolutePath().normalize();
        input=this.root.resolve("input");snapshot=this.root.resolve("snapshot");child=this.root.resolve("commit");
        long metadata=data.domains().stream().mapToLong(d->DistributedSchemaCodec.encodedSize(d.schema())+RangeDirectoryCodec.encodedSize(d.directory())).sum();
        ownerCredit=budget.owner(RemoteMutationToken.Kind.ATOMIC,4*metadata,RatisRequestBudget.files(input,pending(input),snapshot,pending(snapshot),child,pending(child)));
        FileChannel opened=null;FileLock acquired=null;
        try {
            budget.register(this.root.resolve("owner.lock"),32);Files.createDirectories(this.root);opened=FileChannel.open(this.root.resolve("owner.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            try{acquired=opened.tryLock();}catch(OverlappingFileLockException owned){throw new IOException("Atomic owner already active",owned);}if(acquired==null)throw new IOException("Atomic owner already active");
            channel=opened;lock=acquired;Path owner=this.root.resolve("owner");boolean used=Files.exists(owner,LinkOption.NOFOLLOW_LINKS);
            if(used){var bytes=budget.read(owner,128);if(bytes.length!=16)throw new IOException("Invalid atomic owner identity");ownerId=uuid(ByteBuffer.wrap(bytes));}
            else {if(present(input)||present(pending(input)))throw new IOException("Atomic input lost original durable owner");ownerId=UUID.randomUUID();budget.write(owner,ByteBuffer.allocate(16).putLong(ownerId.getMostSignificantBits()).putLong(ownerId.getLeastSignificantBits()).array(),128);}
            receipts=new RatisRemoteMutationReceipts(this.root.resolve("released"),authority(0),used,budget);
            ownerCredit.requireOpening(RatisRequestBudget.files(input,pending(input),snapshot,pending(snapshot),child,pending(child)));recoverInput();receipts.validateJournal(job==null?null:job.token);if(job!=null)job.acknowledged=receipts.retired(job.token);
            if(job==null && (present(snapshot)||present(pending(snapshot))||present(child)||present(pending(child))))throw new IOException("Atomic child graph lost original input");
            requestCredit=job!=null && job.acknowledged?ownerCredit.openedWorkspace(retained(job)):ownerCredit.opened(job==null?0:retained(job),job!=null);
            transactions=RatisTransactionClient.journalOwned(data,snapshotHomes,homes,scheduler,RatisTransactionClient.Limits.defaults(),budget);
            atomic=new RatisAtomicClient(data,homes,scheduler,RatisAtomicClient.Limits.defaults(),budget,()->requestCredit);
        }catch(Throwable failure){if(requestCredit!=null)requestCredit.close();ownerCredit.close();if(acquired!=null)acquired.release();if(opened!=null)opened.close();if(failure instanceof IOException io)throw io;throw new IOException("Atomic owner open failed",failure);}
    }
    private RemoteMutationOwner authority(long sequence){return new RemoteMutationOwner(data.clusterId(),data.databaseId(),ownerId,0,sequence,RemoteMutationToken.Kind.ATOMIC,data.authority());}
    synchronized RemoteMutationOwner owner()throws IOException{requireOpen();return authority(receipts.nextSequence());}
    synchronized RemoteMutationToken prepare(RemoteMutationOwner expected,UUID requestId,long sequence,List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis)throws IOException {
        requireOpen();if(active)throw new IOException("Atomic work remains active");if(!owner().sameAuthority(expected))throw new IOException("Atomic original owner/vector differs");
        long inputBytes=RemoteMutationInputs.atomicSize(mutations,predicates,ttlMillis);
        var token=new RemoteMutationToken(data.clusterId(),data.databaseId(),ownerId,requestId,0,sequence,RemoteMutationToken.Kind.ATOMIC,data.authority(),RemoteMutationInputs.atomic(mutations,predicates,ttlMillis));
        receipts.requireNew(token);if(job!=null && !job.acknowledged){if(!job.token.equals(token))throw new IOException("Atomic original input differs");return token;}
        var declaration=scope(mutations,predicates);data.preflight(declaration);var routes=data.participants(declaration);
        // Charge input, saved results, immutable acceptance and future encoding before allocating identities.
        long retained=256*1024L+8*inputBytes+8L*CommandResult.MAX_BYTES+128*1024L*routes.size();
        var credit=budget.request(RemoteMutationToken.Kind.ATOMIC,retained);
        try {
            retire();var paths=journalGraph();budget.preflight(paths);requestCredit=credit;
            var candidate=new Job();candidate.token=token;candidate.mutations=mutations.stream().map(m->new AtomicMutation(m.column(),copy(m.key()),m.kind(),copy(m.value()),m.returns())).toList();candidate.predicates=predicates.stream().map(p->new AtomicPredicate(p.column(),copy(p.key()),copy(p.expected()))).toList();candidate.ttlMillis=ttlMillis;candidate.expires=Math.addExact(System.currentTimeMillis(),ttlMillis);
            // Schema/key/operator validation is pure and precedes the first allocation/capture.
            physical(candidate);job=candidate;persist();receipts.bind(token);return token;
        }catch(Throwable failure){if(requestCredit!=credit || job==null){credit.close();if(requestCredit==credit)requestCredit=null;}if(job==null)try{budget.fulfilled(journalGraph());}catch(IOException cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw failure;}
    }
    private List<RatisJournalAccess.Reservation> journalGraph(){return List.of(
        new RatisJournalAccess.Reservation(input,MAX_BYTES,2L*MAX_BYTES),
        new RatisJournalAccess.Reservation(root.resolve("released"),2*1024*1024,4L*1024*1024),
        new RatisJournalAccess.Reservation(snapshot,RatisDurableSnapshot.MAX_BYTES,2L*RatisDurableSnapshot.MAX_BYTES),
        new RatisJournalAccess.Reservation(snapshot.resolveSibling("snapshot.lock"),32,0),
        new RatisJournalAccess.Reservation(snapshot.resolveSibling("snapshot.pending.lock"),32,0),
        new RatisJournalAccess.Reservation(child,RatisDurableTransaction.MAX_BYTES,2L*RatisDurableTransaction.MAX_BYTES),
        new RatisJournalAccess.Reservation(child.resolveSibling("commit.lock"),32,0),
        new RatisJournalAccess.Reservation(child.resolveSibling("commit.pending.lock"),32,0));}
    private void requireOpen()throws IOException{if(closed || fenced)throw new IOException("Atomic owner closed or original force unresolved");}
    private Job exact(RemoteMutationToken token)throws IOException{if(receipts.retired(token))throw new IOException("Atomic request expired or acknowledged");if(job==null || !job.token.equals(token))throw new IOException("Original atomic token/input unavailable");return job;}
    synchronized RemoteMutationStatus status(RemoteMutationToken token)throws IOException {
        var retired=receipts.find(token);if(retired!=null)return retired;
        if(job==null || !job.token.requestId().equals(token.requestId()))return new RemoteMutationStatus(token,RemoteMutationStatus.State.UNAVAILABLE,-1,-1,0,null,null);
        var saved=exact(token);boolean done=saved.phase==Phase.DONE;
        return new RemoteMutationStatus(token,done?RemoteMutationStatus.State.TERMINAL:saved.phase==Phase.PREPARED?RemoteMutationStatus.State.PREPARED:RemoteMutationStatus.State.PENDING,
                done?(saved.result.status()==CommandResult.Status.SUCCESS?saved.mutations.size():0):saved.phase==Phase.PREPARED?0:-1,done || saved.phase==Phase.PREPARED?0:-1,done?1:0,done?saved.result.status():null,done?RemoteMutationToken.digest(saved.result.encode()):null);
    }
    synchronized List<byte[]> resultPage(RemoteMutationToken token,int offset,int maximumItems,int maximumBytes)throws IOException {
        if(active || fenced)throw new IOException("Atomic actual work or force unresolved");var saved=exact(token);
        if(offset<0 || offset>1 || maximumItems<1 || maximumItems>RemoteMutationResultPage.MAX_ITEMS || maximumBytes<1 || maximumBytes>CommandResult.MAX_BYTES)throw new IllegalArgumentException("Invalid atomic result page");
        if(saved.phase!=Phase.DONE || offset==1 || saved.result.encodedSize()>maximumBytes)return List.of();return List.of(saved.result.encode());
    }
    synchronized void acknowledge(RemoteMutationToken token,byte[] digest)throws IOException {
        requireOpen();if(active)throw new IOException("Atomic actual work active");var retired=receipts.find(token);if(retired!=null){if(!Arrays.equals(retired.resultDigest(),digest))throw new IOException("Atomic acknowledged digest changed");return;}
        var saved=exact(token);if(saved.phase!=Phase.DONE || !Arrays.equals(RemoteMutationToken.digest(saved.result.encode()),digest))throw new IOException("Atomic ACK differs from forced terminal result");
        receipts.acknowledge(new RemoteMutationStatus(token,RemoteMutationStatus.State.ACKNOWLEDGED,saved.result.status()==CommandResult.Status.SUCCESS?saved.mutations.size():0,0,0,saved.result.status(),digest));saved.acknowledged=true;retire();
    }
    Owned execute(RemoteMutationToken token,RequestContext context){
        final long deadline;try{deadline=scheduler.resolveMonotonicDeadline(context);synchronized(this){requireOpen();exact(token);if(active)throw new IOException("Atomic original work active");active=true;}}catch(Throwable failure){return new Owned(CompletableFuture.failedFuture(failure),CompletableFuture.completedFuture(null),()->{});}
        var observer=new CompletableFuture<Void>();var drained=new CompletableFuture<Void>();
        work(context,deadline,()->{if(job.phase==Phase.PREPARED){job.phase=Phase.ALLOCATING;persist();return true;}if(job.phase==Phase.ALLOCATING && job.identity==null)throw new RatisPreparationUnavailableException("Original identity allocation reply unknown; no replacement allocation allowed");return false;})
            .thenCompose(allocate->{if(!allocate)return CompletableFuture.completedFuture(null);return allocations.allocate(remaining(context,deadline),data.targets(scope(job.mutations,job.predicates))).thenCompose(issued->work(context,deadline,()->{job.identity=issued.identity();job.phase=Phase.ALLOCATED;persist();return null;}));})
            .thenCompose(ignored->drive(context,deadline))
            .exceptionallyCompose(failure->{if(job.accepted==null && oversized(failure))return rejectOutcome(CommandResult.Status.RESULT_TOO_LARGE);if(job.accepted==null && lost(failure))return rejectOriginal();return CompletableFuture.failedFuture(unwrap(failure));})
            .whenComplete((ignored,failure)->{synchronized(this){active=false;}drained.complete(null);if(failure==null)observer.complete(null);else observer.completeExceptionally(unwrap(failure));});
        return new Owned(observer,drained,()->observer.cancel(false));
    }
    private CompletableFuture<Void> drive(RequestContext context,long deadline){
        if(job.phase==Phase.DONE)return CompletableFuture.completedFuture(null);
        if(job.result!=null)return finish(context,deadline);
        if(job.accepted!=null)return publish(context,deadline);
        return work(context,deadline,()->{
            if(job.fence==null){job.fence=transactions.frozenDefinition(job.identity,scope(job.mutations,job.predicates));job.phase=Phase.CAPTURING;persist();}
            if(lease==null)recoverChildFile(snapshot,RatisDurableSnapshot.MAX_BYTES);
            // Only a forced outer publication marker permits acquisition RPCs and random native capture IDs.
            // Fence-only crashes may create their first journal; a published original must never be replaced.
            if(job.snapshotPublished && !present(snapshot))throw new IOException("Original atomic snapshot journal is missing; restore it before resuming the retained capture intent");
            if(lease==null){long ttl=job.expires-System.currentTimeMillis();if(ttl<1 || ttl>job.ttlMillis)throw new RatisSnapshotLostException("Original atomic capture lifetime expired");lease=transactions.openFrozen(snapshot,child,job.identity,job.fence,Duration.ofMillis(ttl),remaining(context,deadline),()->{if(!job.snapshotPublished){job.snapshotPublished=true;persist();}});}
            return null;
        }).thenCompose(ignored->{var acquired=lease.completion();return acquired.isCompletedExceptionally()?lease.resume(remaining(context,deadline)):acquired;}).thenCompose(ignored->work(context,deadline,()->{job.phase=Phase.STAGING;persist();return null;}))
          .thenCompose(ignored->lease.stage(changes(job),conditions(job),remaining(context,deadline)))
          .thenCompose(results->{
              var rejected=results.entrySet().stream().sorted(Map.Entry.comparingByKey(RangeDirectoryCodec.UUID_ORDER)).map(Map.Entry::getValue).filter(result->result.status()!=CommandResult.Status.SUCCESS).findFirst();
              if(rejected.isPresent())return rejectOutcome(rejected.get().status());
              return work(context,deadline,()->{job.accepted=lease.freezeAcceptance();job.phase=Phase.ACCEPTED;persist();return null;}).thenCompose(ignored->publish(context,deadline));
          });
    }
    private CompletableFuture<Void> publish(RequestContext context,long deadline){
        return work(context,deadline,()->{
            if(job.phase.ordinal()<Phase.PUBLISHING.ordinal()){job.phase=Phase.PUBLISHING;persist();}
            recoverChildFile(child,RatisDurableTransaction.MAX_BYTES);
            if(operation==null){if(present(child)){validateAtomicFile(child);operation=atomic.reopen(child,remaining(context,deadline));}else try{operation=atomic.executeAccepted(child,job.identity.commit(),job.accepted,remaining(context,deadline));}finally{if(operation==null)operation=atomic.operation(child);}}
            return null;
        }).thenCompose(ignored->operation.resume(remaining(context,deadline)))
          .thenCompose(result->work(context,deadline,()->{job.result=result;job.phase=Phase.RESULT;persist();return null;}))
          .thenCompose(ignored->finish(context,deadline));
    }
    private CompletableFuture<Void> finish(RequestContext context,long deadline){
        return work(context,deadline,()->{
            if(operation==null && job.accepted!=null){recoverChildFile(child,RatisDurableTransaction.MAX_BYTES);if(!present(child))throw new IOException("Forced atomic result lost its exact child journal");validateAtomicFile(child);operation=atomic.reopen(child,remaining(context,deadline));}return null;
        }).thenCompose(ignored->{if(operation==null)return CompletableFuture.completedFuture(null);return operation.resume(remaining(context,deadline)).thenCompose(result->{if(!Arrays.equals(job.result.encode(),result.encode()))return CompletableFuture.failedFuture(new IOException("Original atomic child result changed"));return operation.consume(remaining(context,deadline));});})
          .thenCompose(ignored->work(context,deadline,()->{if(operation!=null){operation.close();operation=null;}if(lease!=null && job.accepted!=null)lease.finishAcceptedOwnership();if(lease==null && cleanup==null && present(snapshot))cleanup=transactions.reopenForRelease(snapshot,remaining(context,deadline));return null;}))
          .thenCompose(ignored->lease!=null?lease.release(remaining(context,deadline)):cleanup!=null?cleanup.resume(remaining(context,deadline)):CompletableFuture.completedFuture(null))
          .thenCompose(ignored->work(context,deadline,()->{if(lease!=null){lease.close();lease=null;}if(cleanup!=null){cleanup.close();cleanup=null;}job.phase=Phase.DONE;persist();return null;}));
    }
    private static boolean oversized(Throwable failure){for(Throwable cause=failure;cause!=null;cause=cause.getCause())if(cause instanceof RatisStagedResultTooLargeException)return true;return false;}
    private static boolean lost(Throwable failure){for(Throwable cause=failure;cause!=null;cause=cause.getCause())if(cause instanceof RatisSnapshotLostException || cause instanceof RatisPreparationUnavailableException)return true;return false;}
    private CompletableFuture<Void> rejectOriginal(){
        var cleanupContext=RequestContext.batch();
        return rejectOutcome(CommandResult.Status.SESSION_EXPIRED);
    }
    private CompletableFuture<Void> rejectOutcome(CommandResult.Status outcome){
        if(outcome==CommandResult.Status.SUCCESS)throw new IllegalArgumentException("No-effect rejection requires a failure outcome");
        var cleanupContext=RequestContext.batch();return work(cleanupContext,Long.MAX_VALUE,()->{job.result=new CommandResult(outcome,List.of());job.phase=Phase.REJECTING;persist();return null;}).thenCompose(ignored->finish(cleanupContext,Long.MAX_VALUE));
    }
    private <T> CompletableFuture<T> work(RequestContext context,long deadline,Callable<T> body){
        var result=new CompletableFuture<T>();final class Work implements Runnable,RWScheduler.RejectionAwareTask {
            final java.util.concurrent.atomic.AtomicBoolean claimed=new java.util.concurrent.atomic.AtomicBoolean();
            public void reject(RuntimeException failure){if(claimed.compareAndSet(false,true))result.completeExceptionally(failure);}
            public void run(){if(!claimed.compareAndSet(false,true))return;try{requireOpen();remaining(context,deadline);result.complete(body.call());}catch(Throwable failure){result.completeExceptionally(failure);}}
        }var work=new Work();try{scheduler.executor(context.profile(),OperationFamily.MUTATION,deadline).execute(work,WorkloadCost.MAX_ESTIMATED_BYTES);}catch(RuntimeException failure){work.reject(failure);}return result;
    }
    private RequestContext remaining(RequestContext context,long deadline){if(deadline==Long.MAX_VALUE)return context;long nanos=scheduler.remainingMonotonicDeadlineNanos(deadline);if(nanos<=0)throw new CompletionException(new TimeoutException("Atomic observation deadline expired; original ownership retained"));return new RequestContext(context.profile(),nanos);}
    private TransactionScope scope(List<AtomicMutation> mutations,List<AtomicPredicate> predicates){var spans=new ArrayList<TransactionScope.Span>();for(var m:mutations){var schema=data.schema(m.column());var column=schema.column(m.column()).columnId();spans.add(TransactionScope.Span.point(column,schema.encodeLogicalKey(column,m.key()).physicalKey()));}for(var p:predicates){var schema=data.schema(p.column());var column=schema.column(p.column()).columnId();spans.add(TransactionScope.Span.point(column,schema.encodeLogicalKey(column,p.key()).physicalKey()));}return new TransactionScope(spans);}
    private static List<RatisLogicalClient.Change> changes(Job job){return job.mutations.stream().map(m->switch(m.kind()){case PUT->RatisLogicalClient.Change.put(m.column(),m.key(),m.value(),m.returns());case DELETE->RatisLogicalClient.Change.delete(m.column(),m.key(),m.returns());case MERGE->RatisLogicalClient.Change.merge(m.column(),m.key(),m.value(),m.returns());default->throw new IllegalArgumentException("Invalid atomic write");}).toList();}
    private static List<RatisLogicalClient.Condition> conditions(Job job){return job.predicates.stream().map(p->new RatisLogicalClient.Condition(p.column(),p.key(),p.expected())).toList();}
    private RatisLogicalClient.Plan physical(Job job){return RatisLogicalClient.plan(data::schema,changes(job),conditions(job),2L*1024*1024);}
    private static Buf copy(Buf value){return value==null?null:Buf.wrap(value.toByteArray());}
    private static Keys copy(Keys keys){return new Keys(Arrays.stream(keys.keys()).map(RatisAtomicMutationOwner::copy).toArray(Buf[]::new));}
    private static Path pending(Path path){return path.resolveSibling(path.getFileName()+".pending");}
    private static boolean present(Path path){return Files.exists(path,LinkOption.NOFOLLOW_LINKS);}
    private static Throwable unwrap(Throwable failure){while((failure instanceof CompletionException || failure instanceof ExecutionException)&&failure.getCause()!=null)failure=failure.getCause();return failure;}
    private long retained(Job saved){return 256*1024L+8*RemoteMutationInputs.atomicSize(saved.mutations,saved.predicates,saved.ttlMillis)+8L*CommandResult.MAX_BYTES+128*1024L*data.participants(scope(saved.mutations,saved.predicates)).size();}
    private void persist()throws IOException {
        try{budget.write(input,encode(job),MAX_BYTES);}catch(IOException|RuntimeException failure){fenced=true;throw failure;}
    }
    private byte[] encode(Job saved)throws IOException {
        var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);out.writeInt(0x52414d4f);blob(out,saved.token.encode());out.writeLong(saved.ttlMillis);out.writeLong(saved.expires);out.writeByte(saved.phase.ordinal());out.writeBoolean(saved.snapshotPublished);
        out.writeInt(saved.mutations.size());for(var m:saved.mutations){out.writeLong(m.column());key(out,m.key());out.writeByte(m.kind().ordinal());value(out,m.value());out.writeByte(m.returns().ordinal());}
        out.writeInt(saved.predicates.size());for(var p:saved.predicates){out.writeLong(p.column());key(out,p.key());value(out,p.expected());}
        out.writeBoolean(saved.identity!=null);if(saved.identity!=null){uuid(out,saved.identity.snapshot().fence());operation(out,saved.identity.snapshot().business());operation(out,saved.identity.snapshot().firstControl());uuid(out,saved.identity.commit().transaction());operation(out,saved.identity.commit().business());operation(out,saved.identity.commit().firstControl());}
        nullable(out,saved.fence==null?null:saved.fence.encode());out.writeBoolean(saved.accepted!=null);if(saved.accepted!=null){blob(out,saved.accepted.initial().encode());out.writeInt(saved.accepted.intents().size());for(var intent:saved.accepted.intents()){uuid(out,intent.route().groupId());uuid(out,Objects.requireNonNull(intent.captureId()));blob(out,DistributedCommandCodec.encode(intent.acceptedBusiness()));out.writeInt(intent.resultPositions().size());for(int position:intent.resultPositions())out.writeInt(position);}}
        nullable(out,saved.result==null?null:saved.result.encode());out.flush();if(bytes.size()+32>MAX_BYTES)throw new IOException("Atomic outer journal exceeds bound");return bytes.toByteArray();
    }
    private Job decode(byte[] bytes)throws IOException {
        try(var in=new DataInputStream(new ByteArrayInputStream(bytes))){
            if(in.readInt()!=0x52414d4f)throw new IOException("Invalid atomic owner journal");var saved=new Job();saved.token=RemoteMutationToken.decode(blob(in,RemoteMutationToken.MAX_BYTES));
            var authority=new RemoteMutationOwner(saved.token.clusterId(),saved.token.databaseId(),saved.token.ownerId(),saved.token.ownerEpoch(),saved.token.requestSequence(),saved.token.kind(),saved.token.domains());if(!authority(0).sameAuthority(authority))throw new IOException("Atomic original vector/owner changed");
            saved.ttlMillis=in.readLong();saved.expires=in.readLong();int phase=in.readUnsignedByte();if(phase>=Phase.values().length)throw new IOException("Invalid atomic phase");saved.phase=Phase.values()[phase];saved.snapshotPublished=flag(in);
            int count=count(in,256);var mutations=new ArrayList<AtomicMutation>(count);long inputBytes=0;
            for(int n=0;n<count;n++){long column=in.readLong();var key=key(in);int kind=in.readUnsignedByte();var value=value(in);int returns=in.readUnsignedByte();if(kind>Mutation.Kind.MERGE.ordinal() || returns>=Mutation.ReturnMode.values().length)throw new IOException("Invalid atomic input kind");mutations.add(new AtomicMutation(column,key,Mutation.Kind.values()[kind],value,Mutation.ReturnMode.values()[returns]));inputBytes=RemoteMutationInputs.atomicSize(mutations,List.of(),saved.ttlMillis);}
            count=count(in,256-mutations.size());var predicates=new ArrayList<AtomicPredicate>(count);for(int n=0;n<count;n++){predicates.add(new AtomicPredicate(in.readLong(),key(in),value(in)));inputBytes=RemoteMutationInputs.atomicSize(mutations,predicates,saved.ttlMillis);}
            saved.mutations=List.copyOf(mutations);saved.predicates=List.copyOf(predicates);RemoteMutationInputs.atomicSize(saved.mutations,saved.predicates,saved.ttlMillis);
            if(!Arrays.equals(saved.token.inputDigest(),RemoteMutationInputs.atomic(saved.mutations,saved.predicates,saved.ttlMillis)))throw new IOException("Atomic original logical input digest changed");
            if(flag(in))saved.identity=new RatisTransactionClient.Identity(new RatisSnapshotClient.Identity(uuid(in),operation(in),operation(in)),new RatisAtomicClient.Identity(uuid(in),operation(in),operation(in)));
            var fence=nullable(in,SnapshotFence.MAX_BYTES);if(fence!=null)saved.fence=SnapshotFence.decode(fence);
            if(flag(in)){
                if(saved.identity==null || saved.fence==null)throw new IOException("Atomic acceptance lacks original capture definition");
                var initial=TransactionStatus.decode(blob(in,TransactionStatus.MAX_BYTES));int participants=count(in,TransactionStatus.MAX_PARTICIPANTS);var intents=new ArrayList<RatisDurableTransaction.Intent>(participants);
                for(int n=0;n<participants;n++){var group=uuid(in);var capture=uuid(in);var command=DistributedCommandCodec.decode(blob(in,DistributedCommandCodec.MAX_ENCODED_BYTES));var directory=data.directoryForGroup(group);var route=directory.ranges().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow();int positions=count(in,256);var mapping=new ArrayList<Integer>(positions);for(int p=0;p<positions;p++)mapping.add(in.readInt());var control=saved.identity.commit().firstControl();intents.add(new RatisDurableTransaction.Intent(route,command,new OperationId(control.sessionId(),control.epoch(),control.sequence()+n),new OperationId(control.sessionId(),control.epoch(),control.sequence()+64+n),mapping,capture));}
                saved.accepted=new RatisTransactionPlan.Accepted(initial,intents);validateAcceptance(saved);
            }
            var result=nullable(in,CommandResult.MAX_BYTES);if(result!=null)saved.result=CommandResult.decode(result);
            if(in.available()!=0 || saved.expires<1 || !validPhase(saved))throw new IOException("Invalid atomic durable phase graph");
            if(saved.fence!=null){data.require(saved.fence.domains());if(!saved.fence.equals(transactionsDefinition(saved)))throw new IOException("Atomic forced snapshot definition changed");}
            physical(saved);return saved;
        }catch(RuntimeException invalid){throw new IOException("Invalid atomic owner journal",invalid);}
    }
    private static boolean validPhase(Job saved){
        if(saved.snapshotPublished && saved.fence==null)return false;
        boolean rejected=saved.phase==Phase.REJECTING || saved.phase==Phase.DONE && saved.accepted==null;
        if(rejected)return saved.accepted==null && saved.result!=null && saved.result.status()!=CommandResult.Status.SUCCESS;
        if(saved.phase.ordinal()>=Phase.STAGING.ordinal() && !saved.snapshotPublished)return false;
        return (saved.identity!=null)==(saved.phase.ordinal()>=Phase.ALLOCATED.ordinal()) && (saved.fence!=null)==(saved.phase.ordinal()>=Phase.CAPTURING.ordinal()) && (saved.accepted!=null)==(saved.phase.ordinal()>=Phase.ACCEPTED.ordinal()) && (saved.result!=null)==(saved.phase.ordinal()>=Phase.RESULT.ordinal());
    }
    private SnapshotFence transactionsDefinition(Job saved){
        // Constructor recovery precedes client wrapper creation; build the same cached definition directly.
        var fence=saved.fence;return RatisSnapshotPlan.create(data.directories(),scope(saved.mutations,saved.predicates),saved.identity.snapshot().fence(),saved.identity.snapshot().business(),fence.homeGroup(),fence.homeEpoch(),saved.identity.snapshot().firstControl()).fence();
    }
    private void validateAcceptance(Job saved)throws IOException {
        var accepted=saved.accepted;var initial=accepted.initial();var identity=saved.identity.commit();initial.requireInitial();data.require(initial.domains());
        if(!initial.transactionId().equals(identity.transaction()) || !initial.operation().equals(identity.business()) || !Arrays.equals(initial.planDigest(),RatisDurableTransaction.planDigest(accepted.intents())) || accepted.intents().size()!=saved.fence.participants().size())throw new IOException("Atomic accepted identity/participant definition changed");
        var plan=physical(saved);var original=RatisTransactionPlan.create(data.directories(),identity.business(),plan.mutations(),plan.predicates());
        for(var intent:accepted.intents()){
            var partition=original.partitions().stream().filter(p->p.route().groupId().equals(intent.route().groupId())).findFirst().orElseThrow();
            var expected=DistributedCommand.acceptedTransaction(partition.command(),intent.acceptedBusiness().acceptance());
            if(!Arrays.equals(DistributedCommandCodec.encode(expected),DistributedCommandCodec.encode(intent.acceptedBusiness())) || !partition.resultPositions().equals(intent.resultPositions()) || !Arrays.equals(ScopedTransactionRpc.scopeBytes(intent.acceptedBusiness().acceptance().scope()),ScopedTransactionRpc.scopeBytes(saved.fence.participant(intent.route().groupId()).scope())))throw new IOException("Atomic accepted plan changed original input or scope");
        }
        RatisDurableTransaction.validateResultPlan(accepted.intents());
    }
    private void recoverInput()throws IOException {
        Job main=present(input)?decode(budget.read(input,MAX_BYTES)):null;
        if(present(pending(input))){var side=decode(budget.read(pending(input),MAX_BYTES));if(main!=null && (!side.token.equals(main.token) || side.expires!=main.expires || main.snapshotPublished && !side.snapshotPublished || side.phase.ordinal()<main.phase.ordinal() || main.identity!=null && !main.identity.equals(side.identity) || main.fence!=null && !main.fence.equals(side.fence) || main.result!=null && (side.result==null || !Arrays.equals(main.result.encode(),side.result.encode())) || main.accepted!=null && !sameAccepted(main.accepted,side.accepted)))throw new IOException("Atomic input sidecar changed original identity or regressed");main=side;forceRecovered(input,encode(main),MAX_BYTES);}
        job=main;
    }
    private static boolean sameAccepted(RatisTransactionPlan.Accepted expected,RatisTransactionPlan.Accepted actual){
        if(actual==null || !expected.initial().equals(actual.initial()) || expected.intents().size()!=actual.intents().size())return false;
        for(var original:expected.intents()){
            var candidate=actual.intents().stream().filter(intent->intent.route().groupId().equals(original.route().groupId())).findFirst().orElse(null);
            if(candidate==null || !original.route().equals(candidate.route()) || !Objects.equals(original.captureId(),candidate.captureId()) || !original.prepareControl().equals(candidate.prepareControl()) || !original.finalizeControl().equals(candidate.finalizeControl()) || !original.resultPositions().equals(candidate.resultPositions()) || !Arrays.equals(DistributedCommandCodec.encode(original.acceptedBusiness()),DistributedCommandCodec.encode(candidate.acceptedBusiness())))return false;
        }
        return true;
    }
    /** Qualification is synchronous and precedes reopen, whose asynchronous task may immediately contact HOME. */
    private void validateAtomicFile(Path path)throws IOException {
        if(job.accepted==null || job.identity==null)throw new IOException("Atomic child lacks forced outer acceptance/identity");
        try(var workspace=budget.work(65536+4*RatisRequestBudget.files(path));var journal=RatisDurableTransaction.reopen(path,homes,data)){
            var saved=journal.snapshot();var accepted=new RatisTransactionPlan.Accepted(saved.initial(),saved.participants().stream().map(RatisDurableTransaction.ParticipantProgress::intent).toList());
            if(!sameAccepted(job.accepted,accepted) || !saved.initial().transactionId().equals(job.identity.commit().transaction()) || !saved.initial().operation().equals(job.identity.commit().business()))throw new IOException("Atomic child changed original public token definition/accepted intents");
        }
    }
    private void recoverChildFile(Path path,int maximum)throws IOException {
        if(!present(pending(path)))return;
        budget.register(pending(path).resolveSibling(pending(path).getFileName()+".lock"),32);
        try(var workspace=budget.work(65536+4*RatisRequestBudget.files(path,pending(path)))){
            if(path.equals(snapshot)){
                RatisDurableSnapshot.Snapshot side;
                try(var retained=RatisDurableSnapshot.reopen(pending(path),snapshotHomes,data)){side=retained.snapshot();if(!side.initial().fence().equals(job.fence))throw new IOException("Snapshot sidecar changed original forced fence");}
                if(present(path))try(var retained=RatisDurableSnapshot.reopen(path,snapshotHomes,data)){requireSnapshotAdvance(retained.snapshot(),side);}
                // Capture IDs originate in this child journal. Preserve the newest forced request, including unknown replies.
                forceRecovered(path,budget.read(pending(path),maximum),maximum);return;
            }
            if(job.accepted==null)throw new IOException("Atomic child sidecar lacks forced outer acceptance");
            validateAtomicFile(pending(path));if(present(path))validateAtomicFile(path);
            // All atomic capture/control identities already belong to the forced outer acceptance.
            if(present(path)){SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());Files.delete(pending(path));SessionJournalFiles.force(path.getParent());}
            else forceRecovered(path,budget.read(pending(path),maximum),maximum);
        }
    }
    private static void requireSnapshotAdvance(RatisDurableSnapshot.Snapshot main,RatisDurableSnapshot.Snapshot side)throws IOException {
        if(!main.initial().equals(side.initial()) || main.captureMode()!=side.captureMode() || !main.requestedTtl().equals(side.requestedTtl()) || main.releaseRequested() && !side.releaseRequested() || side.lastHome().sequence()<main.lastHome().sequence() || main.observedHome()!=null && (side.observedHome()==null || side.observedHome().generation()<main.observedHome().generation()))throw new IOException("Snapshot sidecar regressed original definition or HOME progress");
        for(var old:main.participants()){
            var next=side.participant(old.route().groupId());
            if(!old.route().equals(next.route()) || old.installFrozen() && !next.installFrozen() || old.releaseFrozen() && !next.releaseFrozen() || old.captureLost() && !next.captureLost() || old.captureReleased() && !next.captureReleased() || old.captureReleaseLost() && !next.captureReleaseLost()
                    || old.capture()!=null && (next.capture()==null || !Arrays.equals(old.capture().bytes(),next.capture().bytes()))
                    || old.captureToken()!=null && !old.captureToken().equals(next.captureToken()) || old.certifiedHandle()!=null && !old.certifiedHandle().equals(next.certifiedHandle())
                    || old.installReceipt()!=null && !old.installReceipt().equals(next.installReceipt()) || old.releaseReceipt()!=null && !old.releaseReceipt().equals(next.releaseReceipt()))throw new IOException("Snapshot sidecar changed original capture or regressed cleanup");
        }
    }
    private void forceRecovered(Path path,byte[] bytes,int maximum)throws IOException {try(var disk=budget.disk(path,maximum)){disk.prepareOwnedReplacement(bytes.length+32L);SessionJournalFiles.write(path,bytes,maximum);disk.forcedReconcile();}}
    private void retire()throws IOException {
        if(job==null)return;if(!job.acknowledged || job.phase!=Phase.DONE)throw new IOException("Original atomic result not acknowledged");
        for(var path:List.of(snapshot,child,input)){if(present(pending(path)))throw new IOException("Original atomic child publication unresolved");if(present(path))budget.remove(path,budget.read(path,RatisDurableTransaction.MAX_BYTES),RatisDurableTransaction.MAX_BYTES,SessionJournalFiles.FILESYSTEM);}
        if(requestCredit!=null){requestCredit.close();requestCredit=null;}budget.fulfilled(journalGraph());job=null;
    }
    private static int count(DataInputStream in,int maximum)throws IOException{int count=in.readInt();if(count<0 || count>maximum || count>in.available()/4)throw new IOException("Atomic journal count exceeds bound");return count;}
    private static boolean flag(DataInputStream in)throws IOException{int value=in.readUnsignedByte();if(value>1)throw new IOException("Invalid atomic boolean");return value==1;}
    private static void uuid(DataOutputStream out,UUID value)throws IOException{out.writeLong(value.getMostSignificantBits());out.writeLong(value.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    private static UUID uuid(ByteBuffer in){return new UUID(in.getLong(),in.getLong());}
    private static void operation(DataOutputStream out,OperationId value)throws IOException{uuid(out,value.sessionId());out.writeLong(value.epoch());out.writeLong(value.sequence());}
    private static OperationId operation(DataInputStream in)throws IOException{return new OperationId(uuid(in),in.readLong(),in.readLong());}
    private static void blob(DataOutputStream out,byte[] value)throws IOException{out.writeInt(value.length);out.write(value);}
    private static byte[] blob(DataInputStream in,int maximum)throws IOException{int size=in.readInt();if(size<0 || size>maximum || size>in.available())throw new IOException("Atomic journal blob exceeds bound");return in.readNBytes(size);}
    private static void nullable(DataOutputStream out,byte[] value)throws IOException{if(value==null)out.writeInt(-1);else blob(out,value);}
    private static byte[] nullable(DataInputStream in,int maximum)throws IOException{in.mark(4);int size=in.readInt();if(size==-1)return null;in.reset();return blob(in,maximum);}
    private static void value(DataOutputStream out,Buf value)throws IOException{nullable(out,value==null?null:value.toByteArray());}
    private static Buf value(DataInputStream in)throws IOException{var bytes=nullable(in,2*1024*1024);return bytes==null?null:Buf.wrap(bytes);}
    private static void key(DataOutputStream out,Keys keys)throws IOException{out.writeInt(keys.keys().length);for(var component:keys.keys())blob(out,component.toByteArray());}
    private static Keys key(DataInputStream in)throws IOException{int count=count(in,DistributedSchemaCodec.MAX_KEY_COMPONENTS);var parts=new Buf[count];long bytes=0;for(int n=0;n<count;n++){var value=blob(in,2*1024*1024);bytes+=value.length;if(bytes>2*1024*1024)throw new IOException("Atomic key exceeds aggregate bound");parts[n]=Buf.wrap(value);}return new Keys(parts);}
    @Override public void close()throws IOException {
        synchronized(this){if(closed)return;if(active)throw new IOException("Atomic actual work active; ownership retained");}
        if(fenced){persist();fenced=false;}if(operation!=null)operation.close();if(lease!=null){if(job!=null && job.accepted!=null)lease.finishAcceptedOwnership();lease.close();}if(cleanup!=null)cleanup.close();atomic.close();transactions.close();
        lock.release();channel.close();if(requestCredit!=null)requestCredit.close();ownerCredit.close();synchronized(this){closed=true;}
    }
}
