package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Exact selected DATA targets plus a bounded configured HOME grant-authority union.
 * HOME authority does not add DATA participants; actual HOME routes are selected by allocated UUID.
 * Durable bounded allocator rotation. Every older issued identity root remains retained and is never recreated. */
final class RatisBackendAllocations implements RatisRocksDBAdapter.AllocationSource,AutoCloseable {
    private final RatisJournalAccess access;
    private final Path root;private final RatisSessionEpochs epochs;private final RWScheduler scheduler;private final long lifetime;private final int maximumGrants;
    private final Map<Integer,RatisGrantIssuer.Issued> issued=new HashMap<>();private final List<RatisBackendSessions.Target> homes;private final List<RatisRoutedClient> routes;private final Map<Integer,RatisTransactionIdentities> owners=new TreeMap<>();private final FileChannel file;private final FileLock lock;
    private int current;private boolean active,closed,fenced;
    RatisBackendAllocations(Path root,RatisSessionEpochs epochs,RWScheduler scheduler,long lifetime,int maximumGrants,List<RatisRoutedClient> routes,List<RatisBackendSessions.Target> homes,RatisJournalAccess access)throws IOException{
        this.access=Objects.requireNonNull(access);access.register(root.resolve("allocator.bin"),44);access.register(root.resolve("allocator.lock"),32);
        this.root=root.toAbsolutePath().normalize();this.epochs=epochs;this.scheduler=scheduler;this.lifetime=lifetime;this.maximumGrants=maximumGrants;this.homes=List.copyOf(homes);this.routes=List.copyOf(routes);Files.createDirectories(this.root);
        Path marker=this.root.resolve("allocator.lock"),state=this.root.resolve("allocator.bin");boolean used=Files.exists(marker,LinkOption.NOFOLLOW_LINKS);
        if(used && !Files.exists(state,LinkOption.NOFOLLOW_LINKS))throw new IOException("Used allocation rotation state is missing");
        file=FileChannel.open(marker,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try{acquired=file.tryLock();if(acquired==null)throw new IOException("Allocation rotation already owned");lock=acquired;
            if(used){byte[] bytes=access.read(state,44);if(bytes.length!=12)throw new IOException("Invalid allocation rotation state");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x42414c43 || in.getInt()!=maximumGrants)throw new IOException("Allocation quota changed");current=in.getInt();if(current<0 || current>=maximumGrants)throw new IOException("Allocation rotation exceeds quota");}
            else persist(0);
            for(int i=0;i<=current;i++)if(Files.exists(root.resolve("grant-"+i).resolve("issuer.bin"),LinkOption.NOFOLLOW_LINKS))open(i,List.of(),false);
        }catch(Throwable failure){for(var owner:owners.values())try{owner.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}if(acquired!=null)acquired.close();file.close();throw failure;}
    }
    private RatisTransactionIdentities open(int index,List<RatisBackendSessions.Target> requested,boolean bind)throws IOException{
        var existing=owners.get(index);Path slot=root.resolve("grant-"+index);
        var mandatory=new ArrayList<>(RatisBackendSessions.bindingReservations(slot,requested,epochs));
        mandatory.add(new RatisJournalAccess.Reservation(slot.resolve("targets"),TARGET_BYTES,bind?2L*(4+requested.stream().mapToLong(t->52L+RangeDirectoryCodec.encodedSize(t.directory())).sum()+32):0));
        mandatory.add(new RatisJournalAccess.Reservation(slot.resolve("identities/identities.bin"),96,192));mandatory.add(new RatisJournalAccess.Reservation(slot.resolve("identities/identities.lock"),32,0));access.preflight(mandatory);if(bind){Files.createDirectories(slot);var frozen=slot.resolve("targets");byte[] targetBytes=targetBytes(requested);if(Files.exists(frozen,LinkOption.NOFOLLOW_LINKS)){if(!Arrays.equals(targetBytes,access.read(frozen,TARGET_BYTES)))throw new IOException("Retained allocation target vector changed");SessionJournalFiles.force(frozen);SessionJournalFiles.force(slot);}else access.write(frozen,targetBytes,TARGET_BYTES);}
        if(!bind){var saved=slot.resolve("targets");if(!Files.isRegularFile(saved,LinkOption.NOFOLLOW_LINKS))throw new IOException("Original issued grant target vector absent");requested=restoreTargets(access.read(saved,TARGET_BYTES));}
        if(existing!=null){if(bind)RatisBackendSessions.bind(slot,epochs,issued.get(index),4096,requested,access);access.fulfilled(mandatory);return existing;}
        var issued=RatisBackendSessions.issueBound(slot,epochs,lifetime,requested,access);if(bind)RatisBackendSessions.bind(slot,epochs,issued,4096,requested,access);
        Path identities=slot.resolve("identities");Files.createDirectories(identities);boolean used=Files.exists(identities.resolve("identities.lock"),LinkOption.NOFOLLOW_LINKS);
        var owner=used?RatisTransactionIdentities.reopenOwned(identities,issued.grant(),4096,scheduler,access):RatisTransactionIdentities.createOwned(identities,issued.grant(),4096,scheduler,access);owners.put(index,owner);this.issued.put(index,issued);access.fulfilled(mandatory);return owner;
    }
    private void persist(int next)throws IOException{try{access.write(root.resolve("allocator.bin"),ByteBuffer.allocate(12).putInt(0x42414c43).putInt(maximumGrants).putInt(next).array(),44);}catch(IOException failure){fenced=true;throw failure;}current=next;}
    @Override public CompletableFuture<RatisRocksDBAdapter.Allocation> allocate(RequestContext context,List<RatisBackendSessions.Target> dataTargets){
        var selected=new LinkedHashMap<UUID,RatisBackendSessions.Target>();for(var target:dataTargets)if(selected.putIfAbsent(target.group(),target)!=null)throw new IllegalArgumentException("Duplicate selected DATA target");
        if(selected.size()>SnapshotFence.MAX_PARTICIPANTS)throw new IllegalArgumentException("Aggregate transaction participant bound exceeded");
        for(var target:homes){var previous=selected.putIfAbsent(target.group(),target);if(previous!=null && !Arrays.equals(previous.directory().payloadDigest(),target.directory().payloadDigest()))throw new IllegalArgumentException("Conflicting HOME and DATA target");}
        return allocateTargets(context,List.copyOf(selected.values()));
    }
    private CompletableFuture<RatisRocksDBAdapter.Allocation> allocateTargets(RequestContext context,List<RatisBackendSessions.Target> requested){
        requested=List.copyOf(requested);final var exactTargets=requested;
        synchronized(this){if(closed || fenced || active)return CompletableFuture.failedFuture(new IOException("Allocation rotation closed/fenced or actual work active"));active=true;}
        var actual=new CompletableFuture<RatisRocksDBAdapter.Allocation>();
        final class Work implements Runnable,RWScheduler.RejectionAwareTask{
            final java.util.concurrent.atomic.AtomicInteger phase=new java.util.concurrent.atomic.AtomicInteger();
            public void run(){if(!phase.compareAndSet(0,1))return;try{if(targetsChanged(current,exactTargets)){if(current==maximumGrants-1)throw new IOException("Retained allocation grant quota exhausted after target change");int next=current+1;open(next,exactTargets,true);persist(next);}var owner=open(current,exactTargets,true);var ledger=epochs.read();if(ledger==null)throw new IOException("Native allocation authority is unavailable");if(!SessionAuthority.from(ledger).permits(issued.get(current).grant()) || owner.nextSequence()>4096-RatisTransactionIdentities.BLOCK_SIZE){if(current==maximumGrants-1)throw new IOException("Retained allocation grant quota exhausted");int next=current+1;open(next,exactTargets,true);persist(next);owner=owners.get(next);}owner.allocate(context,exactTargets).whenComplete((value,error)->finish(value,error));}catch(Throwable failure){finish(null,failure);}}
            public void reject(RuntimeException failure){if(phase.compareAndSet(0,1))finish(null,failure);}
            void finish(RatisRocksDBAdapter.Allocation value,Throwable failure){if(!phase.compareAndSet(1,2))return;synchronized(RatisBackendAllocations.this){active=false;}if(failure==null)actual.complete(value);else actual.completeExceptionally(failure);}
        }
        var work=new Work();try{scheduler.executor(context,OperationFamily.METADATA).execute(work,65536);}catch(RuntimeException rejected){work.reject(rejected);}return actual.copy();
    }
    CompletableFuture<RatisAggregateExportDriver.Allocation> allocateIssued(List<RatisBackendSessions.Target> requested,RequestContext context){return allocateTargets(context,requested).thenApply(allocation->{synchronized(this){var identity=allocation.identity().snapshot().business();var grant=issued.values().stream().filter(value->value.grant().epoch()==identity.epoch() && value.grant().sessionId().equals(identity.sessionId())).findFirst().orElseThrow(()->new IllegalStateException("Issued allocation grant unavailable"));return new RatisAggregateExportDriver.Allocation(allocation.snapshot(),allocation.identity().snapshot(),grant);}});}
    private List<RatisBackendSessions.Target> restoreTargets(byte[] encoded)throws IOException{
        try{var in=ByteBuffer.wrap(encoded);int count=in.getInt();if(count<1 || count>RatisSessionBindingPlan.MAX_TARGETS)throw new IOException("Invalid allocation target count");var restored=new ArrayList<RatisBackendSessions.Target>();var groups=new HashSet<UUID>();
            for(int n=0;n<count;n++){var group=new UUID(in.getLong(),in.getLong());byte[] identity=new byte[32];in.get(identity);int size=in.getInt();if(size<1 || size>in.remaining() || size>RangeDirectoryCodec.MAX_ENCODED_BYTES)throw new IOException("Invalid target directory size");byte[] body=new byte[size];in.get(body);var directory=RangeDirectoryCodec.decode(body);var matches=routes.stream().filter(route->route.bootstrapDirectory().domainId().equals(directory.domainId()) && Arrays.equals(route.sessionIdentityDigest(),identity)).toList();if(matches.size()!=1 || !groups.add(group))throw new IOException("Original allocation namespace route unavailable or ambiguous");restored.add(new RatisBackendSessions.Target(matches.getFirst(),group,directory));}
            if(in.hasRemaining())throw new IOException("Trailing allocation target vector");return List.copyOf(restored);
        }catch(IllegalArgumentException|java.nio.BufferUnderflowException malformed){throw new IOException("Invalid original allocation targets",malformed);}
    }
    private static final int TARGET_BYTES=64*1024*1024;
    private static byte[] targetBytes(List<RatisBackendSessions.Target> targets)throws IOException{var buffer=new ByteArrayOutputStream();var out=new DataOutputStream(buffer);var sorted=targets.stream().sorted(Comparator.comparing(RatisBackendSessions.Target::group,RangeDirectoryCodec.UUID_ORDER)).toList();var groups=new HashSet<UUID>();out.writeInt(sorted.size());for(var target:sorted){if(!groups.add(target.group()))throw new IOException("Repeated allocation target");byte[] directory=RangeDirectoryCodec.encode(target.directory());if((long)buffer.size()+52+directory.length>TARGET_BYTES-32)throw new IOException("Allocation target vector exceeds bound before copy");out.writeLong(target.group().getMostSignificantBits());out.writeLong(target.group().getLeastSignificantBits());out.write(target.routed().sessionIdentityDigest());out.writeInt(directory.length);out.write(directory);}out.flush();return buffer.toByteArray();}
    private boolean targetsChanged(int index,List<RatisBackendSessions.Target> requested)throws IOException{Path saved=root.resolve("grant-"+index).resolve("targets");return Files.exists(saved,LinkOption.NOFOLLOW_LINKS)&&!Arrays.equals(access.read(saved,TARGET_BYTES),targetBytes(requested));}
    @Override public synchronized void close()throws IOException{if(closed)return;if(active)throw new IOException("Actual allocation remains owned");for(var owner:owners.values())owner.close();closed=true;try{lock.close();}finally{file.close();}}
}
