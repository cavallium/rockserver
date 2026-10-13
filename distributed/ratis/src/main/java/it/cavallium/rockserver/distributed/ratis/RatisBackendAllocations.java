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

/** Durable bounded allocator rotation. Every older issued identity root remains retained and is never recreated. */
final class RatisBackendAllocations implements RatisRocksDBAdapter.AllocationSource,AutoCloseable {
    private final Path root;private final RatisSessionEpochs epochs;private final RWScheduler scheduler;private final long lifetime;private final int maximumGrants;
    private final Map<Integer,RatisGrantIssuer.Issued> issued=new HashMap<>();private final List<RatisBackendSessions.Target> targets;private final Map<Integer,RatisTransactionIdentities> owners=new TreeMap<>();private final FileChannel file;private final FileLock lock;
    private int current;private boolean active,closed,fenced;
    RatisBackendAllocations(Path root,RatisSessionEpochs epochs,RWScheduler scheduler,long lifetime,int maximumGrants,List<RatisBackendSessions.Target> targets)throws IOException{
        this.root=root.toAbsolutePath().normalize();this.epochs=epochs;this.scheduler=scheduler;this.lifetime=lifetime;this.maximumGrants=maximumGrants;this.targets=List.copyOf(targets);Files.createDirectories(this.root);
        Path marker=this.root.resolve("allocator.lock"),state=this.root.resolve("allocator.bin");boolean used=Files.exists(marker,LinkOption.NOFOLLOW_LINKS);
        if(used && !Files.exists(state,LinkOption.NOFOLLOW_LINKS))throw new IOException("Used allocation rotation state is missing");
        file=FileChannel.open(marker,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try{acquired=file.tryLock();if(acquired==null)throw new IOException("Allocation rotation already owned");lock=acquired;
            if(used){byte[] bytes=SessionJournalFiles.read(state,44);if(bytes.length!=12)throw new IOException("Invalid allocation rotation state");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x42414c43 || in.getInt()!=maximumGrants)throw new IOException("Allocation quota changed");current=in.getInt();if(current<0 || current>=maximumGrants)throw new IOException("Allocation rotation exceeds quota");}
            else persist(0);
            for(int i=0;i<=current;i++)open(i);
        }catch(Throwable failure){for(var owner:owners.values())try{owner.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}if(acquired!=null)acquired.close();file.close();throw failure;}
    }
    private RatisTransactionIdentities open(int index)throws IOException{
        var existing=owners.get(index);if(existing!=null)return existing;Path slot=root.resolve("grant-"+index);Files.createDirectories(slot);
        var issued=RatisBackendSessions.issue(slot,epochs,lifetime);RatisBackendSessions.bind(slot,epochs,issued,4096,targets);
        Path identities=slot.resolve("identities");Files.createDirectories(identities);boolean used=Files.exists(identities.resolve("identities.lock"),LinkOption.NOFOLLOW_LINKS);
        var owner=used?RatisTransactionIdentities.reopen(identities,issued.grant(),4096,scheduler):RatisTransactionIdentities.create(identities,issued.grant(),4096,scheduler);owners.put(index,owner);this.issued.put(index,issued);return owner;
    }
    private void persist(int next)throws IOException{try{SessionJournalFiles.write(root.resolve("allocator.bin"),ByteBuffer.allocate(12).putInt(0x42414c43).putInt(maximumGrants).putInt(next).array(),44);}catch(IOException failure){fenced=true;throw failure;}current=next;}
    @Override public CompletableFuture<RatisRocksDBAdapter.Allocation> allocate(RequestContext context){
        synchronized(this){if(closed || fenced || active)return CompletableFuture.failedFuture(new IOException("Allocation rotation closed/fenced or actual work active"));active=true;}
        var actual=new CompletableFuture<RatisRocksDBAdapter.Allocation>();
        final class Work implements Runnable,RWScheduler.RejectionAwareTask{
            final java.util.concurrent.atomic.AtomicInteger phase=new java.util.concurrent.atomic.AtomicInteger();
            public void run(){if(!phase.compareAndSet(0,1))return;try{var owner=open(current);var ledger=epochs.read();if(ledger==null)throw new IOException("Native allocation authority is unavailable");if(!SessionAuthority.from(ledger).permits(issued.get(current).grant()) || owner.nextSequence()>4096-RatisTransactionIdentities.BLOCK_SIZE){if(current==maximumGrants-1)throw new IOException("Retained allocation grant quota exhausted");int next=current+1;open(next);persist(next);owner=owners.get(next);}owner.allocate(context).whenComplete((value,error)->finish(value,error));}catch(Throwable failure){finish(null,failure);}}
            public void reject(RuntimeException failure){if(phase.compareAndSet(0,1))finish(null,failure);}
            void finish(RatisRocksDBAdapter.Allocation value,Throwable failure){if(!phase.compareAndSet(1,2))return;synchronized(RatisBackendAllocations.this){active=false;}if(failure==null)actual.complete(value);else actual.completeExceptionally(failure);}
        }
        var work=new Work();try{scheduler.executor(context,OperationFamily.METADATA).execute(work,65536);}catch(RuntimeException rejected){work.reject(rejected);}return actual.copy();
    }
    @Override public synchronized void close()throws IOException{if(closed)return;if(active)throw new IOException("Actual allocation remains owned");for(var owner:owners.values())owner.close();closed=true;try{lock.close();}finally{file.close();}}
}
