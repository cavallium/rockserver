package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.RangeDescriptor;
import it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor;
import org.apache.ratis.client.RaftClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/** Bounded clients with single creation per route, idle eviction and explicit borrowed ownership. */
final class RatisClientPool implements AutoCloseable {
    @FunctionalInterface interface Factory { RaftClient open(RangeDescriptor descriptor) throws IOException; }
    @FunctionalInterface interface OwnerFactory { RaftClient open(RangeDescriptor descriptor,ReplicaDescriptor owner) throws IOException; }
    @FunctionalInterface interface IdentityFactory { RaftClient open(RangeDescriptor descriptor,UUID clientId) throws IOException; }
    private record Route(UUID group, long epoch, long configuration, List<ReplicaDescriptor> replicas,ReplicaDescriptor owner,UUID clientId,boolean snapshot) {
        Route(RangeDescriptor descriptor,ReplicaDescriptor owner,UUID clientId,boolean snapshot) {
            this(descriptor.groupId(), descriptor.rangeEpoch(), descriptor.configurationVersion(), descriptor.replicas(),owner,clientId,snapshot);
        }
    }
    private static final class Entry {
        final CompletableFuture<RaftClient> ready = new CompletableFuture<>();
        int users;boolean invalid;
    }
    final class Lease implements AutoCloseable {
        private final Entry entry;
        private final RaftClient client;
        private final AtomicBoolean released = new AtomicBoolean();
        private Lease(Entry entry, RaftClient client) { this.entry = entry; this.client = client; }
        RaftClient client() {
            if (released.get()) throw new IllegalStateException("Client lease is closed");
            return client;
        }
        void invalidate(){synchronized(RatisClientPool.this){entry.invalid=true;}}
        @Override public void close() {
            if (released.compareAndSet(false, true)) release(entry);
        }
    }

    private final int capacity;
    private final Factory factory;
    private final OwnerFactory ownerFactory;
    private final IdentityFactory identityFactory;private final Factory snapshotFactory;private final OwnerFactory snapshotOwnerFactory;
    private record Retirement(RaftClient client,CompletableFuture<Void> done){}
    private final java.util.ArrayDeque<Retirement> retirementQueue=new java.util.ArrayDeque<>();private boolean retirementWorker;
    private final LinkedHashMap<Route, Entry> clients = new LinkedHashMap<>(16, 0.75f, true);
    private final List<RaftClient> failedRetirements = new ArrayList<>();
    private final ReentrantLock shutdown = new ReentrantLock();
    private boolean closing;
    private IOException broken;

    RatisClientPool(int capacity, Factory factory) { this(capacity,factory,null); }
    RatisClientPool(int capacity, Factory factory,OwnerFactory ownerFactory) {
        this(capacity,factory,ownerFactory,null);
    }
    RatisClientPool(int capacity, Factory factory,OwnerFactory ownerFactory,IdentityFactory identityFactory) {
        this(capacity,factory,ownerFactory,identityFactory,null,null);
    }
    RatisClientPool(int capacity,Factory factory,OwnerFactory ownerFactory,IdentityFactory identityFactory,Factory snapshotFactory,OwnerFactory snapshotOwnerFactory) {
        if (capacity < 1) throw new IllegalArgumentException("Invalid client capacity");
        this.capacity = capacity;
        this.factory = java.util.Objects.requireNonNull(factory);
        this.ownerFactory=ownerFactory;
        this.identityFactory=identityFactory;this.snapshotFactory=snapshotFactory;this.snapshotOwnerFactory=snapshotOwnerFactory;
    }

    Lease acquire(RangeDescriptor descriptor) throws IOException { return acquire(descriptor,null,null); }
    Lease acquireClient(RangeDescriptor descriptor,UUID clientId) throws IOException {
        if(identityFactory==null)throw new IOException("Stable-identity clients unsupported");
        return acquire(descriptor,null,java.util.Objects.requireNonNull(clientId));
    }
    /** The exact frozen member is targeted without ordinary leader discovery, using the same bounded cache. */
    Lease acquireOwner(RangeDescriptor descriptor,ReplicaDescriptor owner) throws IOException {
        java.util.Objects.requireNonNull(descriptor); java.util.Objects.requireNonNull(owner);
        if (!descriptor.replicas().contains(owner)) throw new IllegalArgumentException("Snapshot owner differs from frozen range membership");
        if (ownerFactory==null) throw new IOException("Owner-specific clients are unsupported by this pool");
        return acquire(descriptor,owner,null);
    }
    private record Reservation(Route route,Entry entry,RaftClient retired,boolean create){}
    private Reservation reserve(RangeDescriptor descriptor,ReplicaDescriptor owner,UUID id,boolean snapshot)throws IOException {
        var route=new Route(descriptor,owner,id,snapshot);Entry entry;RaftClient retired=null;boolean create=false;
        synchronized(this){if(closing)throw new IOException("Client pool is closing");if(broken!=null)throw new IOException("Client pool resource retirement failed",broken);entry=clients.get(route);
            if(entry!=null&&entry.invalid){if(entry.users!=0)throw new IOException("Failed client window is still leased");retired=entry.ready.getNow(null);clients.remove(route);entry=null;}
            if(entry==null){if(clients.size()==capacity){var iterator=clients.entrySet().iterator();while(iterator.hasNext()){var candidate=iterator.next().getValue();if(candidate.users==0&&candidate.ready.isDone()){retired=candidate.ready.getNow(null);iterator.remove();break;}}if(retired==null)throw new IOException("All cached range clients are in use");}entry=new Entry();clients.put(route,entry);create=true;}entry.users++;}
        return new Reservation(route,entry,retired,create);
    }
    private void failed(Reservation reservation,Throwable failure){synchronized(this){clients.remove(reservation.route(),reservation.entry());}reservation.entry().ready.completeExceptionally(failure);}
    private void opened(Reservation reservation,RangeDescriptor descriptor,ReplicaDescriptor owner,UUID id,boolean snapshot){
        try{var client=id!=null?identityFactory.open(descriptor,id):snapshot?(owner==null?snapshotFactory.open(descriptor):snapshotOwnerFactory.open(descriptor,owner)):(owner==null?factory.open(descriptor):ownerFactory.open(descriptor,owner));reservation.entry().ready.complete(java.util.Objects.requireNonNull(client));}catch(Throwable failure){failed(reservation,failure);}
    }
    private CompletableFuture<Lease> lease(Entry entry){var result=new CompletableFuture<Lease>();entry.ready.whenComplete((client,failure)->{if(failure!=null){release(entry);result.completeExceptionally(failure);}else{var lease=new Lease(entry,client);if(!result.complete(lease))lease.close();}});return result;}
    private Lease acquire(RangeDescriptor descriptor,ReplicaDescriptor owner,UUID id)throws IOException {
        var reserved=reserve(descriptor,owner,id,false);var result=lease(reserved.entry());openAfterRetirement(reserved,descriptor,owner,id,false);
        try{return RatisRoutedClient.awaitSnapshot(result);}catch(java.io.InterruptedIOException failure){result.cancel(false);result.thenAccept(Lease::close);throw failure;}
    }
    /** Snapshot factories only construct local client objects; leader discovery happens through the asynchronous RPC. */
    CompletableFuture<Lease> acquireSnapshotAsync(RangeDescriptor descriptor,ReplicaDescriptor owner) {
        try{if(snapshotFactory==null || owner!=null&&snapshotOwnerFactory==null)throw new IOException("Snapshot clients unsupported");if(owner!=null&&!descriptor.replicas().contains(owner))throw new IllegalArgumentException("Snapshot owner differs from frozen range membership");var reserved=reserve(descriptor,owner,null,true);var result=lease(reserved.entry());openAfterRetirement(reserved,descriptor,owner,null,true);return result;}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    /** Creation is an actual owned continuation; interrupting/cancelling a waiter never removes its reservation. */
    private void openAfterRetirement(Reservation reserved,RangeDescriptor descriptor,ReplicaDescriptor owner,UUID id,boolean snapshot){if(!reserved.create())return;if(reserved.retired()==null)opened(reserved,descriptor,owner,id,snapshot);else retire(reserved.retired()).whenComplete((ignored,failure)->{if(failure!=null)failed(reserved,failure);else opened(reserved,descriptor,owner,id,snapshot);});}
    private CompletableFuture<Void> retire(RaftClient client){var done=new CompletableFuture<Void>();boolean start;synchronized(this){retirementQueue.addLast(new Retirement(client,done));start=!retirementWorker;if(start)retirementWorker=true;}if(start)Thread.ofVirtual().name("ratis-client-retirement").start(this::retireQueued);return done;}
    private void retireQueued(){while(true){Retirement retired;IOException previous;synchronized(this){retired=retirementQueue.pollFirst();if(retired==null){retirementWorker=false;notifyAll();return;}previous=broken;}
        if(previous!=null){synchronized(this){failedRetirements.add(retired.client());}retired.done().completeExceptionally(previous);continue;}
        try{closeClient(retired.client());retired.done().complete(null);}catch(IOException failure){synchronized(this){broken=failure;failedRetirements.add(retired.client());}retired.done().completeExceptionally(failure);}
    }}

    private synchronized void release(Entry entry) { entry.users--; }
    private static void closeClient(RaftClient client) throws IOException {
        try { RatisRoutedClient.rpc(() -> { client.close(); return null; }); }
        catch (RuntimeException failure) { throw new IOException("Failed to release range client", failure); }
    }

    /** Caller drains calls first; a mistaken early close stops admission and retains leased resources. */
    @Override public void close() throws IOException {
        shutdown.lock();
        try {
            List<RaftClient> resources;
            synchronized (this) {
                closing = true;
                if(retirementWorker || !retirementQueue.isEmpty())throw new IOException("Actual client retirement remains owned");
                if (clients.values().stream().anyMatch(entry -> entry.users != 0)) {
                    throw new IOException("Range clients are still leased");
                }
                resources = new ArrayList<>(failedRetirements);
                clients.values().forEach(entry -> resources.add(entry.ready.join()));
                clients.clear();
                failedRetirements.clear();
            }
            IOException failure = null;
            for (RaftClient client : resources) {
                try { closeClient(client); }
                catch (IOException next) {
                    synchronized (this) { failedRetirements.add(client); }
                    if (failure == null) failure = next; else if (failure != next) failure.addSuppressed(next);
                }
            }
            if (failure != null) throw failure;
        } finally { shutdown.unlock(); }
    }
}
