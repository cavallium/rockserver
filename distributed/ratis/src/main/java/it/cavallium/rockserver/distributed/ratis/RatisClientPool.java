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
    private record Route(UUID group, long epoch, long configuration, List<ReplicaDescriptor> replicas,ReplicaDescriptor owner,UUID clientId) {
        Route(RangeDescriptor descriptor,ReplicaDescriptor owner,UUID clientId) {
            this(descriptor.groupId(), descriptor.rangeEpoch(), descriptor.configurationVersion(), descriptor.replicas(),owner,clientId);
        }
    }
    private static final class Entry {
        final CompletableFuture<RaftClient> ready = new CompletableFuture<>();
        int users;
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
        @Override public void close() {
            if (released.compareAndSet(false, true)) release(entry);
        }
    }

    private final int capacity;
    private final Factory factory;
    private final OwnerFactory ownerFactory;
    private final IdentityFactory identityFactory;
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
        if (capacity < 1) throw new IllegalArgumentException("Invalid client capacity");
        this.capacity = capacity;
        this.factory = java.util.Objects.requireNonNull(factory);
        this.ownerFactory=ownerFactory;
        this.identityFactory=identityFactory;
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
    private Lease acquire(RangeDescriptor descriptor,ReplicaDescriptor owner,UUID clientId) throws IOException {
        Route route = new Route(descriptor,owner,clientId);
        Entry entry;
        RaftClient retired = null;
        boolean create = false;
        synchronized (this) {
            if (closing) throw new IOException("Client pool is closing");
            if (broken != null) throw new IOException("Client pool resource retirement failed", broken);
            entry = clients.get(route);
            if (entry == null) {
                if (clients.size() == capacity) {
                    var iterator = clients.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Entry candidate = iterator.next().getValue();
                        if (candidate.users == 0 && candidate.ready.isDone()) {
                            retired = candidate.ready.join();
                            iterator.remove();
                            break;
                        }
                    }
                    if (retired == null) throw new IOException("All cached range clients are in use");
                }
                entry = new Entry();
                clients.put(route, entry);
                create = true;
            }
            entry.users++;
        }
        if (create) {
            try {
                // The replacement slot is reserved, but no replacement resource opens before retirement.
                if (retired != null) {
                    try { closeClient(retired); }
                    catch (IOException failure) {
                        synchronized (this) { broken = failure; failedRetirements.add(retired); }
                        throw failure;
                    }
                }
                entry.ready.complete(java.util.Objects.requireNonNull(clientId!=null ? identityFactory.open(descriptor,clientId) : owner==null ? factory.open(descriptor) : ownerFactory.open(descriptor,owner)));
            } catch (IOException | RuntimeException | Error failure) {
                synchronized (this) { clients.remove(route, entry); }
                entry.ready.completeExceptionally(failure);
            }
        }
        try { return new Lease(entry, entry.ready.join()); }
        catch (CompletionException failure) {
            release(entry);
            if (failure.getCause() instanceof IOException io) throw io;
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failure.getCause() instanceof Error error) throw error;
            throw new IOException("Range client creation failed", failure.getCause());
        }
    }

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
