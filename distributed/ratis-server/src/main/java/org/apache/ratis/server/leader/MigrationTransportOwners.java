package org.apache.ratis.server.leader;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Owns payload credits through both local onNext return and remote ACK/terminal transport callback. */
public final class MigrationTransportOwners {
    private static final class Owner {final MigrationLogStateMachine.Lease lease;boolean sending=true,finished;Owner(MigrationLogStateMachine.Lease lease){this.lease=lease;}}
    private final Object lock;private final Runnable becameEmpty,becameOwned;
    private final Map<Long,Owner> owners=new HashMap<>();private boolean terminal;private final CompletableFuture<Void> drained=new CompletableFuture<>();
    /** Callbacks mutate only the registry under this same lock; they must never perform IO or publish futures. */
    public MigrationTransportOwners(Object lock,Runnable becameEmpty,Runnable becameOwned){this.lock=Objects.requireNonNull(lock);this.becameEmpty=Objects.requireNonNull(becameEmpty);this.becameOwned=Objects.requireNonNull(becameOwned);}
    /** False means this transport never accepted the frame; any supplied lease is retired here. */
    public boolean begin(long id,MigrationLogStateMachine.Lease lease){synchronized(lock){if(terminal){if(lease!=null)lease.close();return false;}if(lease==null)return true;if(owners.putIfAbsent(id,new Owner(lease))!=null){lease.close();throw new IllegalStateException("Duplicate migration transport call ID");}becameOwned.run();return true;}}
    public void sent(long id){boolean done;synchronized(lock){var owner=owners.get(id);if(owner!=null){owner.sending=false;release(id,owner);}done=isDrained();}if(done)drained.complete(null);}
    public void acknowledged(long id){boolean done;synchronized(lock){var owner=owners.get(id);if(owner!=null){owner.finished=true;release(id,owner);}done=isDrained();}if(done)drained.complete(null);}
    /** A timeout or an appender stop request is not a transport terminal callback. */
    public void terminal(){boolean done;synchronized(lock){terminal=true;for(var id:List.copyOf(owners.keySet())){var owner=owners.get(id);owner.finished=true;release(id,owner);}done=isDrained();}if(done)drained.complete(null);}
    private void release(long id,Owner owner){if(owner.finished && !owner.sending){owners.remove(id);owner.lease.close();if(owners.isEmpty())becameEmpty.run();}}
    private boolean isDrained(){return terminal && owners.isEmpty();}
    public CompletableFuture<Void> drained(){return drained.copy();}
    public int size(){synchronized(lock){return owners.size();}}
}
