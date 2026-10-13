package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.impl.CommittedApplyStateMachine;

/** Dedicated committed-head ownership. Waiters contain scalars only; no proposal credit is reused. */
public final class RatisCommittedApplyDispatcher implements AutoCloseable {
    public record Lane(long bytes,int owners) {
        public Lane {if(bytes<1 || bytes>RatisNodeResourceBudget.MAX_BYTES || owners<1 || owners>RatisNodeResourceBudget.MAX_OWNERS)throw new IllegalArgumentException("Invalid committed apply lane");}
    }
    public record Limits(Lane read,Lane ordinary,Lane protectedLane,int groups) {
        public Limits {Objects.requireNonNull(read);Objects.requireNonNull(ordinary);Objects.requireNonNull(protectedLane);if(groups<1 || groups>RatisNodeResourceBudget.MAX_OWNERS)throw new IllegalArgumentException("Invalid committed group bound");}
    }
    public record Policy(Limits node,Limits pool) {
        public Policy {Objects.requireNonNull(node);Objects.requireNonNull(pool);}
        public static Policy defaults(){
            long maximumEntry=it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES+4096;
            long read=RatisRangeStateMachine.committedReadWeight(maximumEntry),execution=RatisRangeStateMachine.committedExecutionWeight(maximumEntry);
            // Two maximum-size heads node-wide, one per pool; smaller heads share the bounded owner slots.
            return new Policy(new Limits(new Lane(Math.multiplyExact(2,read),2),new Lane(Math.multiplyExact(2,execution),4),new Lane(Math.multiplyExact(2,execution),16),RatisNodePlan.MAX_LOCAL_GROUPS),
                    new Limits(new Lane(read,1),new Lane(execution,2),new Lane(execution,8),RatisNodePlan.MAX_LOCAL_GROUPS));
        }
    }
    public record Usage(long readBytes,int reads,long ordinaryBytes,int ordinary,long protectedBytes,int protectedOwners,int waiters) {}
    private static final class Counter {long bytes;int owners;}
    private static final class Pool {final Limits limits;final Counter[] lanes={new Counter(),new Counter(),new Counter()};int groups;Pool(Limits limits){this.limits=limits;}}
    private record Waiter(Group group,int lane,long bytes) {}
    private final Limits limits;private final Map<UUID,Pool> pools=new LinkedHashMap<>();private final Map<UUID,Group> groups=new HashMap<>();
    private final Counter[] lanes={new Counter(),new Counter(),new Counter()};private final LinkedHashSet<Waiter> waiters=new LinkedHashSet<>();private boolean stopped;
    public RatisCommittedApplyDispatcher(Limits node,Map<UUID,Limits> declaredPools){limits=Objects.requireNonNull(node);if(declaredPools.isEmpty() || declaredPools.size()>RatisNodeResourceBudget.MAX_POOLS)throw new IllegalArgumentException("Committed pools must be declared");declaredPools.forEach((id,value)->pools.put(Objects.requireNonNull(id),new Pool(Objects.requireNonNull(value))));}
    public synchronized Group register(UUID groupId,UUID poolId,long readBytes,long maximumExecutionBytes,long maximumSegmentBytes,Function<LogEntryProto,CommittedApplyStateMachine.Descriptor> describe){
        var pool=pools.get(poolId);if(pool==null || stopped || groups.containsKey(groupId) || groups.size()>=limits.groups || pool.groups>=pool.limits.groups)throw new IllegalArgumentException("Invalid committed group registration");
        if(readBytes<1 || maximumExecutionBytes<1 || maximumSegmentBytes<1 || readBytes>Math.min(limits.read.bytes,pool.limits.read.bytes) || maximumExecutionBytes>Math.min(Math.min(limits.ordinary.bytes,pool.limits.ordinary.bytes),Math.min(limits.protectedLane.bytes,pool.limits.protectedLane.bytes)))throw new IllegalArgumentException("Committed policy cannot admit the largest declared head");
        var group=new Group(groupId,pool,readBytes,maximumExecutionBytes,maximumSegmentBytes,Objects.requireNonNull(describe));groups.put(groupId,group);pool.groups++;return group;
    }
    private static Lane lane(Limits value,int lane){return switch(lane){case 0->value.read;case 1->value.ordinary;default->value.protectedLane;};}
    private boolean fits(Waiter w){var local=w.group.pool.lanes[w.lane];var global=lanes[w.lane];var a=lane(limits,w.lane);var b=lane(w.group.pool.limits,w.lane);return !w.group.active && (w.lane!=0 || !w.group.reading) && w.bytes<=a.bytes-global.bytes && w.bytes<=b.bytes-local.bytes && global.owners<a.owners && local.owners<b.owners;}
    private boolean turn(Waiter w){for(var earlier:waiters){if(earlier.equals(w))return true;if(earlier.lane==w.lane && fits(earlier))return false;}return true;}
    private Token reserve(Group group,int lane,long bytes,boolean wait,long generation)throws IOException,InterruptedException {
        synchronized(this){var w=new Waiter(group,lane,bytes);if(wait){if(group.waiter!=null)throw new IllegalStateException("Only one scalar waiter per group");group.waiter=w;waiters.add(w);}try{for(;;){if(stopped || group.stopped)throw new CommittedApplyStateMachine.Stopped();if(group.paused || generation!=group.generation)throw new CommittedApplyStateMachine.Retry();if(fits(w) && turn(w)){lanes[lane].bytes+=bytes;lanes[lane].owners++;group.pool.lanes[lane].bytes+=bytes;group.pool.lanes[lane].owners++;if(lane==0){group.reading=true;group.readGeneration=generation;}else group.active=true;return new Token(group,lane,bytes,generation);}if(!wait)return null;wait();}}finally{if(wait){waiters.remove(w);group.waiter=null;notifyAll();}}}
    }
    public final class Group implements CommittedApplyStateMachine.Admission {
        private final UUID id;private final Pool pool;private final long readBytes,maximumExecutionBytes,maximumSegmentBytes;private final Function<LogEntryProto,CommittedApplyStateMachine.Descriptor> describe;
        private long generation,readGeneration;private Thread updater;private Runnable pendingAction,pendingCancel;private Runnable wake=()->{};
        private boolean active,reading,stopped,paused;private Waiter waiter;private CommittedApplyStateMachine.Descriptor offered;private Token token;
        private Group(UUID id,Pool pool,long readBytes,long maximumExecutionBytes,long maximumSegmentBytes,Function<LogEntryProto,CommittedApplyStateMachine.Descriptor> describe){this.id=id;this.pool=pool;this.readBytes=readBytes;this.maximumExecutionBytes=maximumExecutionBytes;this.maximumSegmentBytes=maximumSegmentBytes;this.describe=describe;}
        @Override public CommittedApplyStateMachine.ReadPermit awaitRead(long index)throws IOException,InterruptedException{
            final long expected;
            synchronized(RatisCommittedApplyDispatcher.this){expected=generation;if(updater!=null && updater!=Thread.currentThread())throw new IllegalStateException("Group updater changed");updater=Thread.currentThread();}
            for(;;){Runnable action;
                synchronized(RatisCommittedApplyDispatcher.this){if(stopped || RatisCommittedApplyDispatcher.this.stopped)throw new CommittedApplyStateMachine.Stopped();if(expected!=generation)throw new CommittedApplyStateMachine.Retry();if(paused){RatisCommittedApplyDispatcher.this.wait();continue;}action=pendingAction;pendingAction=null;pendingCancel=null;if(action==null && active){RatisCommittedApplyDispatcher.this.wait();continue;}}
                if(action!=null){action.run();continue;}
                return reserve(this,0,readBytes,true,expected);
            }
        }
        /** Every asynchronous leg is staged before returning its observer; only the updater pumps JNI. */
        public CompletableFuture<Message> executeNative(Callable<Message> action){
            var result=new CompletableFuture<Message>();Runnable run=()->{try{result.complete(action.call());}catch(Throwable failed){result.completeExceptionally(failed);}};final Runnable signal;
            synchronized(RatisCommittedApplyDispatcher.this){if(stopped || paused || RatisCommittedApplyDispatcher.this.stopped)return CompletableFuture.completedFuture(Message.EMPTY);if(pendingAction!=null)throw new IllegalStateException("Duplicate native continuation");pendingAction=run;pendingCancel=()->result.complete(Message.EMPTY);signal=wake;RatisCommittedApplyDispatcher.this.notifyAll();}
            signal.run();return result;
        }
        @Override public void bindWakeup(Runnable wakeup){synchronized(RatisCommittedApplyDispatcher.this){wake=Objects.requireNonNull(wakeup);}}
        @Override public void pump(){pumpNativeAction();}
        boolean pumpNativeAction(){Runnable action;synchronized(RatisCommittedApplyDispatcher.this){if(updater==null)updater=Thread.currentThread();if(Thread.currentThread()!=updater)throw new IllegalStateException("Only the original updater pumps native work");action=pendingAction;pendingAction=null;pendingCancel=null;}if(action!=null)action.run();return action!=null;}
        @Override public CommittedApplyStateMachine.Descriptor describe(LogEntryProto entry)throws IOException {var value=describe.apply(entry);if(value.bytes()>maximumExecutionBytes)throw new IOException("Committed entry exceeds frozen supported bound");long original;synchronized(RatisCommittedApplyDispatcher.this){original=readGeneration;}return new CommittedApplyStateMachine.Descriptor(value.term(),value.index(),value.bodyCase(),value.lane(),value.bytes(),value.digest(),original);}
        @Override public CommittedApplyStateMachine.ApplyPermit tryDispatch(CommittedApplyStateMachine.Descriptor descriptor)throws IOException {try{return reserve(this,descriptor.lane()==CommittedApplyStateMachine.Lane.ORDINARY?1:2,descriptor.bytes(),false,descriptor.generation());}catch(InterruptedException impossible){throw new AssertionError(impossible);}}
        @Override public CommittedApplyStateMachine.ApplyPermit awaitDispatch(CommittedApplyStateMachine.Descriptor descriptor)throws IOException,InterruptedException{return reserve(this,descriptor.lane()==CommittedApplyStateMachine.Lane.ORDINARY?1:2,descriptor.bytes(),true,descriptor.generation());}
        @Override public void validate(CommittedApplyStateMachine.Descriptor descriptor)throws IOException{synchronized(RatisCommittedApplyDispatcher.this){if(stopped || RatisCommittedApplyDispatcher.this.stopped)throw new CommittedApplyStateMachine.Stopped();if(paused || descriptor.generation()!=generation)throw new CommittedApplyStateMachine.Retry();}}
        @Override public void offer(CommittedApplyStateMachine.Descriptor descriptor,CommittedApplyStateMachine.ApplyPermit permit)throws IOException {synchronized(RatisCommittedApplyDispatcher.this){validate(descriptor);if(token!=null || !(permit instanceof RatisCommittedApplyDispatcher.Token t) || t.group!=this || t.closed || t.generation!=descriptor.generation())throw new IOException("Conflicting committed handoff");offered=descriptor;token=t;}}
        public CommittedApplyStateMachine.ApplyPermit claim(long term,long index,boolean running)throws IOException{synchronized(RatisCommittedApplyDispatcher.this){if(token==null)return null;if(!running){paused=true;generation++;RatisCommittedApplyDispatcher.this.notifyAll();throw new CommittedApplyStateMachine.Retry();}validate(offered);if(offered.term()!=term || offered.index()!=index)throw new IllegalStateException("Committed handoff identity changed");token.claim();return token;}}
        @Override public void withdraw(CommittedApplyStateMachine.Descriptor descriptor,CommittedApplyStateMachine.ApplyPermit permit){synchronized(RatisCommittedApplyDispatcher.this){if(token==permit){token=null;offered=null;}}}
        public void resume(){synchronized(RatisCommittedApplyDispatcher.this){paused=false;RatisCommittedApplyDispatcher.this.notifyAll();}}
        @Override public void wakeup(){Runnable cancel;synchronized(RatisCommittedApplyDispatcher.this){generation++;paused=true;pendingAction=null;cancel=pendingCancel;pendingCancel=null;RatisCommittedApplyDispatcher.this.notifyAll();}if(cancel!=null)cancel.run();}
        @Override public void stop(){Runnable cancel;synchronized(RatisCommittedApplyDispatcher.this){stopped=true;pendingAction=null;cancel=pendingCancel;pendingCancel=null;RatisCommittedApplyDispatcher.this.notifyAll();}if(cancel!=null)cancel.run();}
        @Override public void validateSegmentBytes(long bytes){if(bytes<1 || bytes>maximumSegmentBytes)throw new IllegalArgumentException("Raft segment read policy exceeds committed staging declaration");}
    }
    private final class Token implements CommittedApplyStateMachine.ReadPermit,CommittedApplyStateMachine.ApplyPermit {
        final Group group;final int lane;final long bytes,generation;boolean claimed,closed;
        Token(Group group,int lane,long bytes,long generation){this.group=group;this.lane=lane;this.bytes=bytes;this.generation=generation;}
        @Override public long generation(){return generation;}
        @Override public void claim()throws IOException{synchronized(RatisCommittedApplyDispatcher.this){if(group.stopped || stopped)throw new CommittedApplyStateMachine.Stopped();if(group.paused || group.generation!=generation)throw new CommittedApplyStateMachine.Retry();if(closed || lane==0)throw new IllegalStateException("Invalid committed token transfer");claimed=true;}}
        @Override public boolean claimed(){synchronized(RatisCommittedApplyDispatcher.this){return claimed;}}
        @Override public void close(){synchronized(RatisCommittedApplyDispatcher.this){if(closed)return;closed=true;lanes[lane].bytes-=bytes;lanes[lane].owners--;group.pool.lanes[lane].bytes-=bytes;group.pool.lanes[lane].owners--;if(lane==0)group.reading=false;else group.active=false;RatisCommittedApplyDispatcher.this.notifyAll();}}
    }
    public synchronized Usage stats(){return new Usage(lanes[0].bytes,lanes[0].owners,lanes[1].bytes,lanes[1].owners,lanes[2].bytes,lanes[2].owners,waiters.size());}
    public void awaitDrained(java.time.Duration timeout)throws IOException {
        long budget=timeout.toNanos(),started=System.nanoTime();
        synchronized(this){while(lanes[0].owners+lanes[1].owners+lanes[2].owners>0){long left=budget-(System.nanoTime()-started);if(left<=0)throw new IOException("Actual committed native ownership did not drain");try{TimeUnit.NANOSECONDS.timedWait(this,left);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Committed native drain interrupted",interrupted);}}}
    }
    @Override public void close(){List<Group> values;synchronized(this){stopped=true;values=List.copyOf(groups.values());notifyAll();}values.forEach(Group::stop);}
}
