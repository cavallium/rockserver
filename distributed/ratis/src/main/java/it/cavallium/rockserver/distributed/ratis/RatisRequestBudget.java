package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SERVER_OVERLOADED;

/** Credits precede owner construction and input copies, and outlive observers until actual ownership drains. */
final class RatisRequestBudget implements AutoCloseable {
    record Geometry(long inputBytes,long resultBytes,long maximumItemBytes,long maximumResultBytes,long logicalBytes,int items) {
        long bytes(){long journal=56+RemoteMutationToken.BYTES+36+32+8L*items+inputBytes+resultBytes;return Math.addExact(64L*1024+logicalBytes,Math.addExact(4*journal,4*(maximumItemBytes+maximumResultBytes)));}
    }
    private final RemoteMutationOwnerLimits limits;private final ThreadPoolExecutor recovery;private long bytes;private int owners,requests;private boolean closed;
    RatisRequestBudget(RemoteMutationOwnerLimits limits){this.limits=Objects.requireNonNull(limits);recovery=new ThreadPoolExecutor(limits.recoveryWorkers(),limits.recoveryWorkers(),0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(limits.maximumRequests()),Thread.ofPlatform().daemon(true).name("ratis-point-recovery-",0).factory(),new ThreadPoolExecutor.AbortPolicy());}
    RemoteMutationOwnerLimits limits(){return limits;}
    private void capacity(long amount){if(closed || amount<0 || amount>limits.maximumBytes()-limits.recoveryBytes()-limits.renewalBytes()-bytes)throw RocksDBException.of(SERVER_OVERLOADED,"Shared point owner byte envelope exhausted");}
    synchronized Owner owner(long directoryBytes,long openingBytes){long base=256L*1024+directoryBytes;long opening=Math.addExact(base,4*openingBytes);if(owners>=limits.maximumPointOwners())throw RocksDBException.of(SERVER_OVERLOADED,"Point owner history count exhausted");capacity(opening);owners++;bytes+=opening;return new Owner(base,opening);}
    final class Owner implements AutoCloseable {
        final long base;private long charged;private boolean released,opened;
        Owner(long base,long charged){this.base=base;this.charged=charged;}
        Lease opened(Geometry retained){synchronized(RatisRequestBudget.this){if(released || opened)throw new IllegalStateException("Owner construction credit already transferred");long amount=retained==null?0:retained.bytes();if(retained!=null && requests>=limits.maximumRequests())throw RocksDBException.of(SERVER_OVERLOADED,"Shared retained point request count exhausted");long replacement=base+amount;long delta=replacement-charged;if(delta>0)capacity(delta);bytes+=delta;charged=base;opened=true;if(retained!=null){requests++;return new Lease(amount);}return null;}}
        @Override public void close(){synchronized(RatisRequestBudget.this){if(released)return;released=true;owners--;bytes-=charged;}}
    }
    synchronized Lease request(Geometry geometry){long amount=geometry.bytes();if(requests>=limits.maximumRequests())throw RocksDBException.of(SERVER_OVERLOADED,"Shared point request count exhausted");capacity(amount);requests++;bytes+=amount;return new Lease(amount);}
    synchronized Lease work(long amount){capacity(amount);bytes+=amount;return new Lease(amount,false);}
    final class Lease implements AutoCloseable {final long amount;final boolean request;boolean released;Lease(long amount){this(amount,true);}Lease(long amount,boolean request){this.amount=amount;this.request=request;}@Override public void close(){synchronized(RatisRequestBudget.this){if(released)return;released=true;bytes-=amount;if(request)requests--;}}}
    <T> CompletableFuture<T> recover(Callable<T> work){var result=new CompletableFuture<T>();try{recovery.execute(() -> {try{result.complete(work.call());}catch(Throwable failure){result.completeExceptionally(failure);}});}catch(RuntimeException failure){result.completeExceptionally(failure);}return result;}
    /** Fixed workspace is shared only by the bounded recovery workers; excess is charged to the same envelope. */
    RecoveryCredit recoveryCredit(){synchronized(this){if(recoveryOwners>=limits.recoveryWorkers())throw RocksDBException.of(SERVER_OVERLOADED,"Shared recovery owners exhausted");recoveryOwners++;return new RecoveryCredit();}}
    private int recoveryOwners;
    final class RecoveryCredit implements AutoCloseable {
        private long extra;private boolean released;
        void require(long geometry){synchronized(RatisRequestBudget.this){if(released)throw new IllegalStateException("Recovery credit released");long needed=Math.max(0,geometry-limits.recoveryBytes()/limits.recoveryWorkers());if(needed>extra){capacity(needed-extra);bytes+=needed-extra;extra=needed;}}}
        @Override public void close(){synchronized(RatisRequestBudget.this){if(released)return;released=true;bytes-=extra;recoveryOwners--;}}
    }
    synchronized long chargedBytes(){return bytes+limits.recoveryBytes()+limits.renewalBytes();}
    synchronized int retainedRequests(){return requests;}
    static long files(Path... paths)throws IOException{long bytes=0;for(var path:paths)if(Files.exists(path,LinkOption.NOFOLLOW_LINKS)){if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign point owner journal object");bytes=Math.addExact(bytes,Files.size(path));}return bytes;}
    static Geometry geometry(DistributedSchema schema,long column,List<Keys> keys,List<Buf> values,RatisRocksDBDirect.WriteKind kind,RequestType.RequestTypeId type,boolean multi){
        if(keys.isEmpty() || keys.size()>256 || !multi && keys.size()!=1 || kind!=RatisRocksDBDirect.WriteKind.DELETE && keys.size()!=values.size())throw new IllegalArgumentException("Invalid point input count");var shape=RatisLogicalClient.supported(schema,column).schema();long physical=0;for(int n=0;n<shape.keysCount();n++)physical+=shape.key(n);long input=0,logical=0,maximum=0;
        for(int n=0;n<keys.size();n++){var components=keys.get(n).keys();if(components.length!=shape.keysCount())throw new IllegalArgumentException("Logical key shape changed");long members=shape.variableLengthKeysCount()==0?0:4;for(int part=0;part<components.length;part++){int size=components[part].size();if(part<shape.fixedLengthKeysCount()?size!=shape.key(part):size>65535)throw new IllegalArgumentException("Logical key width changed");logical+=size;if(part>=shape.fixedLengthKeysCount())members+=2L+size;}long value=kind==RatisRocksDBDirect.WriteKind.DELETE || values.get(n)==null?0:values.get(n).size();logical+=value;long item=DistributedCommandCodec.HEADER_BYTES+26+physical+members+value;if(item>DistributedCommandCodec.MAX_ENCODED_BYTES)throw new IllegalArgumentException("Point item exceeds command bound");input+=item;maximum=Math.max(maximum,item);}
        if(input>RatisDirectRequestJournal.MAX_INPUT)throw new IllegalArgumentException("Point input exceeds aggregate bound");boolean returns=type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.ENSURE;long oneResult=!returns?8:shape.hasValue()?CommandResult.MAX_BYTES:12;long result=Math.min(RatisDirectRequestJournal.MAX_RESULTS+CommandResult.MAX_BYTES,keys.size()*oneResult);return new Geometry(input,result,maximum,oneResult,logical,keys.size());
    }
    static Geometry geometry(DistributedSchema schema,RatisDirectRequestJournal.Request request){long input=0,maximum=0,logical=0;for(var item:request.items){var mutation=item.mutations().getFirst();var shape=schema.column(mutation.columnId()).schema();long hashes=0;for(int n=shape.fixedLengthKeysCount();n<shape.keysCount();n++)hashes+=shape.key(n);long bytes=DistributedCommandCodec.encodedSize(item);input+=bytes;maximum=Math.max(maximum,bytes);logical+=mutation.encodedSize()-26-hashes-(shape.variableLengthKeysCount()==0?0:4+2L*shape.variableLengthKeysCount());}boolean returns=request.type!=RequestType.RequestTypeId.NOTHING && request.type!=RequestType.RequestTypeId.ENSURE;boolean hasValue=schema.column(request.items.getFirst().mutations().getFirst().columnId()).schema().hasValue();long one=!returns?8:hasValue?CommandResult.MAX_BYTES:12;return new Geometry(input,Math.min(RatisDirectRequestJournal.MAX_RESULTS+CommandResult.MAX_BYTES,request.items.size()*one),maximum,one,logical,request.items.size());}
    @Override public synchronized void close()throws IOException{if(owners!=0 || requests!=0 || recoveryOwners!=0)throw new IOException("Shared point owners/requests remain charged");closed=true;recovery.shutdown();}
}
