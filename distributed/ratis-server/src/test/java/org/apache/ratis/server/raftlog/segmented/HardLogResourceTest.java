/* Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
 * See http://www.apache.org/licenses/LICENSE-2.0. Provided AS IS without warranties. */
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.LogResourceScope;
import org.apache.ratis.server.impl.LogResourceStateMachine;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class HardLogResourceTest {
  @TempDir Path root;
  private static final long MAX_ENTRY_BYTES = (64L << 20) + 4096;
  private static final long WRITE_BUFFER_BYTES = 64L << 10;
  /** Controllable policy fixture; the application shared CAS policy has separate tests. */
  static final class Policy implements LogResourceStateMachine.Resources {
    final long maxBytes,maxHeaders,maxSegments;
    volatile long bytes,headers,segments,maximumRead; volatile int intake,readCalls,activeReads; final List<Long> growth=new ArrayList<>();
    Policy(long bytes,long headers,long segments) { maxBytes=bytes;maxHeaders=headers;maxSegments=segments; }
    synchronized Memory allocate(long b,long h,long s) throws IOException { if(b>maxBytes-bytes || h>maxHeaders-headers || s>maxSegments-segments)throw new IOException("Controlled resident/header ceiling"); bytes+=b;headers+=h;segments+=s;return new Memory(b,h,s); }
    synchronized void release(long b,long h,long s) { bytes-=b;headers-=h;segments-=s;assertTrue(bytes>=0 && headers>=0 && segments>=0);notifyAll(); }
    synchronized void awaitHeaders(long expected) throws InterruptedException {
      long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
      while(headers!=expected) {
        long remaining=deadline-System.nanoTime();
        if(remaining<=0)fail("Actual header cleanup did not reach "+expected+": "+headers);
        TimeUnit.NANOSECONDS.timedWait(this,remaining);
      }
    }
    final class Memory implements LogResourceStateMachine.ResidentReservation {
      long b,h,s;boolean closed;
      Memory(long b,long h,long s) { this.b=b;this.h=h;this.s=s; }
      private synchronized void take(long bytes,long headers,long segments) { if(closed || bytes>b || headers>h || segments>s)throw new IllegalStateException("Prepaid memory exceeded");b-=bytes;h-=headers;s-=segments; }
      public LogResourceStateMachine.ResidentReservation split(long bytes,long headers,long segments) { take(bytes,headers,segments);return new Memory(bytes,headers,segments); }
      public LogResourceStateMachine.Lease transferResident(long bytes,long headers,long segments) { take(bytes,headers,segments);var once=new AtomicBoolean();return ()->{if(once.compareAndSet(false,true))release(bytes,headers,segments);}; }
      public synchronized void close() { if(closed)return;closed=true;release(b,h,s);b=h=s=0; }
    }
    public LogResourceStateMachine.Lease reserveDecode(long bytes) { return ()->{}; }
    public LogResourceStateMachine.Lease reserveIntake(long bytes,boolean heartbeat) { intake++;var once=new AtomicBoolean();return ()->{if(once.compareAndSet(false,true))intake--;}; }
    public LogResourceStateMachine.Lease reserveReply(boolean heartbeat) { return ()->{}; }
    public LogResourceStateMachine.Lease reserveResident(long bytes,long headers,long segments)throws IOException { return allocate(bytes,headers,segments); }
    public LogResourceStateMachine.ResidentReservation reserveInventory(long bytes,long headers,long segments)throws IOException { return allocate(bytes,headers,segments); }
    public synchronized LogResourceStateMachine.Lease reserveRead(long bytes) { readCalls++;activeReads++;maximumRead=Math.max(maximumRead,bytes);var once=new AtomicBoolean();return ()->{synchronized(this){if(once.compareAndSet(false,true))activeReads--;}}; }
    public long metadataReservationsLimit() { return Math.min(maxHeaders,Math.min(maxBytes/512,maxSegments/2)); }
    public LogResourceStateMachine.DiskReservation reserveDiskGrowth(long bytes,boolean progress) { growth.add(bytes);return ()->{}; }
    public LogResourceStateMachine.AppendReservation reserveAppend(LogResourceStateMachine.AppendFootprint f)throws IOException { var mem=allocate(f.residentBytes(),f.headers(),f.segments());var in=reserveIntake(f.serializedIntakeBytes(),f.protectedProgress());growth.add(f.diskUpperBound());return append(mem,in); }
    public LogResourceStateMachine.AppendReservation reserveMetadata(LogResourceStateMachine.AppendFootprint f,LogResourceStateMachine.ResidentReservation mem) { growth.add(f.diskUpperBound());return append(mem,()->{}); }
    private LogResourceStateMachine.AppendReservation append(LogResourceStateMachine.ResidentReservation mem,LogResourceStateMachine.Lease intake) { return new LogResourceStateMachine.AppendReservation() {boolean finished;public LogResourceStateMachine.Lease transferResident(long b,long h,long s){return mem.transferResident(b,h,s);}public LogResourceStateMachine.ResidentReservation split(long b,long h,long s){return mem.split(b,h,s);}public void actualFinished(){if(!finished){finished=true;intake.close();}}public void close(){assertTrue(finished);mem.close();}}; }
  }
  static final class Machine extends BaseStateMachine implements LogResourceStateMachine {
    final Policy policy; final CompletableFuture<Void> heldFlush;
    Machine(Policy policy,CompletableFuture<Void> heldFlush) { this.policy=policy;this.heldFlush=heldFlush; }
    public Resources logResources() { return policy; }
    @Override public DataApi data() { return new DataApi(){ public CompletableFuture<Void> flush(long index){return heldFlush==null?CompletableFuture.completedFuture(null):heldFlush;} }; }
  }
  record Opened(SegmentedRaftLog log,RaftStorage storage) implements AutoCloseable { public void close()throws IOException {log.close();} }
  Opened open(Path path,Machine machine,boolean recover) throws Exception {
    var storage=RaftStorage.newBuilder().setDirectory(path.toFile()).setOption(recover?RaftStorage.StartupOption.RECOVER:RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(SizeInBytes.valueOf(0)).build();storage.initialize();
    var props=new RaftProperties();RaftServerConfigKeys.Log.setForceSyncNum(props,1);RaftServerConfigKeys.Log.setUnsafeFlushEnabled(props,false);RaftServerConfigKeys.Log.setAsyncFlushEnabled(props,false);RaftServerConfigKeys.Log.StateMachineData.setSync(props,true);RaftServerConfigKeys.Log.StateMachineData.setCachingEnabled(props,false);RaftServerConfigKeys.Log.setQueueByteLimit(props,SizeInBytes.valueOf("128MB"));RaftServerConfigKeys.Log.setQueueElementLimit(props,64);RaftServerConfigKeys.Log.Appender.setBufferByteLimit(props,SizeInBytes.valueOf(MAX_ENTRY_BYTES));RaftServerConfigKeys.Log.setWriteBufferSize(props,SizeInBytes.valueOf(WRITE_BUFFER_BYTES));
    var member=RaftGroupMemberId.valueOf(RaftPeerId.valueOf("hard-log"),RaftGroupId.randomId());
    var log=SegmentedRaftLog.newBuilder().setMemberId(member).setStateMachine(machine).setStorage(storage).setProperties(props).setSnapshotIndexSupplier(()->-1).setNotifyTruncatedLogEntry(entry->{}).setGetTransactionContext((entry,create)->null).setSubmitUpdateCommitEvent(()->{}).build();
    try { log.open(-1,entry->{});return new Opened(log,storage); } catch(Throwable failure) { try {log.close();}catch(Throwable cleanup){failure.addSuppressed(cleanup);}throw failure; }
  }
  static LogEntryProto entry(long index,ByteString payload) { return LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload)).build(); }
  static void waitAppend(List<CompletableFuture<Long>> writes)throws Exception { CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).get(10,TimeUnit.SECONDS); }
  @Test void coldValidatedEmptyOpenSegmentIsRemovedBeforeSameIndexAppendAndOwnersDrain()throws Exception{
    var initial=new Policy(WRITE_BUFFER_BYTES+8192,8,16);var path=root.resolve("empty-open");final Path empty;
    try(var opened=open(path,new Machine(initial,null),false)){
      empty=opened.storage().getStorageDir().getCurrentDir().toPath().resolve("log_inprogress_0");
      // Real current-format writer, interrupted after opening/flushing its header and
      // before any entry: no synthetic log entry, commit, snapshot or native marker.
      try(var writer=new SegmentedRaftLogOutputStream(empty.toFile(),false,1L<<20,4096,java.nio.ByteBuffer.allocate(64*1024))){writer.flush();}
      assertTrue(java.nio.file.Files.exists(empty));
    }
    assertEquals(0,initial.bytes);assertEquals(0,initial.segments);assertEquals(0,initial.intake);
    var recovered=new Policy(WRITE_BUFFER_BYTES+8192,8,16);
    try(var opened=open(path,new Machine(recovered,null),true)){
      assertFalse(java.nio.file.Files.exists(empty),"Parsed zero-entry inode must not collide with the next StartLogSegment");
      waitAppend(opened.log().append(List.of(entry(0,ByteString.copyFromUtf8("first-real-row")))));
      assertEquals("first-real-row",opened.log().get(0).getStateMachineLogEntry().getLogData().toStringUtf8());
    }
    assertEquals(0,recovered.bytes);assertEquals(0,recovered.headers);assertEquals(0,recovered.segments);assertEquals(0,recovered.intake);
    var reopened=new Policy(WRITE_BUFFER_BYTES+8192,8,16);try(var opened=open(path,new Machine(reopened,null),true)){assertEquals("first-real-row",opened.log().get(0).getStateMachineLogEntry().getLogData().toStringUtf8());}
    assertEquals(0,reopened.bytes);assertEquals(0,reopened.headers);assertEquals(0,reopened.segments);
  }
  @Test void realLogHeadersRemainAfterEvictionMetadataUsesPrepaidAllowanceAndReopenRestoresBytes() throws Exception {
    var policy=new Policy(WRITE_BUFFER_BYTES+8192,4,16);Path path=root.resolve("workflow");
    try(var opened=open(path,new Machine(policy,null),false)) {
      var log=opened.log();assertEquals(WRITE_BUFFER_BYTES,policy.bytes);waitAppend(log.append(List.of(entry(0,ByteString.copyFromUtf8("zero")),entry(1,ByteString.copyFromUtf8("one")))));assertEquals(4,policy.headers);
      assertThrows(ExecutionException.class,()->waitAppend(log.append(List.of(entry(2,ByteString.copyFromUtf8("refused"))))));assertEquals(2,log.getNextIndex());
      long index=log.appendMetadata(1,1);assertEquals(2,index);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(log.getFlushIndex()<index && System.nanoTime()<deadline)Thread.onSpinWait();assertTrue(log.getFlushIndex()>=index);
      policy.awaitHeaders(3); // Flush-index publication precedes the metadata completion cleanup callback.
      log.getRaftLogCache().getOpenSegment().evictCache();assertEquals(3,policy.headers);assertEquals("one",log.get(1).getStateMachineLogEntry().getLogData().toStringUtf8());assertTrue(policy.readCalls>0);
      assertEquals(1,policy.growth.stream().filter(bytes->bytes>1L<<20).count(),"Shared preallocation is owned once, not per tiny append");
    }
    assertEquals(0,policy.bytes);assertEquals(0,policy.headers);assertEquals(0,policy.segments);assertEquals(0,policy.intake);
    var restored=new Policy(WRITE_BUFFER_BYTES+8192,4,16);try(var opened=open(path,new Machine(restored,null),true)) {assertEquals("zero",opened.log().get(0).getStateMachineLogEntry().getLogData().toStringUtf8());assertEquals("one",opened.log().get(1).getStateMachineLogEntry().getLogData().toStringUtf8());assertEquals(3,restored.headers);}
    assertEquals(0,restored.headers);
  }
  @Test void observerCancellationKeepsActualDataFlushAndCacheOwnersUntilWriterReturns() throws Exception {
    var policy=new Policy(WRITE_BUFFER_BYTES+8192,8,16);var flush=new CompletableFuture<Void>();var opened=open(root.resolve("blocked"),new Machine(policy,flush),false);
    try {var observer=opened.log().appendEntry(entry(0,ByteString.copyFromUtf8("held")));observer.cancel(false);assertTrue(policy.intake>0);assertTrue(policy.headers>0);flush.complete(null);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(policy.intake!=0 && System.nanoTime()<deadline)Thread.onSpinWait();assertEquals(0,policy.intake);assertTrue(policy.headers>0);}
    finally {flush.complete(null);opened.close();}assertEquals(0,policy.headers);assertEquals(0,policy.bytes);
  }
  static SegmentedRaftLog.Task controlledTask() {
    return new SegmentedRaftLog.Task() {
      void execute() { }
      long getEndIndex() { return 7; }
    };
  }
  @Test void installationFailureWaitsForActualReturnAndDerivedCreditRelease() throws Exception {
    var task = controlledTask(); var failure = new IOException("Controlled task failure before finally");
    var closing = new CountDownLatch(1); var release = new CountDownLatch(1); var credits = new AtomicInteger(1);
    var observer = SegmentedRaftLog.installAndDrain(() -> {
      SegmentedRaftLogWorker.trackActualTask(task, () -> {
        closing.countDown();
        try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("Credit release barrier timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
        credits.decrementAndGet();
      });
      return task.getFuture();
    });
    task.failed(failure); assertFalse(observer.isDone()); assertEquals(1, credits.get());
    try (var worker = Executors.newVirtualThreadPerTaskExecutor()) {
      var returned = worker.submit(task::returned);
      try {
        assertTrue(closing.await(5, TimeUnit.SECONDS)); assertFalse(observer.isDone()); assertEquals(1, credits.get());
        var canceled = observer.copy(); canceled.cancel(false); assertFalse(observer.isDone());
      } finally { release.countDown(); }
      returned.get(5, TimeUnit.SECONDS);
    }
    var terminal = assertThrows(ExecutionException.class, () -> observer.get(5, TimeUnit.SECONDS));
    assertSame(failure, terminal.getCause()); assertEquals(0, credits.get());
  }
  @Test void laterInstallationDispatchFailureWaitsForEarlierTaskReturn() throws Exception {
    var task = controlledTask(); var failure = new IllegalStateException("Later purge dispatch failed");
    var credits = new AtomicInteger(1);
    var observer = SegmentedRaftLog.installAndDrain(() -> {
      SegmentedRaftLogWorker.trackActualTask(task, credits::decrementAndGet);
      task.done();
      throw failure;
    });
    assertFalse(observer.isDone()); assertEquals(1, credits.get());
    task.returned();
    var terminal = assertThrows(ExecutionException.class, () -> observer.get(5, TimeUnit.SECONDS));
    assertSame(failure, terminal.getCause()); assertEquals(0, credits.get());
  }
  @Test void supportedOversizedEntryOwnsItsActualPayloadAcrossEvictionAndColdLoad() throws Exception {
    // The declared cap covers the complete LogEntryProto, including its twelve-byte envelope.
    var bytes=new byte[Math.toIntExact(MAX_ENTRY_BYTES)-12];bytes[0]=1;bytes[bytes.length-1]=2;var payload=ByteString.copyFrom(bytes);assertEquals(MAX_ENTRY_BYTES,entry(0,payload).getSerializedSize());var policy=new Policy(WRITE_BUFFER_BYTES+(80L<<20),8,64);Path path=root.resolve("oversized");
    try(var opened=open(path,new Machine(policy,null),false)) {waitAppend(opened.log().append(List.of(entry(0,payload))));assertTrue(policy.bytes>64L<<20);opened.log().getRaftLogCache().getOpenSegment().evictCache();assertEquals(payload,opened.log().get(0).getStateMachineLogEntry().getLogData());}
    var recovered=new Policy(WRITE_BUFFER_BYTES+(80L<<20),8,64);try(var opened=open(path,new Machine(recovered,null),true)) {assertEquals(payload,opened.log().get(0).getStateMachineLogEntry().getLogData());}assertEquals(0,recovered.bytes);assertEquals(3*MAX_ENTRY_BYTES+65536,recovered.maximumRead);assertEquals(0,recovered.activeReads);
  }
}
