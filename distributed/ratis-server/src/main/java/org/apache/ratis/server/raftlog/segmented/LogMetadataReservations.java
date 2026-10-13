/* Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
 * See http://www.apache.org/licenses/LICENSE-2.0. Provided AS IS without warranties. */
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.impl.LogResourceScope;
import org.apache.ratis.server.impl.LogResourceStateMachine;
import java.util.concurrent.ConcurrentSkipListMap;

/** Every newly admitted non-metadata row prepays one bounded committed-index metadata row. */
final class LogMetadataReservations implements AutoCloseable {
  static final long BYTES=512,SEGMENTS=2;
  private record Promise(long term,LogResourceStateMachine.ResidentReservation owner) {}
  private final ConcurrentSkipListMap<Long,Promise> promises=new ConcurrentSkipListMap<>();
  private volatile long recoveredCut=-1;
  private volatile boolean closed;
  void recoveredCut(long cut) { recoveredCut=Math.max(recoveredCut,cut); }
  boolean needsPromise(LogEntryProto entry) { return !entry.hasMetadataEntry() && entry.getIndex()>recoveredCut; }
  void record(LogEntryProto entry) {
    if(!needsPromise(entry))return;
    var parent=java.util.Objects.requireNonNull(LogResourceScope.currentResident());
    var child=parent.split(BYTES,1,SEGMENTS); var created=new Promise(entry.getTerm(),child);
    if(promises.putIfAbsent(entry.getIndex(),created)!=null) { child.close(); throw new IllegalStateException("Duplicate metadata allowance at "+entry.getIndex()); }
  }
  LogResourceStateMachine.ResidentReservation claim(long committedIndex) {
    var claimed=promises.remove(committedIndex);
    if(claimed==null)throw new IllegalStateException("Committed metadata has no prepaid allowance at "+committedIndex);
    return new LogResourceStateMachine.ResidentReservation() {
      final java.util.concurrent.atomic.AtomicBoolean used=new java.util.concurrent.atomic.AtomicBoolean(),closed=new java.util.concurrent.atomic.AtomicBoolean();
      public LogResourceStateMachine.Lease transferResident(long bytes,long headers,long segments) { var result=claimed.owner().transferResident(bytes,headers,segments); if(headers>0)used.set(true); return result; }
      public LogResourceStateMachine.ResidentReservation split(long bytes,long headers,long segments) { var result=claimed.owner().split(bytes,headers,segments); if(headers>0)used.set(true); return result; }
      public void close() { if(!closed.compareAndSet(false,true))return; if(!used.get() && !LogMetadataReservations.this.closed && promises.putIfAbsent(committedIndex,claimed)==null)return; claimed.owner().close(); }
    };
  }
  void completed(long committedIndex) {
    for(;;) { var first=promises.firstEntry(); if(first==null || first.getKey()>committedIndex)break; if(promises.remove(first.getKey(),first.getValue()))first.getValue().owner().close(); }
    recoveredCut(committedIndex);
  }
  void truncate(long from) { for(var entry:promises.tailMap(from,true).entrySet())if(promises.remove(entry.getKey(),entry.getValue()))entry.getValue().owner().close(); }
  @Override public void close() { closed=true; truncate(Long.MIN_VALUE); }
}
