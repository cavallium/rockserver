/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.impl.LogResourceStateMachine;
import org.apache.ratis.server.impl.LogResourceScope;
import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ratis.util.CodeInjectionForTesting;


/**
 * In-memory cache for a log segment file. All the updates will be first written
 * into LogSegment then into corresponding files in the same order.
 *
 * This class will be protected by the {@link SegmentedRaftLog}'s read-write lock.
 */
public final class LogSegment {

  static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

  enum Op {
    LOAD_SEGMENT_FILE,
    REMOVE_CACHE,
    CHECK_SEGMENT_FILE_FULL,
    WRITE_CACHE_WITH_STATE_MACHINE_CACHE,
    WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE
  }

  static long getEntrySize(LogEntryProto entry, Op op) {
    LogEntryProto e = entry;
    if (op == Op.CHECK_SEGMENT_FILE_FULL) {
      e = LogProtoUtils.removeStateMachineData(entry);
    } else if (op == Op.LOAD_SEGMENT_FILE || op == Op.WRITE_CACHE_WITH_STATE_MACHINE_CACHE) {
      Preconditions.assertTrue(entry == LogProtoUtils.removeStateMachineData(entry),
          () -> "Unexpected LogEntryProto with StateMachine data: op=" + op + ", entry=" + entry);
    } else {
      Preconditions.assertTrue(op == Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE || op == Op.REMOVE_CACHE,
          () -> "Unexpected op " + op + ", entry=" + entry);
    }
    final int serialized = e.getSerializedSize();
    return serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized) + 4L;
  }

  static class LogRecord {
    /** starting offset in the file */
    private final long offset;
    private final LogEntryHeader logEntryHeader;
    private LogResourceStateMachine.Lease resource;

    LogRecord(long offset, LogEntryProto entry) {
      this(offset,entry,LogResourceScope.currentResident()==null ? null : LogResourceScope.currentResident().transferResident(0,1,0));
    }
    LogRecord(long offset,LogEntryProto entry,LogResourceStateMachine.Lease resource) {
      this.offset = offset;
      this.logEntryHeader = LogEntryHeader.valueOf(entry);
      this.resource=resource;
    }
    void releaseResource() { var retained=resource; resource=null; if(retained!=null)retained.close(); }

    LogEntryHeader getLogEntryHeader() {
      return logEntryHeader;
    }

    TermIndex getTermIndex() {
      return getLogEntryHeader().getTermIndex();
    }

    long getOffset() {
      return offset;
    }
  }

  private static class Records {
    private final ConcurrentNavigableMap<Long, LogRecord> map = new ConcurrentSkipListMap<>();

    int size() {
      return map.size();
    }

    LogRecord getFirst() {
      final Map.Entry<Long, LogRecord> first = map.firstEntry();
      return first != null? first.getValue() : null;
    }

    LogRecord getLast() {
      final Map.Entry<Long, LogRecord> last = map.lastEntry();
      return last != null? last.getValue() : null;
    }

    LogRecord get(long i) {
      return map.get(i);
    }

    long append(LogRecord record) {
      final long index = record.getTermIndex().getIndex();
      final LogRecord previous = map.put(index, record);
      Preconditions.assertNull(previous, "previous");
      return index;
    }

    LogRecord removeLast() {
      final Map.Entry<Long, LogRecord> last = map.pollLastEntry();
      return Objects.requireNonNull(last, "last == null").getValue();
    }

    void clear() {
      map.clear();
    }
  }

  static LogSegment newOpenSegment(RaftStorage storage, long start, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0);
    return new LogSegment(storage, true, start, start - 1, maxOpSize, raftLogMetrics);
  }
  static LogSegment newOpenSegment(RaftStorage storage,long start,SizeInBytes maxOpSize,SegmentedRaftLogMetrics metrics,LogResourceStateMachine.Resources resources,LogMetadataReservations metadata) {
    return new LogSegment(storage,true,start,start-1,maxOpSize,metrics,resources,metadata);
  }

  @VisibleForTesting
  static LogSegment newCloseSegment(RaftStorage storage,
      long start, long end, SizeInBytes maxOpSize, SegmentedRaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0 && end >= start);
    return new LogSegment(storage, false, start, end, maxOpSize, raftLogMetrics);
  }

  static LogSegment newLogSegment(RaftStorage storage, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics metrics) {
    return startEnd.isOpen()? newOpenSegment(storage, startEnd.getStartIndex(), maxOpSize, metrics)
        : newCloseSegment(storage, startEnd.getStartIndex(), startEnd.getEndIndex(), maxOpSize, metrics);
  }

  public static int readSegmentFile(File file, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      CorruptionPolicy corruptionPolicy, SegmentedRaftLogMetrics raftLogMetrics, Consumer<LogEntryProto> entryConsumer)
      throws IOException {
    int count = 0;
    try(SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, startEnd, maxOpSize, raftLogMetrics)) {
      for(LogEntryProto prev = null, next; (next = in.nextEntry()) != null; prev = next) {
        if (prev != null) {
          Preconditions.assertTrue(next.getIndex() == prev.getIndex() + 1,
              "gap between entry %s and entry %s", prev, next);
        }

        if (entryConsumer != null) {
          entryConsumer.accept(next);
        }
        count++;
      }
    } catch (IOException ioe) {
      switch (corruptionPolicy) {
        case EXCEPTION: throw ioe;
        case WARN_AND_RETURN:
          LOG.warn("Failed to read segment file {} ({}): only {} entries read successfully",
              file, startEnd, count, ioe);
          break;
        default:
          throw new IllegalStateException("Unexpected enum value: " + corruptionPolicy
              + ", class=" + CorruptionPolicy.class);
      }
    }

    return count;
  }

  static LogSegment loadSegment(RaftStorage storage, File file, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      boolean keepEntryInCache, Consumer<LogEntryProto> logConsumer, SegmentedRaftLogMetrics raftLogMetrics)
      throws IOException {
    final LogSegment segment = newLogSegment(storage, startEnd, maxOpSize, raftLogMetrics);
    final CorruptionPolicy corruptionPolicy = CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
    final boolean isOpen = startEnd.isOpen();
    final int entryCount = readSegmentFile(file, startEnd, maxOpSize, corruptionPolicy, raftLogMetrics, entry -> {
      segment.append(keepEntryInCache || isOpen, entry, Op.LOAD_SEGMENT_FILE, true);
      if (logConsumer != null) {
        logConsumer.accept(entry);
      }
    });
    LOG.info("Successfully read {} entries from segment file {}", entryCount, file);

    final long start = startEnd.getStartIndex();
    final long end = isOpen? segment.getEndIndex(): startEnd.getEndIndex();
    final int expectedEntryCount = Math.toIntExact(end - start + 1);
    final boolean corrupted = entryCount != expectedEntryCount;
    if (corrupted) {
      LOG.warn("Segment file is corrupted: expected to have {} entries but only {} entries read successfully",
          expectedEntryCount, entryCount);
    }

    if (entryCount == 0) {
      // The segment does not have any entries, delete the file.
      final Path deleted = FileUtils.deleteFile(file);
      LOG.info("Deleted RaftLog segment since entry count is zero: startEnd={}, path={}", startEnd, deleted);
      return null;
    } else if (file.length() > segment.getTotalFileSize()) {
      // The segment has extra padding, truncate it.
      FileUtils.truncateFile(file, segment.getTotalFileSize());
    }

    try {
      segment.assertSegment(start, entryCount, corrupted, end);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read segment file " + file, e);
    }
    return segment;
  }

  static LogSegment loadSegment(RaftStorage storage,File file,LogSegmentStartEnd startEnd,SizeInBytes maxOpSize,
      boolean keepCache,Consumer<LogEntryProto> consumer,SegmentedRaftLogMetrics metrics,LogResourceStateMachine.Resources resources,LogMetadataReservations metadata) throws IOException {
    if(resources==null)return loadSegment(storage,file,startEnd,maxOpSize,keepCache,consumer,metrics);
    long count; final long[] debts={0},cached={0};
    try(var read=resources.reserveRead(LogResourceScope.coldReadBytes(maxOpSize.getSize()))) {
      count=readSegmentFile(file,startEnd,maxOpSize,CorruptionPolicy.get(storage,RaftStorage::getLogCorruptionPolicy),metrics,entry->{ if(metadata.needsPromise(entry))debts[0]++; cached[0]=Math.addExact(cached[0],entry.getSerializedSize()+256L); });
    }
    long metadataBytes=Math.multiplyExact(debts[0],LogMetadataReservations.BYTES),headers=Math.addExact(count,debts[0]),segments=Math.addExact(1,Math.multiplyExact(debts[0],LogMetadataReservations.SEGMENTS));
    long payload=Math.addExact(keepCache || startEnd.isOpen() ? cached[0] : 0,metadataBytes);
    LogResourceStateMachine.ResidentReservation inventory;
    try { inventory=resources.reserveInventory(payload,headers,segments); }
    catch(IOException rejected) { if(startEnd.isOpen() || payload==metadataBytes)throw rejected; keepCache=false; payload=metadataBytes; inventory=resources.reserveInventory(metadataBytes,headers,segments); }
    LogSegment segment=null;
    try(var reserved=inventory;var scope=LogResourceScope.enter(inventory);var read=resources.reserveRead(LogResourceScope.coldReadBytes(maxOpSize.getSize()))) {
      segment=new LogSegment(storage,startEnd.isOpen(),startEnd.getStartIndex(),startEnd.isOpen()?startEnd.getStartIndex()-1:startEnd.getEndIndex(),maxOpSize,metrics,resources,metadata);
      final LogSegment loaded=segment; final boolean cache=keepCache || startEnd.isOpen();
      int actual=readSegmentFile(file,startEnd,maxOpSize,CorruptionPolicy.get(storage,RaftStorage::getLogCorruptionPolicy),metrics,entry->{ loaded.append(cache,entry,Op.LOAD_SEGMENT_FILE,true); if(consumer!=null)consumer.accept(entry); });
      if(actual!=count)throw new IOException("Cold log inventory changed while loading");
      if(actual==0) {
        segment.clear();
        // A validated empty open segment has no cached owner. Leaving its inode behind
        // makes the next genuine StartLogSegment at this same index collide on cold open.
        FileUtils.deleteFile(file);
        return null;
      }
      segment.assertSegment(startEnd.getStartIndex(),actual,false,segment.getEndIndex()); return segment;
    } catch(IOException|RuntimeException|Error failure) { if(segment!=null)segment.clear(); throw failure; }
  }

  private void assertSegment(long expectedStart, int expectedEntryCount, boolean corrupted, long expectedEnd) {
    Preconditions.assertSame(expectedStart, getStartIndex(), "Segment start index");
    Preconditions.assertSame(expectedEntryCount, records.size(), "Number of records");

    final long expectedLastIndex = expectedStart + expectedEntryCount - 1;
    Preconditions.assertSame(expectedLastIndex, getEndIndex(), "Segment end index");

    final LogRecord last = records.getLast();
    if (last != null) {
      Preconditions.assertSame(expectedLastIndex, last.getTermIndex().getIndex(), "Index at the last record");
      final LogRecord first = records.getFirst();
      Objects.requireNonNull(first, "first record");
      Preconditions.assertSame(expectedStart, first.getTermIndex().getIndex(), "Index at the first record");
    }
    if (!corrupted) {
      Preconditions.assertSame(expectedEnd, expectedLastIndex, "End/last Index");
    }
  }

  /**
   * The current log entry loader simply loads the whole segment into the memory.
   * In most of the cases this may be good enough considering the main use case
   * for load log entries is for leader appending to followers.
   *
   * In the future we can make the cache loader configurable if necessary.
   */
  class LogEntryLoader extends CacheLoader<LogRecord, LogEntryProto> {
    private final SegmentedRaftLogMetrics raftLogMetrics;

    LogEntryLoader(SegmentedRaftLogMetrics raftLogMetrics) {
      this.raftLogMetrics = raftLogMetrics;
    }

    @Override
    public LogEntryProto load(LogRecord key) throws IOException {
      final File file = getFile();
      // note the loading should not exceed the endIndex: it is possible that
      // the on-disk log file should be truncated but has not been done yet.
      final AtomicReference<LogEntryProto> toReturn = new AtomicReference<>();
      final LogSegmentStartEnd startEnd = LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen);
      readSegmentFile(file, startEnd, maxOpSize,
          getLogCorruptionPolicy(), raftLogMetrics, entry -> {
        final TermIndex ti = TermIndex.valueOf(entry);
        putEntryCache(ti, entry, Op.LOAD_SEGMENT_FILE);
        if (ti.equals(key.getTermIndex())) {
          toReturn.set(entry);
        }
      });
      loadingTimes.incrementAndGet();
      final LogEntryProto proto = toReturn.get();
      if (proto == null) {
        throw new RaftLogIOException("Failed to load log entry " + key);
      }
      return proto;
    }
  }

  File getFile() {
    return LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen).getFile(storage);
  }

  private volatile boolean isOpen;
  private long totalFileSize = SegmentedRaftLogFormat.getHeaderLength();
  private AtomicLong totalCacheSize = new AtomicLong(0);
  /** Segment start index, inclusive. */
  private final long startIndex;
  /** Segment end index, inclusive. */
  private volatile long endIndex;
  private final RaftStorage storage;
  private final SizeInBytes maxOpSize;
  private final LogEntryLoader cacheLoader;
  /** later replace it with a metric */
  private final AtomicInteger loadingTimes = new AtomicInteger();

  /**
   * the list of records is more like the index of a segment
   */
  private final Records records = new Records();
  /**
   * the entryCache caches the content of log entries.
   */
  private final Map<TermIndex, LogEntryProto> entryCache = new ConcurrentHashMap<>();
  private final Map<TermIndex,LogResourceStateMachine.Lease> cacheResources=new java.util.HashMap<>();
  private final LogResourceStateMachine.Resources resources;
  private final LogMetadataReservations metadata;
  private LogResourceStateMachine.Lease segmentResource;

  private LogSegment(RaftStorage storage, boolean isOpen, long start, long end, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics raftLogMetrics) {
    this(storage,isOpen,start,end,maxOpSize,raftLogMetrics,null,null);
  }
  private LogSegment(RaftStorage storage,boolean isOpen,long start,long end,SizeInBytes maxOpSize,SegmentedRaftLogMetrics raftLogMetrics,LogResourceStateMachine.Resources resources,LogMetadataReservations metadata) {
    this.storage = storage;
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
    this.maxOpSize = maxOpSize;
    this.cacheLoader = new LogEntryLoader(raftLogMetrics);
    this.resources=resources;
    this.metadata=metadata;
    var reservation=LogResourceScope.currentResident(); segmentResource=resources==null ? null : java.util.Objects.requireNonNull(reservation,"Segment creation requires pre-effect resource reservation").transferResident(0,0,1);
  }

  long getStartIndex() {
    return startIndex;
  }

  long getEndIndex() {
    if (!isOpen) {
      return endIndex;
    }
    final LogRecord last = records.getLast();
    return last == null ? getStartIndex() - 1 : last.getTermIndex().getIndex();
  }

  boolean isOpen() {
    return isOpen;
  }

  int numOfEntries() {
    return Math.toIntExact(getEndIndex() - startIndex + 1);
  }

  CorruptionPolicy getLogCorruptionPolicy() {
    return CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
  }

  void appendToOpenSegment(LogEntryProto entry, Op op, boolean verifyEntryIndex) {
    Preconditions.assertTrue(isOpen(), "The log segment %s is not open for append", this);
    append(true, entry, op, verifyEntryIndex);
  }
  static final class EntryResources implements AutoCloseable {
    private LogResourceStateMachine.Lease header,payload;
    EntryResources(LogEntryProto entry,LogResourceStateMachine.Resources resources) {
      if(resources==null)return; var reserved=java.util.Objects.requireNonNull(LogResourceScope.currentResident());
      header=reserved.transferResident(0,1,0);
      try { payload=reserved.transferResident(entry.getSerializedSize()+256L,0,0); } catch(Throwable failure) { header.close(); header=null; throw failure; }
    }
    LogResourceStateMachine.Lease header() { var value=header; header=null; return value; }
    LogResourceStateMachine.Lease payload() { var value=payload; payload=null; return value; }
    public void close() { if(header!=null) { header.close(); header=null; } if(payload!=null) { payload.close(); payload=null; } }
  }
  void appendToOpenSegment(LogEntryProto entry,Op op,EntryResources prepared) { Preconditions.assertTrue(isOpen()); append(true,entry,op,false,prepared); }

  public static final String APPEND_RECORD = LogSegment.class.getSimpleName() + ".append";
  private void append(boolean keepEntryInCache, LogEntryProto entry, Op op, boolean verifyEntryIndex) {
    append(keepEntryInCache,entry,op,verifyEntryIndex,null);
  }
  private void append(boolean keepEntryInCache,LogEntryProto entry,Op op,boolean verifyEntryIndex,EntryResources prepared) {
    Objects.requireNonNull(entry, "entry == null");
    if (verifyEntryIndex) {
      verifyEntryIndex(entry.getIndex());
    }
    if(metadata!=null && prepared==null)metadata.record(entry);
    final LogRecord record = prepared==null ? new LogRecord(totalFileSize,entry) : new LogRecord(totalFileSize,entry,prepared.header());
    if (keepEntryInCache) {
      // It is important to put the entry into the cache before appending the
      // record to the record list. Otherwise, a reader thread may get the
      // record from the list but not the entry from the cache.
      putEntryCache(record.getTermIndex(), entry, op,prepared==null ? null : prepared.payload());
      CodeInjectionForTesting.execute(APPEND_RECORD, this, record.getTermIndex());
    }
    records.append(record);

    totalFileSize += getEntrySize(entry, op);
    endIndex = entry.getIndex();
  }

  void verifyEntryIndex(long entryIndex) {
    final LogRecord currentLast = records.getLast();
    if (currentLast == null) {
      Preconditions.assertTrue(entryIndex == startIndex,
          "gap between start index %s and first entry to append %s",
          startIndex, entryIndex);
    } else {
      Preconditions.assertTrue(entryIndex == currentLast.getTermIndex().getIndex() + 1,
          "gap between entries %s and %s", entryIndex, currentLast.getTermIndex().getIndex());
    }
  }

  LogEntryProto getEntryFromCache(TermIndex ti) {
    return entryCache.get(ti);
  }

  /**
   * Acquire LogSegment's monitor so that there is no concurrent loading.
   */
  LogEntryProto loadCache(LogRecord record) throws RaftLogIOException {
    LogEntryProto entry = entryCache.get(record.getTermIndex());
    if (entry != null) {
      return entry;
    }
    try {
      if(resources!=null) {
        try(var read=LogResourceScope.hasAdmittedRead() ? null : resources.reserveRead(LogResourceScope.coldReadBytes(maxOpSize.getSize()));var input=new SegmentedRaftLogInputStream(getFile(),LogSegmentStartEnd.valueOf(startIndex,endIndex,isOpen),maxOpSize,null)) {
          LogEntryProto requested;
          while((requested=input.nextEntry())!=null) {
            if(requested.getIndex()>record.getTermIndex().getIndex())break;
            if(TermIndex.valueOf(requested).equals(record.getTermIndex())) { putEntryCache(record.getTermIndex(),requested,Op.LOAD_SEGMENT_FILE); loadingTimes.incrementAndGet(); return requested; }
          }
          throw new RaftLogIOException("Requested bounded log entry not found: "+record);
        }
      }
      return cacheLoader.load(record);
    } catch (RaftLogIOException e) {
      throw e;
    } catch (Exception e) {
      throw new RaftLogIOException("Failed to loadCache for log entry " + record, e);
    }
  }

  LogRecord getLogRecord(long index) {
    if (index >= startIndex && index <= getEndIndex()) {
      return records.get(index);
    }
    return null;
  }

  TermIndex getLastTermIndex() {
    final LogRecord last = records.getLast();
    return last == null ? null : last.getTermIndex();
  }

  long getTotalFileSize() {
    return totalFileSize;
  }

  long getTotalCacheSize() {
    return totalCacheSize.get();
  }

  /**
   * Remove records from the given index (inclusive)
   */
  synchronized void truncate(long fromIndex) {
    Preconditions.assertTrue(fromIndex >= startIndex && fromIndex <= endIndex);
    for (long index = endIndex; index >= fromIndex; index--) {
      final LogRecord removed = records.removeLast();
      Preconditions.assertSame(index, removed.getTermIndex().getIndex(), "removedIndex");
      removeEntryCache(removed.getTermIndex(), Op.REMOVE_CACHE);
      totalFileSize = removed.offset;
      removed.releaseResource();
    }
    isOpen = false;
    this.endIndex = fromIndex - 1;
  }

  void close() {
    Preconditions.assertTrue(isOpen());
    isOpen = false;
  }

  @Override
  public String toString() {
    return isOpen() ? "log_" + "inprogress_" + startIndex :
        "log-" + startIndex + "_" + endIndex;
  }

  /** Comparator to find <code>index</code> in list of <code>LogSegment</code>s. */
  static final Comparator<Object> SEGMENT_TO_INDEX_COMPARATOR = (o1, o2) -> {
    if (o1 instanceof LogSegment && o2 instanceof Long) {
      return ((LogSegment) o1).compareTo((Long) o2);
    } else if (o1 instanceof Long && o2 instanceof LogSegment) {
      return Integer.compare(0, ((LogSegment) o2).compareTo((Long) o1));
    }
    throw new IllegalStateException("Unexpected objects to compare(" + o1 + "," + o2 + ")");
  };

  private int compareTo(Long l) {
    return (l >= getStartIndex() && l <= getEndIndex()) ? 0 :
        (this.getEndIndex() < l ? -1 : 1);
  }

  synchronized void clear() {
    var removed=new java.util.ArrayList<LogRecord>(); for(long index=startIndex;index<=endIndex;index++) { var record=records.get(index); if(record!=null)removed.add(record); }
    records.clear();
    removed.forEach(LogRecord::releaseResource);
    evictCache();
    endIndex = startIndex - 1;
    var retained=segmentResource; segmentResource=null; if(retained!=null)retained.close();
  }

  int getLoadingTimes() {
    return loadingTimes.get();
  }

  synchronized void evictCache() {
    entryCache.clear();
    totalCacheSize.set(0);
    var retained=new java.util.ArrayList<>(cacheResources.values()); cacheResources.clear(); retained.forEach(LogResourceStateMachine.Lease::close);
  }

  synchronized void putEntryCache(TermIndex key, LogEntryProto value, Op op) {
    putEntryCache(key,value,op,null);
  }
  private synchronized void putEntryCache(TermIndex key,LogEntryProto value,Op op,LogResourceStateMachine.Lease supplied) {
    if(resources!=null && entryCache.containsKey(key)) { if(supplied!=null)supplied.close(); return; }
    LogResourceStateMachine.Lease retained=supplied;
    if(resources!=null && retained==null && !entryCache.containsKey(key)) {
      var reservation=LogResourceScope.currentResident(); long bytes=value.getSerializedSize()+256L;
      if(reservation!=null)retained=reservation.transferResident(bytes,0,0);
      else try { retained=resources.reserveResident(bytes,0,0); } catch(IOException optionalCacheFull) { return; }
    }
    LogEntryProto previous = entryCache.put(key, value);
    long previousSize = 0;
    if (previous != null) {
      // Different threads maybe load LogSegment file into cache at the same time, so duplicate maybe happen
      previousSize = getEntrySize(value, Op.REMOVE_CACHE);
    }
    totalCacheSize.getAndAdd(getEntrySize(value, op) - previousSize);
    previous=null; if(retained!=null)cacheResources.put(key,retained);
  }

  synchronized void removeEntryCache(TermIndex key, Op op) {
    LogEntryProto value = entryCache.remove(key);
    if (value != null) {
      totalCacheSize.getAndAdd(-getEntrySize(value, op));
    }
    value=null; var retained=cacheResources.remove(key); if(retained!=null)retained.close();
  }

  boolean hasCache() {
    return isOpen || !entryCache.isEmpty(); // open segment always has cache.
  }

  boolean containsIndex(long index) {
    return startIndex <= index && getEndIndex() >= index;
  }

  boolean hasEntries() {
    return numOfEntries() > 0;
  }

}
