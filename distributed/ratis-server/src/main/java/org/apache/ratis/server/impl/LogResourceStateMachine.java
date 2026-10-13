/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.impl;

import org.apache.ratis.server.RaftServer;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Nonblocking physical log-resource admission, independent of committed application admission. */
public interface LogResourceStateMachine {
  Resources logResources();
  /** Strict trusted canonical control classification; malformed and unknown input is ordinary. */
  default boolean isProtectedLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto entry) throws IOException { return false; }

  default boolean areProtectedLogEntries(Iterable<org.apache.ratis.proto.RaftProtos.LogEntryProto> entries) throws IOException {
    for(var entry:entries)if(!isProtectedLogEntry(entry))return false;
    return true;
  }

  interface Lease extends AutoCloseable { @Override void close(); }
  @FunctionalInterface interface DiskReservation { void actualFinished(); }

  record AppendFootprint(long serializedIntakeBytes,long residentBytes,long headers,long segments,long diskUpperBound,
                         boolean protectedProgress) {
    public AppendFootprint {
      if(serializedIntakeBytes<0 || residentBytes<0 || headers<0 || segments<0 || diskUpperBound<0) {
        throw new IllegalArgumentException("Negative log-resource footprint");
      }
    }
  }

  interface ResidentReservation extends Lease {
    /** Transfers already charged memory to the exact resident owner; no aggregate acquisition under cache locks. */
    Lease transferResident(long bytes,long headers,long segments);
    default ResidentReservation split(long bytes,long headers,long segments) { throw new IllegalStateException("Resident split is not configured"); }
  }
  interface AppendReservation extends ResidentReservation {
    /** Called only after all accepted append/data I/O has actually returned, never from an observer cancellation. */
    void actualFinished();
  }

  interface Resources {
    default Lease reserveStream() throws IOException { throw new IOException("Bounded transport stream admission is not configured"); }
    default Lease reserveClassification(long scratchBytes) throws IOException { throw new IOException("Classification scratch admission is not configured"); }
    Lease reserveDecode(long maximumSerializedBytes) throws IOException;
    Lease reserveIntake(long serializedBytes,boolean protectedHeartbeat) throws IOException;
    default Lease reserveControlIntake(long serializedBytes) throws IOException { throw new IOException("Protected control intake is not configured"); }
    Lease reserveReply(boolean protectedHeartbeat) throws IOException;
    AppendReservation reserveAppend(AppendFootprint footprint) throws IOException;
    default AppendReservation reserveMetadata(AppendFootprint footprint,ResidentReservation committedAllowance) throws IOException {
      throw new IOException("Committed log metadata allowance is not configured");
    }
    default DiskReservation reserveDiskGrowth(long bytes,boolean protectedProgress) throws IOException { throw new IOException("Shared log disk-growth reservation is not configured"); }
    default long metadataReservationsLimit() { return 0; }
    Lease reserveResident(long bytes,long headers,long segments) throws IOException;
    default ResidentReservation reserveInventory(long bytes,long headers,long segments) throws IOException {
      throw new IOException("Cold log inventory reservation is not configured");
    }
    Lease reserveRead(long bytes) throws IOException;
  }

  /** Runtime binds once before RPC startup and removes the binding only after its accepted work has drained. */
  final class Registration implements AutoCloseable {
    private final RaftServer server;
    private final Resources resources;
    private boolean closed;
    private Registration(RaftServer server,Resources resources) { this.server=server; this.resources=resources; }
    @Override public void close() {
      synchronized(LogResourceStateMachine.class) {
        if(closed)return;
        var next=new IdentityHashMap<RaftServer,Resources>(Bindings.current);
        if(next.get(server)!=resources)throw new IllegalStateException("Log-resource server binding changed");
        next.remove(server); Bindings.current=next; closed=true;
      }
    }
  }
  final class Bindings {
    private Bindings() {}
    private static volatile Map<RaftServer,Resources> current=new IdentityHashMap<>();
  }
  static Registration bindServer(RaftServer server,Resources resources) {
    Objects.requireNonNull(server); Objects.requireNonNull(resources);
    synchronized(LogResourceStateMachine.class) {
      if(Bindings.current.containsKey(server))throw new IllegalStateException("Log-resource server already bound");
      var next=new IdentityHashMap<RaftServer,Resources>(Bindings.current); next.put(server,resources); Bindings.current=next;
      return new Registration(server,resources);
    }
  }
  static Resources forServer(RaftServer server) { return Bindings.current.get(server); }
}
