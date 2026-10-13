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
package org.apache.ratis.server.impl;

import java.io.IOException;
import java.util.Arrays;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.StateMachine;

/** Admission before the pinned updater materializes a committed head. Tokens own actual work, not futures. */
public interface CommittedApplyStateMachine extends StateMachine {
  final class Retry extends IOException { public Retry() { super("Committed read invalidated by reload"); } }
  final class Stopped extends IOException { public Stopped() { super("Committed group stopped"); } }
  enum Lane { ORDINARY, PROTECTED }
  record Descriptor(long term, long index, int bodyCase, Lane lane, long bytes, byte[] digest, long generation) {
    public Descriptor { if (bytes < 1 || digest.length != 32) throw new IllegalArgumentException(); digest = digest.clone(); }
    @Override public byte[] digest() { return digest.clone(); }
    public boolean sameEntry(Descriptor other) { return term == other.term && index == other.index
        && bodyCase == other.bodyCase && lane == other.lane && bytes == other.bytes && generation == other.generation && Arrays.equals(digest, other.digest); }
  }
  interface ReadPermit extends AutoCloseable { long generation(); @Override void close(); }
  interface ApplyPermit extends AutoCloseable {
    /** Set by the state machine before retaining any entry/context graph. */
    void claim() throws IOException;
    boolean claimed();
    /** Release only after actual native work and cleanup have drained. */
    @Override void close();
  }
  interface Admission {
    ReadPermit awaitRead(long index) throws IOException, InterruptedException;
    Descriptor describe(LogEntryProto entry) throws IOException;
    ApplyPermit tryDispatch(Descriptor descriptor) throws IOException;
    ApplyPermit awaitDispatch(Descriptor descriptor) throws IOException, InterruptedException;
    void validate(Descriptor descriptor) throws IOException;
    void offer(Descriptor descriptor, ApplyPermit permit) throws IOException;
    void withdraw(Descriptor descriptor, ApplyPermit permit);
    void bindWakeup(Runnable wakeup);
    void pump();
    void wakeup();
    void stop();
    void validateSegmentBytes(long bytes);
  }
  Admission committedApplyAdmission();
}
