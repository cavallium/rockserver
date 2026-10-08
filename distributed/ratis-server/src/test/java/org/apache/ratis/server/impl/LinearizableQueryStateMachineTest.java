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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the exact production dispatch/barrier seams without a server, RPC or thread pool. */
class LinearizableQueryStateMachineTest {
  private static final Message REQUEST = Message.valueOf("request");

  @Test void ordinaryReadsNeverUseLinearizableHook() {
    var stateMachine = new Queries();
    assertSame(stateMachine.ordinaryReply,
        RaftServerImpl.queryStateMachine(stateMachine, REQUEST, false).join());
    assertEquals(1, stateMachine.ordinary);
    assertEquals(0, stateMachine.linearizable);
  }

  @Test void linearizableReadsFallbackForUnextendedStateMachines() {
    var stateMachine = new OrdinaryQueries();
    assertSame(stateMachine.reply,
        RaftServerImpl.queryStateMachine(stateMachine, REQUEST, true).join());
    assertEquals(1, stateMachine.queries);
  }

  @Test void linearizableHookWaitsForBothReadIndexAndApply() {
    var stateMachine = new Queries();
    var readIndex = new CompletableFuture<Long>();
    var applied = new CompletableFuture<Long>();
    var result = RaftServerImpl.afterReadBarrier(readIndex, index -> {
      assertEquals(42L, index.longValue());
      return applied;
    }, () -> RaftServerImpl.queryStateMachine(stateMachine, REQUEST, true));
    assertFalse(result.isDone());
    assertEquals(0, stateMachine.linearizable);
    readIndex.complete(42L);
    assertFalse(result.isDone());
    assertEquals(0, stateMachine.linearizable);
    applied.complete(42L);
    assertSame(stateMachine.linearizableReply, result.join());
    assertEquals(1, stateMachine.linearizable);
    assertEquals(0, stateMachine.ordinary);
  }

  @Test void failedReadIndexNeverWaitsOrQueries() {
    var stateMachine = new Queries();
    var failure = new IOException("read index failed");
    var result = RaftServerImpl.afterReadBarrier(CompletableFuture.<Long>failedFuture(failure),
        index -> { fail("must not wait after read-index failure"); return null; },
        () -> RaftServerImpl.queryStateMachine(stateMachine, REQUEST, true));
    assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
    assertEquals(0, stateMachine.linearizable);
    assertEquals(0, stateMachine.ordinary);
  }

  @Test void failedApplyBarrierNeverQueries() {
    var stateMachine = new Queries();
    var failure = new IOException("apply wait failed");
    var applied = new CompletableFuture<Long>();
    var result = RaftServerImpl.afterReadBarrier(CompletableFuture.completedFuture(42L), index -> applied,
        () -> RaftServerImpl.queryStateMachine(stateMachine, REQUEST, true));
    applied.completeExceptionally(failure);
    assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
    assertEquals(0, stateMachine.linearizable);
    assertEquals(0, stateMachine.ordinary);
  }

  private static final class OrdinaryQueries extends BaseStateMachine {
    final Message reply = Message.valueOf("ordinary");
    int queries;
    @Override public CompletableFuture<Message> query(Message request) {
      assertSame(REQUEST, request);
      queries++;
      return CompletableFuture.completedFuture(reply);
    }
  }

  private static final class Queries extends BaseStateMachine implements LinearizableQueryStateMachine {
    final Message ordinaryReply = Message.valueOf("ordinary");
    final Message linearizableReply = Message.valueOf("linearizable");
    int ordinary;
    int linearizable;
    @Override public CompletableFuture<Message> query(Message request) {
      assertSame(REQUEST, request);
      ordinary++;
      return CompletableFuture.completedFuture(ordinaryReply);
    }
    @Override public CompletableFuture<Message> queryLinearizable(Message request) {
      assertSame(REQUEST, request);
      linearizable++;
      return CompletableFuture.completedFuture(linearizableReply);
    }
  }
}
