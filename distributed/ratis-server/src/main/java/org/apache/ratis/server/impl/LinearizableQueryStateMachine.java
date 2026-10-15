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

import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.StateMachine;

/** Optional query hook invoked by the pinned server after its linearizable read barrier completes. */
public interface LinearizableQueryStateMachine extends StateMachine {
  /**
   * Whether this query requires execution on the actual ready leader, rather than a follower
   * that forwards ReadIndex. Called before the barrier; inspect only bounded cached/header state,
   * without IO, payload decoding or retaining request data. Owner-specific stale reads do not use it.
   */
  default boolean requiresLeaderForLinearizableQuery(Message request) { return false; }

  /** The server has obtained a fresh read index and waited for that index to be applied. */
  CompletableFuture<Message> queryLinearizable(Message request);
}
