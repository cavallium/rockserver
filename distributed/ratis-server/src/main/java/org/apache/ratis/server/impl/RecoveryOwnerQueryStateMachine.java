package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.StateMachine;

/** Optional owner-local recovery evidence that does not require any committed log entry. */
public interface RecoveryOwnerQueryStateMachine extends StateMachine {
  /**
   * Bounded request classification only: no IO, allocation of request bodies, or readiness publication.
   * Core may route this request to queryStale at minimum index zero even on an empty log.
   * The actual query must qualify its owner and durable evidence; this grants no read-index freshness,
   * log progress, snapshot installation, voting, public DATA or absent-outcome authority.
   */
  boolean isRecoveryOwnerQuery(Message request);
}
