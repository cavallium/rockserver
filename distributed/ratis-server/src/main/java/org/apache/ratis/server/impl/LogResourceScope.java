/* Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
 * See http://www.apache.org/licenses/LICENSE-2.0. Provided AS IS without warranties. */
package org.apache.ratis.server.impl;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/** Thread-confined handoff from pre-effect append reservation to exact cache and actual I/O owners. */
public final class LogResourceScope implements AutoCloseable {
  private static final ThreadLocal<LogResourceScope> CURRENT=new ThreadLocal<>();
  private static final ThreadLocal<Integer> ADMITTED_READ = new ThreadLocal<>();

  /** Marked input buffer, checksum frame and parsed proto, plus bounded framing/parser overhead. */
  public static long coldReadBytes(long maximumEntryBytes) {
    return Math.addExact(Math.multiplyExact(3, maximumEntryBytes), 65536);
  }

  /** The caller retains its dispatcher read/execution permit through parser and proto ownership. */
  public static AutoCloseableRead admittedRead() { return new AutoCloseableRead(); }
  public static boolean hasAdmittedRead() { return ADMITTED_READ.get() != null; }
  public static final class AutoCloseableRead implements AutoCloseable {
    private final Thread owner = Thread.currentThread();
    private final Integer previous = ADMITTED_READ.get();
    private final int depth = previous == null ? 1 : previous + 1;
    private boolean closed;
    private AutoCloseableRead() { ADMITTED_READ.set(depth); }
    @Override public void close() {
      if (closed || Thread.currentThread() != owner || !Integer.valueOf(depth).equals(ADMITTED_READ.get()))
        throw new IllegalStateException("Admitted-read scope nesting changed");
      closed = true;
      if (previous == null) ADMITTED_READ.remove(); else ADMITTED_READ.set(previous);
    }
  }
  private final LogResourceScope previous;
  private final LogResourceStateMachine.ResidentReservation reservation;
  private final ArrayList<CompletableFuture<?>> actual;
  private LogResourceScope(LogResourceStateMachine.ResidentReservation reservation) {
    previous=CURRENT.get(); this.reservation=reservation;
    actual=previous!=null && previous.reservation==reservation ? previous.actual : new ArrayList<>(); CURRENT.set(this);
  }
  public static LogResourceScope enter(LogResourceStateMachine.ResidentReservation reservation) { return new LogResourceScope(reservation); }
  public static LogResourceStateMachine.AppendReservation currentReservation() { var scope=CURRENT.get(); return scope!=null && scope.reservation instanceof LogResourceStateMachine.AppendReservation append ? append : null; }
  public static LogResourceStateMachine.ResidentReservation currentResident() { var scope=CURRENT.get(); return scope==null ? null : scope.reservation; }
  public static void track(CompletableFuture<?> future) { var scope=CURRENT.get(); if(scope!=null)scope.actual.add(future); }
  public CompletableFuture<Void> actualDrain() { return CompletableFuture.allOf(actual.stream().map(future->future.handle((value,error)->null)).toArray(CompletableFuture[]::new)); }
  @Override public void close() { if(CURRENT.get()!=this)throw new IllegalStateException("Log-resource scope nesting changed"); if(previous==null)CURRENT.remove(); else CURRENT.set(previous); }
}
