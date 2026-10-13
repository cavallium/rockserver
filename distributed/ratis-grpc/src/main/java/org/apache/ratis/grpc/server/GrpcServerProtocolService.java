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
package org.apache.ratis.grpc.server;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.impl.LogResourceStateMachine;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCallStreamObserver;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageOrBuilder;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.thirdparty.io.grpc.*;
import java.io.InputStream;
import org.apache.ratis.util.BatchLogger;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class GrpcServerProtocolService implements RaftServerProtocolServiceGrpc.AsyncService, BindableService {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolService.class);

  private static final ThreadLocal<Boolean> ADMITTED_DECODE = new ThreadLocal<>();
  record DecodedAppend(AppendEntriesRequestProto request, LogResourceStateMachine.Lease lease) {}

  @Override public ServerServiceDefinition bindService() {
    var original = RaftServerProtocolServiceGrpc.bindService(this);
    var builder = ServerServiceDefinition.builder(original.getServiceDescriptor().getName());
    for (var method : original.getMethods()) {
      if (method.getMethodDescriptor().getFullMethodName().equals(RaftServerProtocolServiceGrpc.getAppendEntriesMethod().getFullMethodName())) {
        @SuppressWarnings("unchecked") var append = (ServerMethodDefinition<AppendEntriesRequestProto, AppendEntriesReplyProto>)method;
        builder.addMethod(admittedAppendMethod(append));
      } else builder.addMethod(method);
    }
    return builder.build();
  }

  ServerMethodDefinition<DecodedAppend, AppendEntriesReplyProto> admittedAppendMethod(
      ServerMethodDefinition<AppendEntriesRequestProto, AppendEntriesReplyProto> original) {
    var descriptor = original.getMethodDescriptor();
    var marshaller = new MethodDescriptor.Marshaller<DecodedAppend>() {
      @Override public InputStream stream(DecodedAppend value) { return descriptor.getRequestMarshaller().stream(value.request()); }
      @Override public DecodedAppend parse(InputStream input) {
        var resources = LogResourceStateMachine.forServer(server);
        if (resources == null) return new DecodedAppend(descriptor.parseRequest(input), () -> {});
        long maximum = GrpcConfigKeys.messageSizeMax(server.getProperties(), LOG::debug).getSize();
        LogResourceStateMachine.Lease lease = null;
        try {
          long frameBytes = input instanceof KnownLength ? input.available() : maximum;
          if (frameBytes < 0 || frameBytes > maximum) throw new IOException("Append frame exceeds transport limit");
          lease = resources.reserveDecode(frameBytes);
          var request = descriptor.parseRequest(input);
          if (request.getSerializedSize() > frameBytes) throw new IOException("Append decoded beyond admitted frame");
          return new DecodedAppend(request, lease);
        } catch (Throwable failure) {
          if (lease != null) lease.close();
          throw Status.RESOURCE_EXHAUSTED.withDescription("Append decode unavailable").withCause(failure).asRuntimeException();
        }
      }
    };
    var admitted = descriptor.<DecodedAppend, AppendEntriesReplyProto>toBuilder(marshaller, descriptor.getResponseMarshaller()).build();
    return ServerMethodDefinition.create(admitted, (call, headers) -> {
      @SuppressWarnings({"rawtypes", "unchecked"}) ServerCall<AppendEntriesRequestProto, AppendEntriesReplyProto> forwarded = (ServerCall)call;
      var listener = original.getServerCallHandler().startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(forwarded) {
        @Override public MethodDescriptor<AppendEntriesRequestProto, AppendEntriesReplyProto> getMethodDescriptor() { return descriptor; }
      }, headers);
      return new ServerCall.Listener<DecodedAppend>() {
        @Override public void onMessage(DecodedAppend value) {
          var prior = ADMITTED_DECODE.get();
          ADMITTED_DECODE.set(true);
          try { listener.onMessage(value.request()); }
          finally { if (prior == null) ADMITTED_DECODE.remove(); else ADMITTED_DECODE.set(prior); value.lease().close(); }
        }
        @Override public void onHalfClose() { listener.onHalfClose(); }
        @Override public void onCancel() { listener.onCancel(); }
        @Override public void onComplete() { listener.onComplete(); }
        @Override public void onReady() { listener.onReady(); }
      };
    });
  }

  // Socket-free fixtures exercise exactly the same parse/delivery ownership as the bound method.
  void admittedOnNext(StreamObserver<AppendEntriesRequestProto> listener, AppendEntriesRequestProto request) throws IOException {
    try (var lease = LogResourceStateMachine.forServer(server).reserveDecode(request.getSerializedSize())) {
      var prior = ADMITTED_DECODE.get(); ADMITTED_DECODE.set(true);
      try { listener.onNext(request); } finally { if (prior == null) ADMITTED_DECODE.remove(); else ADMITTED_DECODE.set(prior); }
    }
  }

  private enum BatchLogKey implements BatchLogger.Key {
    COMPLETED_REQUEST,
    COMPLETED_REPLY
  }

  static class PendingServerRequest<REQUEST> {
    private final REQUEST request;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    PendingServerRequest(REQUEST request) {
      this.request = request;
    }

    REQUEST getRequest() {
      return request;
    }

    CompletableFuture<Void> getFuture() {
      return future;
    }
  }

  abstract class ServerRequestStreamObserver<REQUEST, REPLY extends MessageOrBuilder>
      implements StreamObserver<REQUEST> {
    private final RaftServer.Op op;
    private final Supplier<String> nameSupplier;
    private final StreamObserver<REPLY> responseObserver;
    /** For ordered {@link #onNext(Object)} requests. */
    private final AtomicReference<PendingServerRequest<REQUEST>> previousOnNext = new AtomicReference<>();
    /** For both ordered and unordered {@link #onNext(Object)} requests. */
    private final AtomicReference<CompletableFuture<REPLY>> requestFuture
        = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    ServerRequestStreamObserver(RaftServer.Op op, StreamObserver<REPLY> responseObserver) {
      this.op = op;
      this.nameSupplier = MemoizedSupplier.valueOf(() -> getId() + "_" + op);
      this.responseObserver = responseObserver;
    }

    String getName() {
      return nameSupplier.get();
    }

    private String getPreviousRequestString() {
      return Optional.ofNullable(previousOnNext.get())
          .map(PendingServerRequest::getRequest)
          .map(this::requestToString)
          .orElse(null);
    }

    abstract CompletableFuture<REPLY> process(REQUEST request) throws IOException;

    abstract long getCallId(REQUEST request);

    abstract String requestToString(REQUEST request);

    abstract String replyToString(REPLY reply);

    abstract boolean replyInOrder(REQUEST request);

    StatusRuntimeException wrapException(Throwable e, REQUEST request) {
      return GrpcUtil.wrapException(e, getCallId(request));
    }

    private void handleError(Throwable e, REQUEST request) {
      GrpcUtil.warn(LOG, () -> getId() + ": Failed " + op + " request " + requestToString(request), e);
      if (isClosed.compareAndSet(false, true)) {
        responseObserver.onError(wrapException(e, request));
      }
    }

    private synchronized REPLY handleReply(REPLY reply) {
      if (!isClosed.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: reply {}", getId(), replyToString(reply));
        }
        responseObserver.onNext(reply);
      }
      return reply;
    }

    void composeRequest(CompletableFuture<REPLY> current) {
      requestFuture.updateAndGet(previous -> previous.thenCompose(reply -> current));
    }

    @Override
    public void onNext(REQUEST request) {
      if (!replyInOrder(request)) {
        try {
          composeRequest(process(request).thenApply(this::handleReply));
        } catch (Exception e) {
          handleError(e, request);
        }
        return;
      }

      final PendingServerRequest<REQUEST> current = new PendingServerRequest<>(request);
      final PendingServerRequest<REQUEST> previous = previousOnNext.getAndSet(current);
      final CompletableFuture<Void> previousFuture = Optional.ofNullable(previous)
          .map(PendingServerRequest::getFuture)
          .orElse(CompletableFuture.completedFuture(null));
      try {
        final CompletableFuture<REPLY> f = process(request).exceptionally(e -> {
          // Handle cases, such as RaftServer is paused
          handleError(e, request);
          current.getFuture().completeExceptionally(e);
          return null;
        }).thenCombine(previousFuture, (reply, v) -> {
          handleReply(reply);
          current.getFuture().complete(null);
          return null;
        });
        composeRequest(f);
      } catch (Exception e) {
        handleError(e, request);
        current.getFuture().completeExceptionally(e);
      }
    }

    @Override
    public void onCompleted() {
      if (isClosed.compareAndSet(false, true)) {
        BatchLogger.print(BatchLogKey.COMPLETED_REQUEST, getName(),
            suffix -> LOG.info("{}: Completed {}, lastRequest: {} {}",
                getId(), op, getPreviousRequestString(), suffix));
        requestFuture.get().thenAccept(reply -> {
          BatchLogger.print(BatchLogKey.COMPLETED_REPLY, getName(),
              suffix -> LOG.info("{}: Completed {}, lastReply: {} {}",
                  getId(), op, ProtoUtils.shortDebugString(reply), suffix));
          responseObserver.onCompleted();
        });
      }
    }
    @Override
    public void onError(Throwable t) {
      GrpcUtil.warn(LOG, () -> getId() + ": "+ op + " onError, lastRequest: " + getPreviousRequestString(), t);
      if (isClosed.compareAndSet(false, true)) {
        Status status = Status.fromThrowable(t);
        if (status != null && status.getCode() != Status.Code.CANCELLED) {
          responseObserver.onCompleted();
        }
      }
    }
  }

  private final Supplier<RaftPeerId> idSupplier;
  private final RaftServer server;

  GrpcServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server) {
    this.idSupplier = idSupplier;
    this.server = server;
  }

  RaftPeerId getId() {
    return idSupplier.get();
  }

  @Override
  public void requestVote(RequestVoteRequestProto request,
      StreamObserver<RequestVoteReplyProto> responseObserver) {
    try {
      final RequestVoteReplyProto reply = server.requestVote(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      GrpcUtil.warn(LOG, () -> getId() + ": Failed requestVote " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public void startLeaderElection(StartLeaderElectionRequestProto request,
      StreamObserver<StartLeaderElectionReplyProto> responseObserver) {
    try {
      final StartLeaderElectionReplyProto reply = server.startLeaderElection(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Throwable e) {
      GrpcUtil.warn(LOG,
          () -> getId() + ": Failed startLeaderElection " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public void readIndex(ReadIndexRequestProto request, StreamObserver<ReadIndexReplyProto> responseObserver) {
    final Consumer<Throwable> warning = e -> GrpcUtil.warn(LOG,
            () -> getId() + ": Failed readIndex " + ProtoUtils.toString(request.getServerRequest()), e);
    GrpcUtil.asyncCall(responseObserver, () -> server.readIndexAsync(request), Function.identity(), warning);
  }

  @Override
  public StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseObserver) {
    final LogResourceStateMachine.Resources resources = LogResourceStateMachine.forServer(server);
    if (resources != null) {
      if (!(responseObserver instanceof ServerCallStreamObserver<?>)) {
        throw new IllegalArgumentException("Budgeted append requires transport flow control");
      }
      return new AdmittedAppendObserver(resources,
          (ServerCallStreamObserver<AppendEntriesReplyProto>) responseObserver);
    }
    return new ServerRequestStreamObserver<AppendEntriesRequestProto, AppendEntriesReplyProto>(
        RaftServerProtocol.Op.APPEND_ENTRIES, responseObserver) {
      @Override
      CompletableFuture<AppendEntriesReplyProto> process(AppendEntriesRequestProto request) throws IOException {
        return server.appendEntriesAsync(request);
      }

      @Override
      long getCallId(AppendEntriesRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      String requestToString(AppendEntriesRequestProto request) {
        return ServerStringUtils.toAppendEntriesRequestString(request, null);
      }

      @Override
      String replyToString(AppendEntriesReplyProto reply) {
        return ServerStringUtils.toAppendEntriesReplyString(reply);
      }

      @Override
      boolean replyInOrder(AppendEntriesRequestProto request) {
        return request.getEntriesCount() != 0;
      }

      @Override
      StatusRuntimeException wrapException(Throwable e, AppendEntriesRequestProto request) {
        return GrpcUtil.wrapException(e, getCallId(request), request.getEntriesCount() == 0);
      }
    };
  }

  /** Demand owns the maximum decode graph; ordered continuations own only bounded reply metadata. */
  final class AdmittedAppendObserver implements StreamObserver<AppendEntriesRequestProto> {
    final class PendingReply {
      final long callId;
      final boolean heartbeat;
      final CompletableFuture<Void> delivered = new CompletableFuture<>();
      final AtomicReference<LogResourceStateMachine.Lease> lease;
      PendingReply(long callId, boolean heartbeat, LogResourceStateMachine.Lease lease) {
        this.callId = callId;
        this.heartbeat = heartbeat;
        this.lease = new AtomicReference<>(lease);
      }
      void release() {
        final LogResourceStateMachine.Lease held = lease.getAndSet(null);
        if (held != null) {
          held.close();
        }
        pending.remove(this);
      }
    }

    private final LogResourceStateMachine.Resources resources;
    private final ServerCallStreamObserver<AppendEntriesReplyProto> response;
    private final long maximumBytes;
    private final AtomicReference<LogResourceStateMachine.Lease> streamOwner = new AtomicReference<>();
    private final AtomicBoolean demand = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean inputCompleted = new AtomicBoolean();
    private final Set<PendingReply> pending = ConcurrentHashMap.newKeySet();
    private CompletableFuture<Void> previousOrdered = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> allDelivered = CompletableFuture.completedFuture(null);

    AdmittedAppendObserver(LogResourceStateMachine.Resources resources,
        ServerCallStreamObserver<AppendEntriesReplyProto> response) {
      this.resources = resources;
      this.response = response;
      maximumBytes = GrpcConfigKeys.messageSizeMax(server.getProperties(), LOG::debug).getSize();
      response.disableAutoRequest();
      response.setOnCancelHandler(this::cancel);
      try { streamOwner.set(resources.reserveStream()); requestNext(); }
      catch (IOException unavailable) { fail(unavailable); }
    }

    private void releaseStream() { var held=streamOwner.getAndSet(null); if(held!=null)held.close(); }
    private void releaseDecode() { demand.set(false); }

    private void requestNext() {
      if (closed.get() || inputCompleted.get()) return;
      if (!demand.compareAndSet(false, true)) throw new IllegalStateException("Append demand already outstanding");
      if (closed.get() || inputCompleted.get() || response.isCancelled()) { releaseDecode(); return; }
      response.request(1);
    }

    private void terminate() {
      releaseDecode();
      releaseStream();
      for (PendingReply reply : pending) {
        reply.release();
        reply.delivered.complete(null);
      }
    }

    private void cancel() {
      if (closed.compareAndSet(false, true)) {
        terminate();
      }
    }

    private synchronized void fail(Throwable failure) {
      if (closed.compareAndSet(false, true)) {
        terminate();
        response.onError(Status.RESOURCE_EXHAUSTED.withDescription("Append intake unavailable")
            .withCause(failure).asRuntimeException());
      }
    }

    private synchronized void deliver(PendingReply pendingReply, AppendEntriesReplyProto reply, Throwable failure) {
      try {
        if (!closed.get()) {
          if (failure != null) {
            if (closed.compareAndSet(false, true)) {
              terminate();
              response.onError(GrpcUtil.wrapException(failure, pendingReply.callId, pendingReply.heartbeat));
            }
          } else {
            response.onNext(reply);
          }
        }
      } catch (Exception e) {
        cancel();
      } finally {
        pendingReply.release();
        pendingReply.delivered.complete(null);
      }
    }

    @Override
    public void onNext(AppendEntriesRequestProto request) {
      final boolean decoded = demand.getAndSet(false) && Boolean.TRUE.equals(ADMITTED_DECODE.get());
      if (closed.get() || inputCompleted.get()) {
        return;
      }
      LogResourceStateMachine.Lease replyLease = null;
      LogResourceStateMachine.Lease intake = null;
      try {
        if (!decoded || request.getSerializedSize() > maximumBytes) {
          throw new IOException("Append without bounded decode demand");
        }
        final var machine = server.getDivision(ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId()))
            .getStateMachine();
        if (!(machine instanceof LogResourceStateMachine)) {
          throw new IOException("Append group has no log resource admission");
        }
        final LogResourceStateMachine.Resources groupResources = ((LogResourceStateMachine) machine).logResources();
        final boolean heartbeat = request.getEntriesCount() == 0;
        boolean protectedControl = !heartbeat && ((LogResourceStateMachine)machine).areProtectedLogEntries(request.getEntriesList());
        replyLease = groupResources.reserveReply(heartbeat || protectedControl);
        intake = protectedControl ? groupResources.reserveControlIntake(request.getSerializedSize()) : groupResources.reserveIntake(request.getSerializedSize(), heartbeat);
        final PendingReply current = new PendingReply(request.getServerRequest().getCallId(), heartbeat, replyLease);
        replyLease = null;
        pending.add(current);
        if (closed.get()) {
          current.release();
          current.delivered.complete(null);
          return;
        }
        final CompletableFuture<Void> previous = heartbeat ? CompletableFuture.completedFuture(null) : previousOrdered;
        if (!heartbeat) {
          previousOrdered = current.delivered;
        }
        allDelivered = allDelivered.thenCombine(current.delivered, (a, b) -> null);
        final CompletableFuture<AppendEntriesReplyProto> actual;
        try {
          actual = server.appendEntriesAsync(request);
        } catch (Exception e) {
          deliver(current, null, e);
          return;
        }
        final LogResourceStateMachine.Lease actualIntake = intake;
        intake = null;
        // No request/Pending payload is captured here. Cancellation never refunds actual work.
        actual.whenComplete((reply, failure) -> {
          actualIntake.close();
          if (failure != null) {
            deliver(current, null, failure);
          } else {
            previous.whenComplete((v, ignored) -> deliver(current, reply, null));
          }
        });
      } catch (Exception e) {
        fail(e);
      } finally {
        if (replyLease != null) {
          replyLease.close();
        }
        if (intake != null) {
          intake.close();
        }
      }
      requestNext();
    }

    @Override
    public void onCompleted() {
      inputCompleted.set(true);
      releaseDecode();
      allDelivered.thenRun(this::completeReplyStream);
    }

    private synchronized void completeReplyStream() {
      if (closed.compareAndSet(false, true)) {
        releaseStream();
        response.onCompleted();
      }
    }

    @Override
    public void onError(Throwable failure) {
      cancel();
    }
  }

  @Override
  public StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseObserver) {
    return new ServerRequestStreamObserver<InstallSnapshotRequestProto, InstallSnapshotReplyProto>(
        RaftServerProtocol.Op.INSTALL_SNAPSHOT, responseObserver) {
      @Override
      CompletableFuture<InstallSnapshotReplyProto> process(InstallSnapshotRequestProto request) throws IOException {
        return CompletableFuture.completedFuture(server.installSnapshot(request));
      }

      @Override
      long getCallId(InstallSnapshotRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      String requestToString(InstallSnapshotRequestProto request) {
        return ServerStringUtils.toInstallSnapshotRequestString(request);
      }

      @Override
      String replyToString(InstallSnapshotReplyProto reply) {
        return ServerStringUtils.toInstallSnapshotReplyString(reply);
      }

      @Override
      boolean replyInOrder(InstallSnapshotRequestProto installSnapshotRequestProto) {
        return true;
      }
    };
  }
}
