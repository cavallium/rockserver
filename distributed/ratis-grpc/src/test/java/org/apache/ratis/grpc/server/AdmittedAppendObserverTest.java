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

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.LogResourceStateMachine;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.ProtoUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AdmittedAppendObserverTest {
  static final class Budget implements LogResourceStateMachine.Resources {
    int decode, intake, reply;
    int protectedIntake;
    boolean denyDecode, denyIntake;
    long maximumDecode;
    private LogResourceStateMachine.Lease lease(Runnable release) {
      return new LogResourceStateMachine.Lease() {
        boolean closed;
        @Override public void close() {
          assertFalse(closed, "lease released twice");
          closed = true;
          release.run();
        }
      };
    }
    @Override public LogResourceStateMachine.Lease reserveStream(){return ()->{};}
    @Override public LogResourceStateMachine.Lease reserveDecode(long maximumSerializedBytes) throws IOException {
      if (denyDecode) throw new IOException("decode full");
      maximumDecode = maximumSerializedBytes;
      decode++;
      return lease(() -> decode--);
    }
    @Override public LogResourceStateMachine.Lease reserveIntake(long serializedBytes, boolean heartbeat) throws IOException {
      if (denyIntake) throw new IOException("intake full");
      assertTrue(serializedBytes > 0);
      intake++;
      if (heartbeat) protectedIntake++;
      return lease(() -> intake--);
    }
    @Override public LogResourceStateMachine.AppendReservation reserveAppend(LogResourceStateMachine.AppendFootprint footprint) {
      throw new AssertionError("transport must not reserve append owners");
    }
    @Override public LogResourceStateMachine.Lease reserveResident(long bytes, long headers, long segments) {
      throw new AssertionError("transport must not reserve resident owners");
    }
    @Override public LogResourceStateMachine.Lease reserveRead(long bytes) {
      throw new AssertionError("transport must not reserve log reads");
    }
    @Override public LogResourceStateMachine.Lease reserveReply(boolean heartbeat) {
      reply++;
      return lease(() -> reply--);
    }
  }

  static final class Response extends ServerCallStreamObserver<AppendEntriesReplyProto> {
    int demand;
    boolean disabled, cancelled, completed;
    Runnable cancellation;
    Throwable failure;
    final List<Long> replies = new ArrayList<>();
    @Override public void disableAutoRequest() { disabled = true; }
    @Override public void disableAutoInboundFlowControl() { fail("must disable initial auto request too"); }
    @Override public void request(int count) { assertTrue(disabled); assertEquals(1, count); demand += count; }
    @Override public boolean isCancelled() { return cancelled; }
    @Override public void setOnCancelHandler(Runnable handler) { cancellation = handler; }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(Runnable handler) { }
    @Override public void setCompression(String compression) { }
    @Override public void setMessageCompression(boolean enabled) { }
    @Override public void onNext(AppendEntriesReplyProto reply) { replies.add(reply.getServerReply().getCallId()); }
    @Override public void onError(Throwable error) { failure = error; }
    @Override public void onCompleted() { completed = true; }
    void cancel() { cancelled = true; cancellation.run(); }
  }

  static final class Fixture implements AutoCloseable {
    final Budget budget = new Budget();
    final Response response = new Response();
    final RaftGroupId group = RaftGroupId.randomId();
    final Map<Long, CompletableFuture<AppendEntriesReplyProto>> actual = new HashMap<>();
    final LogResourceStateMachine.Registration registration;
    final GrpcServerProtocolService service;
    StreamObserver<AppendEntriesRequestProto> input;
    int invoked;
    Fixture() {
      var machine = (LogResourceStateMachine) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[]{LogResourceStateMachine.class, org.apache.ratis.statemachine.StateMachine.class}, (proxy, method, args) -> {
            if (method.getName().equals("logResources")) return budget;
            if (method.getName().equals("isProtectedLogEntry") || method.getName().equals("areProtectedLogEntries")) return false;
            throw new UnsupportedOperationException(method.getName());
          });
      var division = (RaftServer.Division) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[]{RaftServer.Division.class}, (proxy, method, args) -> {
            if (method.getName().equals("getStateMachine")) return machine;
            throw new UnsupportedOperationException(method.getName());
          });
      var server = (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{RaftServer.class},
          (proxy, method, args) -> switch (method.getName()) {
            case "getProperties" -> new RaftProperties();
            case "getDivision" -> { assertEquals(group, args[0]); yield division; }
            case "appendEntriesAsync" -> {
              invoked++;
              long call = ((AppendEntriesRequestProto) args[0]).getServerRequest().getCallId();
              yield actual.computeIfAbsent(call, key -> new CompletableFuture<>());
            }
            default -> throw new UnsupportedOperationException(method.getName());
          });
      registration = LogResourceStateMachine.bindServer(server, budget);
      service = new GrpcServerProtocolService(() -> RaftPeerId.valueOf("follower"), server);
    }
    void open() { input = service.appendEntries(response); }
    void send(long call, boolean heartbeat) {
      var request = AppendEntriesRequestProto.newBuilder().setServerRequest(RaftRpcRequestProto.newBuilder()
          .setCallId(call).setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(group)));
      if (!heartbeat) request.addEntries(LogEntryProto.newBuilder().setTerm(1).setIndex(call));
      try { service.admittedOnNext(input, request.build()); } catch(IOException denied) { response.onError(Status.RESOURCE_EXHAUSTED.withCause(denied).asRuntimeException()); }
    }
    void finish(long call) {
      actual.get(call).complete(AppendEntriesReplyProto.newBuilder().setServerReply(
          org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto.newBuilder().setCallId(call)).build());
    }
    @Override public void close() {
      if (input != null) input.onError(Status.CANCELLED.asRuntimeException());
      actual.values().forEach(future -> future.completeExceptionally(new IOException("test drain")));
      registration.close();
      assertEquals(0, budget.decode);
      assertEquals(0, budget.intake);
      assertEquals(0, budget.reply);
    }
  }

  @Test void idleDemandHoldsNoDecodeLeaseAndArrivingRejectedDecodeNeverEntersServer() {
    try (var f = new Fixture()) {
      f.budget.denyDecode = true;
      f.open();
      assertTrue(f.response.disabled);
      assertEquals(1, f.response.demand);
      assertEquals(0, f.budget.decode);
      f.send(1, true);
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, Status.fromThrowable(f.response.failure).getCode());
      assertEquals(0, f.invoked);
    }
  }

  @Test void rejectedIntakeNeverEntersServer() {
    try (var f = new Fixture()) {
      f.open();
      assertEquals(0, f.budget.decode);
      f.budget.denyIntake = true;
      f.send(1, false);
      assertEquals(0, f.invoked);
      assertEquals(0, f.budget.reply);
      assertEquals(0, f.budget.decode);
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, Status.fromThrowable(f.response.failure).getCode());
    }
  }

  @Test void completedPayloadIsReleasedWhileOrderedReplyWaits() {
    try (var f = new Fixture()) {
      f.open();
      f.send(1, false);
      f.send(2, false);
      assertEquals(2, f.budget.intake);
      f.finish(2);
      assertEquals(1, f.budget.intake);
      assertEquals(2, f.budget.reply);
      assertTrue(f.response.replies.isEmpty());
      f.finish(1);
      assertEquals(List.of(1L, 2L), f.response.replies);
      assertEquals(0, f.budget.intake);
      assertEquals(0, f.budget.reply);
    }
  }

  @Test void cancellationKeepsActualAppendOwnershipUntilCompletion() {
    try (var f = new Fixture()) {
      f.open();
      f.send(1, false);
      f.response.cancel();
      assertEquals(1, f.budget.intake);
      assertEquals(0, f.budget.decode);
      assertEquals(0, f.budget.reply);
      assertFalse(f.actual.get(1L).isCancelled());
      f.finish(1);
      assertEquals(0, f.budget.intake);
      assertTrue(f.response.replies.isEmpty());
    }
  }

  @Test void halfCloseDeliversOutstandingRepliesAndCompletes() {
    try (var f = new Fixture()) {
      f.open();
      f.send(1, false);
      f.input.onCompleted();
      assertEquals(0, f.budget.decode);
      assertFalse(f.response.completed);
      f.finish(1);
      assertEquals(List.of(1L), f.response.replies);
      assertTrue(f.response.completed);
    }
  }

  @Test void heartbeatProgressBypassesOrderedDataReply() {
    try (var f = new Fixture()) {
      f.open();
      f.send(1, false);
      f.send(2, true);
      assertEquals(1, f.budget.protectedIntake);
      f.finish(2);
      assertEquals(List.of(2L), f.response.replies);
      assertEquals(1, f.budget.intake);
      f.finish(1);
      assertEquals(List.of(2L, 1L), f.response.replies);
    }
  }

  @Test void failureBehindUnfinishedPredecessorTerminatesWithoutRefundingPredecessor() {
    try (var f = new Fixture()) {
      f.open();
      f.send(1, false);
      f.send(2, false);
      f.actual.get(2L).completeExceptionally(new IOException("second append failed"));
      assertNotNull(f.response.failure);
      assertEquals(1, f.budget.intake);
      assertEquals(0, f.budget.reply);
      assertEquals(0, f.budget.decode);
      f.finish(1);
      assertEquals(0, f.budget.intake);
      assertTrue(f.response.replies.isEmpty());
    }
  }

  @Test void failedActualAppendDrainsPayloadAndTerminatesWithoutReply() {
    try (var f = new Fixture()) {
      f.open();
      f.send(1, false);
      f.actual.get(1L).completeExceptionally(new IOException("append failed"));
      assertEquals(0, f.budget.intake);
      assertEquals(0, f.budget.reply);
      assertEquals(0, f.budget.decode);
      assertNotNull(f.response.failure);
      assertTrue(f.response.replies.isEmpty());
    }
  }
  @Test void boundMarshallerChargesArrivingFrameUntilActualDeliveryNotIdleDemand() throws Exception {
    try(var f=new Fixture()) {
      @SuppressWarnings("unchecked") var method=(org.apache.ratis.thirdparty.io.grpc.ServerMethodDefinition<GrpcServerProtocolService.DecodedAppend,AppendEntriesReplyProto>)f.service.bindService().getMethods().stream().filter(m->m.getMethodDescriptor().getFullMethodName().endsWith("/appendEntries")).findFirst().orElseThrow();
      var descriptor=method.getMethodDescriptor();
      class Call extends org.apache.ratis.thirdparty.io.grpc.ServerCall<GrpcServerProtocolService.DecodedAppend,AppendEntriesReplyProto> {
        int demand;
        public void request(int count){demand+=count;}
        public void sendHeaders(org.apache.ratis.thirdparty.io.grpc.Metadata headers){}
        public void sendMessage(AppendEntriesReplyProto reply){}
        public void close(Status status,org.apache.ratis.thirdparty.io.grpc.Metadata trailers){}
        public boolean isCancelled(){return false;}
        public org.apache.ratis.thirdparty.io.grpc.MethodDescriptor<GrpcServerProtocolService.DecodedAppend,AppendEntriesReplyProto> getMethodDescriptor(){return descriptor;}
      }
      var first=new Call();var second=new Call();
      var listener=method.getServerCallHandler().startCall(first,new org.apache.ratis.thirdparty.io.grpc.Metadata());
      var other=method.getServerCallHandler().startCall(second,new org.apache.ratis.thirdparty.io.grpc.Metadata());
      assertEquals(1,first.demand);assertEquals(1,second.demand);assertEquals(0,f.budget.decode);
      var request=AppendEntriesRequestProto.newBuilder().setServerRequest(RaftRpcRequestProto.newBuilder().setCallId(1).setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(f.group))).build();
      class Frame extends java.io.ByteArrayInputStream implements org.apache.ratis.thirdparty.io.grpc.KnownLength {Frame(byte[] bytes){super(bytes);}}
      var decoded=descriptor.parseRequest(new Frame(request.toByteArray()));
      assertEquals(1,f.budget.decode);assertEquals(request.getSerializedSize(),f.budget.maximumDecode);
      listener.onMessage(decoded);assertEquals(0,f.budget.decode);assertEquals(1,f.invoked);f.finish(1);
      listener.onCancel();other.onCancel();
    }
  }

}
