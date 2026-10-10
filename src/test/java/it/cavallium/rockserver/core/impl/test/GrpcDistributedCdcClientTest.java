package it.cavallium.rockserver.core.impl.test;

import io.grpc.*;
import it.cavallium.rockserver.core.client.GrpcDistributedCdcClient;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.cdc.proto.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.server.DistributedCdcGrpcService;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Stub dispatches directly into the Reactor service; no server, channel transport or sockets. */
class GrpcDistributedCdcClientTest {
    static final class DirectChannel extends Channel {
        final DistributedCdcGrpcService service;
        CallOptions lastOptions; Object lastRequest;
        DirectChannel(DistributedCdcAsyncAPI backend) { service=new DistributedCdcGrpcService(backend); }
        public String authority() { return "direct-fixture"; }
        public <Q,R> ClientCall<Q,R> newCall(MethodDescriptor<Q,R> descriptor,CallOptions options) {
            lastOptions=options;
            return new ClientCall<>() {
                Listener<R> listener; Q request; Disposable observation; boolean ended;
                public void start(Listener<R> listener,Metadata headers) { this.listener=listener; }
                public void request(int count) {}
                public void sendMessage(Q request) { this.request=request; lastRequest=request; }
                @SuppressWarnings("unchecked") public void halfClose() {
                    Mono<?> response=switch(descriptor.getFullMethodName().substring(descriptor.getFullMethodName().lastIndexOf('/')+1)) {
                        case "Context" -> service.context((CdcContextRequest)request);
                        case "Create" -> service.create((CdcCreateRequest)request);
                        case "Recover" -> service.recover((CdcSubscriptionRequest)request);
                        case "Status" -> service.status((CdcSubscriptionRequest)request);
                        case "Poll" -> service.poll((CdcPollRequest)request);
                        case "Acknowledge" -> service.acknowledge((CdcAcknowledgeRequest)request);
                        case "CloseSubscription" -> service.closeSubscription((CdcSubscriptionRequest)request);
                        default -> throw new AssertionError(descriptor.getFullMethodName());
                    };
                    listener.onHeaders(new Metadata()); observation=response.subscribe(value->{ if(!ended) listener.onMessage((R)value); },error->{ if(!ended) { ended=true; listener.onClose(Status.fromThrowable(error),new Metadata()); } },()->{ if(!ended) { ended=true; listener.onClose(Status.OK,new Metadata()); } });
                }
                public void cancel(String message,Throwable cause) { if(observation!=null) observation.dispose(); if(!ended) { ended=true; listener.onClose(Status.CANCELLED.withDescription(message),new Metadata()); } }
            };
        }
    }
    DistributedCdcSubscription.Definition definition() {
        var lineage=new DistributedCdcCheckpoint.Lineage(new UUID(0,4),new UUID(0,5),1);
        return new DistributedCdcSubscription.Definition(new UUID(0,1),new UUID(0,2),new UUID(0,3),new UUID(0,6),new UUID(0,7),new byte[32],List.of(new UUID(0,10)),new DistributedCdcCheckpoint(Map.of(lineage,DistributedCdcCheckpoint.Cursor.beginning())));
    }
    @Test void mandatoryContextDeadlineAndExactStatusIdentityUseGeneratedStub() {
        var backend=mock(DistributedCdcAsyncAPI.class); var definition=definition(); var status=new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition),DistributedCdcSubscriptions.Phase.ACTIVE);
        when(backend.status(eq(definition.subscriptionId()),any())).thenReturn(CompletableFuture.completedFuture(status));
        var channel=new DirectChannel(backend); var client=new GrpcDistributedCdcClient(channel);
        assertEquals(status,client.status(definition.subscriptionId(),RequestContext.latency(Duration.ofSeconds(5))).join());
        assertNotNull(channel.lastOptions.getDeadline()); var request=(CdcSubscriptionRequest)channel.lastRequest; assertTrue(request.hasContext()); assertTrue(request.getContext().getTimeoutNanos()>0 && request.getContext().getTimeoutNanos()<=TimeUnit.SECONDS.toNanos(5));
        assertEquals(DistributedCdcWire.MAX_FRAME_BYTES,channel.lastOptions.getMaxInboundMessageSize());
        var calls=mockingDetails(backend).getInvocations().size(); assertTrue(client.status(definition.subscriptionId(),null).isCompletedExceptionally()); assertEquals(calls,mockingDetails(backend).getInvocations().size());
    }
    @Test void cancellingClientObservationDoesNotCancelBackendCreateOrCloseSubscription() {
        var backend=mock(DistributedCdcAsyncAPI.class); var definition=definition(); var pending=new CompletableFuture<DistributedCdcSubscriptions.Status>(); when(backend.create(eq(definition),any())).thenReturn(pending);
        var client=new GrpcDistributedCdcClient(new DirectChannel(backend)); var observation=client.create(definition,RequestContext.batch()); assertTrue(observation.cancel(false)); assertFalse(pending.isCancelled());
        pending.complete(new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition),DistributedCdcSubscriptions.Phase.ACTIVE)); verify(backend,never()).closeSubscription(any(),any());
    }
    @Test void recoverAckAndCloseHaveExplicitDistinctBackendCalls() {
        var backend=mock(DistributedCdcAsyncAPI.class); var definition=definition(); var active=new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition),DistributedCdcSubscriptions.Phase.ACTIVE);
        var closed=new DistributedCdcSubscriptions.Status(active.state().close(),DistributedCdcSubscriptions.Phase.CLOSED); UUID batch=new UUID(0,8);
        when(backend.recover(eq(definition.subscriptionId()),any())).thenReturn(CompletableFuture.completedFuture(active)); when(backend.acknowledge(eq(definition.subscriptionId()),eq(batch),any())).thenReturn(CompletableFuture.completedFuture(active)); when(backend.closeSubscription(eq(definition.subscriptionId()),any())).thenReturn(CompletableFuture.completedFuture(closed));
        var channel=new DirectChannel(backend); var client=new GrpcDistributedCdcClient(channel);
        assertEquals(active,client.recover(definition.subscriptionId(),RequestContext.batch()).join()); verify(backend,never()).acknowledge(any(),any(),any());
        assertEquals(active,client.acknowledge(definition.subscriptionId(),batch,RequestContext.batch()).join()); assertEquals(DistributedCdcWire.uuid(batch),((CdcAcknowledgeRequest)channel.lastRequest).getBatchId());
        assertEquals(closed,client.closeSubscription(definition.subscriptionId(),RequestContext.batch()).join()); verify(backend).closeSubscription(eq(definition.subscriptionId()),any());
    }
}
