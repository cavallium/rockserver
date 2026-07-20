package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPIRequestHandler;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPIRequestHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class RocksDBAPIRequestHandlerTest {

    private final RocksDBSyncAPIRequestHandler syncHandler = new RocksDBSyncAPIRequestHandler() {
    };
    private final RocksDBAsyncAPIRequestHandler asyncHandler = new RocksDBAsyncAPIRequestHandler() {
    };

    @Test
    void defaultSyncRequestHandlerReturnsPublicNotImplementedError() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction(1_000);

        var failure = assertThrows(RocksDBException.class, () -> syncHandler.requestSync(command));

        assertUnsupportedRequest(failure, command);
    }

    @Test
    void defaultAsyncSingleRequestHandlerReturnsPublicNotImplementedError() throws Exception {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction(1_000);

        var future = asyncHandler.requestAsync(command);
        var failure = assertThrows(ExecutionException.class, future::get);

        assertUnsupportedRequest(assertInstanceOf(RocksDBException.class, failure.getCause()), command);
    }

    @Test
    void defaultAsyncStreamRequestHandlerReturnsPublicNotImplementedError() {
        var command = new RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw(4, 0, 1);

        var publisher = asyncHandler.requestAsync(command);

        StepVerifier.create(Flux.from(publisher))
                .expectErrorSatisfies(failure ->
                        assertUnsupportedRequest(assertInstanceOf(RocksDBException.class, failure), command))
                .verify();
    }

    @Test
    void defaultAsyncStreamRequestHandlerHonorsCancelDuringSubscribe() {
        var command = new RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw(4, 0, 1);
        var publisher = asyncHandler.requestAsync(command);
        var subscribed = new AtomicBoolean();
        var valueReceived = new AtomicBoolean();
        var terminalReceived = new AtomicBoolean();

        publisher.subscribe(new Subscriber<Object>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscribed.set(true);
                subscription.cancel();
            }

            @Override
            public void onNext(Object value) {
                valueReceived.set(true);
            }

            @Override
            public void onError(Throwable throwable) {
                terminalReceived.set(true);
            }

            @Override
            public void onComplete() {
                terminalReceived.set(true);
            }
        });

        assertTrue(subscribed.get());
        assertFalse(valueReceived.get());
        assertFalse(terminalReceived.get());
    }

    private static void assertUnsupportedRequest(RocksDBException failure,
                                                  RocksDBAPICommand<?, ?, ?> command) {
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, failure.getErrorUniqueId());
        assertEquals("Unsupported request type: " + command, failure.getMessage());
    }
}
