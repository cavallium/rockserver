package it.cavallium.rockserver.core.common.cdc;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * Wrapper for a CDC event that exposes an acknowledgment action.
 * Ack commits the event's sequence for the given subscription on the server,
 * enabling at-least-once processing with simple reactive composition.
 */
public record CDCEventAck(@NotNull CDCEvent event, @NotNull Supplier<Mono<Void>> acknowledger) {

    /**
     * Acknowledge successful processing of this event.
     * The returned Mono completes when the commit has been persisted on the server.
     */
    public Mono<Void> ack() { return acknowledger.get(); }
}
