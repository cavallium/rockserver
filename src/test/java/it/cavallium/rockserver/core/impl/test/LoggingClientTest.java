package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.client.LoggingClient;
import org.junit.jupiter.api.Test;

class LoggingClientTest {

    @Test
    void formatErrorMessageHandlesNullMessageAndMissingThrowable() {
        assertEquals("<missing error signal throwable>", LoggingClient.formatErrorMessage(null));
        assertEquals("boom", LoggingClient.formatErrorMessage(new RuntimeException("boom")));
        assertEquals("java.lang.RuntimeException", LoggingClient.formatErrorMessage(new RuntimeException()));
    }
}
