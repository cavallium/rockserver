package org.apache.ratis.server.impl;

import java.io.Closeable;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Ownership injection only: no RaftServer, RPC service, thread pool or network is constructed. */
class RockserverRaftServersTest {
    @Test void successfulConstructionTransfersOwnershipWithoutClosing() throws Exception {
        var resource=new Resource();
        assertSame(resource,RockserverRaftServers.initialize(resource,owned -> owned.initialized=true));
        assertTrue(resource.initialized); assertEquals(0,resource.closes); resource.close(); assertEquals(1,resource.closes);
    }
    @Test void everyInitializationFailureClosesUnreturnedOwnerAndPreservesOriginalCause() {
        for (Throwable failure:new Throwable[]{new IOException("IO"),new IllegalStateException("runtime"),new AssertionError("error")}) {
            var resource=new Resource(); var actual=assertThrows(failure.getClass(),() -> RockserverRaftServers.initialize(resource,owned -> {
                if (failure instanceof IOException io) throw io;
                if (failure instanceof RuntimeException runtime) throw runtime;
                throw (Error)failure;
            }));
            assertSame(failure,actual); assertEquals(1,resource.closes);
        }
    }
    @Test void closeFailureIsSuppressedRatherThanLosingTheInitializationFailure() {
        var resource=new Resource(); resource.closeFailure=new IOException("cleanup"); var failure=new IOException("initialize");
        var actual=assertThrows(IOException.class,() -> RockserverRaftServers.initialize(resource,owned -> { throw failure; }));
        assertSame(failure,actual); assertArrayEquals(new Throwable[]{resource.closeFailure},actual.getSuppressed()); assertEquals(1,resource.closes);
    }
    static final class Resource implements Closeable {
        int closes; boolean initialized; IOException closeFailure;
        public void close() throws IOException { closes++; if (closeFailure!=null) throw closeFailure; }
    }
}
