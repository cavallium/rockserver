package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.LoggingClient;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.replication.ratis.RatisServerLifecycle;

import java.io.Closeable;
import java.io.IOException;

import org.jetbrains.annotations.Nullable;

public abstract class Server implements Closeable {

    private final RocksDBConnection client;
    private final RatisServerLifecycle ratisLifecycle;
    private boolean ratisLifecycleStarted;
    private boolean ratisLifecycleClosed;

    public Server(RocksDBConnection client) {
        this(client, null);
    }

    public Server(RocksDBConnection client, @Nullable RatisServerLifecycle ratisLifecycle) {
        this.client = new LoggingClient(requireArgument(client, "client"));
        this.ratisLifecycle = ratisLifecycle;
    }

    public RocksDBConnection getClient() {
        return client;
    }

    public abstract void start() throws IOException;

    protected final synchronized void startRatisLifecycle() throws IOException {
        if (ratisLifecycle == null) {
            return;
        }
        if (ratisLifecycleClosed) {
            throw new IllegalStateException("Ratis lifecycle is closed");
        }
        if (ratisLifecycleStarted) {
            return;
        }
        ratisLifecycle.start();
        ratisLifecycleStarted = true;
    }

    protected final synchronized void closeRatisLifecycle() throws IOException {
        if (ratisLifecycle == null || ratisLifecycleClosed) {
            return;
        }
        ratisLifecycle.close();
        ratisLifecycleClosed = true;
    }

    protected final void startRatisThenPublicEndpoint(EndpointStartAction endpointStart,
                                                       EndpointCloseAction endpointClose) throws IOException {
        requireArgument(endpointStart, "endpointStart");
        requireArgument(endpointClose, "endpointClose");
        try {
            startRatisLifecycle();
            endpointStart.start();
        } catch (IOException | RuntimeException | Error e) {
            closePublicEndpointThenRatisAfterStartFailure(endpointClose, e);
            throw e;
        }
    }

    protected final void closePublicEndpointThenRatis(EndpointCloseAction endpointClose) throws IOException {
        requireArgument(endpointClose, "endpointClose");
        Throwable failure = null;
        try {
            endpointClose.close();
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
        }
        try {
            closeRatisLifecycle();
        } catch (IOException | RuntimeException | Error e) {
            if (failure == null) {
                failure = e;
            } else {
                addSuppressedCleanupFailure(failure, e);
            }
        }
        throwCleanupFailure(failure);
    }

    protected final void closeEndpointResources(EndpointCloseAction... endpointCloses) throws IOException {
        requireArgument(endpointCloses, "endpointCloses");
        Throwable failure = null;
        for (int i = 0; i < endpointCloses.length; i++) {
            var endpointClose = requireArgument(endpointCloses[i], "endpointCloses[" + i + "]");
            try {
                endpointClose.close();
            } catch (IOException | RuntimeException | Error e) {
                if (failure == null) {
                    failure = e;
                } else {
                    addSuppressedCleanupFailure(failure, e);
                }
            }
        }
        throwCleanupFailure(failure);
    }

    private static void throwCleanupFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private void closePublicEndpointThenRatisAfterStartFailure(EndpointCloseAction endpointClose, Throwable failure) {
        try {
            endpointClose.close();
        } catch (IOException | RuntimeException | Error closeFailure) {
            addSuppressedCleanupFailure(failure, closeFailure);
        }
        closeRatisLifecycleAfterStartFailure(failure);
    }

    protected final void closeRatisLifecycleAfterStartFailure(Throwable failure) {
        try {
            closeRatisLifecycle();
        } catch (IOException | RuntimeException | Error closeFailure) {
            addSuppressedCleanupFailure(failure, closeFailure);
        }
    }

    private static void addSuppressedCleanupFailure(Throwable failure, Throwable closeFailure) {
        if (failure != closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    @Override
    public void close() throws IOException {
        closeRatisLifecycle();
    }

    @FunctionalInterface
    protected interface EndpointCloseAction {
        void close() throws IOException;
    }

    @FunctionalInterface
    protected interface EndpointStartAction {
        void start() throws IOException;
    }
}
