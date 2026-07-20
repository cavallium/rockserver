package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ratis.statemachine.impl.BaseStateMachine;

public final class RatisClasspathAdapterRuntimeProbe {

    private RatisClasspathAdapterRuntimeProbe() {
    }

    public static void run() throws IOException {
        var existing = descriptor("runtime-rollback-existing", 19011);
        var added = descriptor("runtime-rollback-added", 19012);
        var rejected = descriptor("runtime-rollback-rejected", 19013);
        var closeAttempts = new AtomicInteger();
        var rollbackCloseFailure = new IOException("injected staged partition close failure");
        var additionFailure = new IOException("injected staged partition addition failure");

        try (var runtime = new RatisClasspathAdapterRuntime(
                List.of(existing),
                _ -> new BaseStateMachine(),
                (descriptor, _) -> descriptor.partitionId().equals(added.partitionId())
                        ? new CloseFailingPartitionServer(closeAttempts, rollbackCloseFailure)
                        : new NoOpPartitionServer()
        )) {
            runtime.start();
            try {
                runtime.reload(
                        List.of(descriptor("runtime-rollback-existing", 19014)),
                        _ -> new BaseStateMachine()
                );
                throw new IllegalStateException("Runtime descriptor-change probe must reject live descriptor changes");
            } catch (RatisFeatureUnavailableException e) {
                require(e.getMessage().contains(existing.partitionId()),
                        "Runtime descriptor-change probe must identify the changed partition");
            }
            require(runtime.isStarted(), "Rejected live descriptor changes must keep runtime started");
            require(runtime.clients().containsKey(existing.partitionId()),
                    "Rejected live descriptor changes must keep existing clients published");
            var retiredClient = runtime.borrowedClient(existing.partitionId());

            IOException reportedFailure;
            try {
                runtime.reload(
                        List.of(existing, added, rejected),
                        descriptor -> {
                            if (descriptor.partitionId().equals(rejected.partitionId())) {
                                throw additionFailure;
                            }
                            return new BaseStateMachine();
                        }
                );
                throw new IllegalStateException("Runtime staged rollback probe must inject an addition failure");
            } catch (IOException e) {
                reportedFailure = e;
            }
            require(reportedFailure == additionFailure,
                    "Runtime staged rollback must preserve the original addition failure");
            require(Arrays.asList(reportedFailure.getSuppressed()).contains(rollbackCloseFailure),
                    "Runtime staged rollback must suppress its retained cleanup failure");
            require(closeAttempts.get() == 2,
                    "Runtime staged rollback must retry retained server cleanup during fail-closed shutdown");
            require(!runtime.isStarted(), "Failed staged rollback cleanup must stop runtime");
            require(runtime.clients().isEmpty(), "Failed staged rollback cleanup must unpublish clients");
            require(rejectsRetiredRuntimeClient(retiredClient),
                    "Failed staged rollback cleanup must retire stale client handles");
            require(!runtime.health().isReady(), "Failed staged rollback cleanup must degrade runtime health");
        }

        System.out.println("Ratis adapter-runtime typed unsupported reload and staged rollback cleanup probe OK");
    }

    private static RatisPartitionDescriptor descriptor(String id, int port) {
        var peer = new RatisPeerDescriptor(id + "-node", "grpc://127.0.0.1:" + port, "ratis-spike");
        return new RatisPartitionDescriptor(
                id + "-partition",
                UUID.randomUUID().toString(),
                peer.peerId(),
                List.of(peer),
                Path.of("target", "ratis-spike-" + id)
        );
    }

    private static boolean rejectsRetiredRuntimeClient(
            it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient client) {
        try {
            client.queryLocal(new byte[0]).join();
            return false;
        } catch (CompletionException e) {
            return e.getCause() instanceof IllegalStateException
                    && e.getCause().getMessage().contains("partition is retired");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static class NoOpPartitionServer implements RatisClasspathAdapterRuntime.PartitionServer {

        @Override
        public void start() {
        }

        @Override
        public void reloadStateMachine(RatisClasspathAdapterRuntime.StateMachineFactory stateMachineFactory) {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static final class CloseFailingPartitionServer extends NoOpPartitionServer {

        private final AtomicInteger closeAttempts;
        private final IOException closeFailure;

        private CloseFailingPartitionServer(AtomicInteger closeAttempts, IOException closeFailure) {
            this.closeAttempts = closeAttempts;
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() throws IOException {
            if (closeAttempts.incrementAndGet() == 1) {
                throw closeFailure;
            }
        }
    }
}
