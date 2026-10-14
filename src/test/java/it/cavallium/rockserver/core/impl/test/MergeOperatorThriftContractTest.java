package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.ThriftServer;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MergeOperatorThriftContractTest {
    private static final RequestContext CONTEXT = RequestContext.ingest();

    private static class Fixture {
        byte[] storedHash;
        long version;
        final ArrayList<String> calls = new ArrayList<>();
        final RocksDB.Iface wire;

        Fixture() throws Exception {
            var sync = new RocksDBSyncAPI() {
                @Override @SuppressWarnings("unchecked")
                public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> command) {
                    return (RS) apply(command);
                }
            };
            var async = new RocksDBAsyncAPI() {
                @Override public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(
                        String id, Long fromSeq, long maxEvents) {
                    throw new AssertionError("Unexpected CDC request");
                }
                @Override @SuppressWarnings("unchecked")
                public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> command) {
                    return (RA) CompletableFuture.completedFuture(apply(command));
                }
            };
            var backend = (RocksDBConnection) Proxy.newProxyInstance(RocksDBConnection.class.getClassLoader(),
                    new Class<?>[]{RocksDBConnection.class}, (p, m, a) -> {
                        assertEquals(CONTEXT, a[0]);
                        return switch (m.getName()) {
                            case "getSyncApi" -> sync;
                            case "getAsyncApi" -> async;
                            default -> throw new AssertionError(m.getName());
                        };
                    });
            var type = Class.forName(ThriftServer.class.getName() + "$ThriftHandler");
            var constructor = type.getDeclaredConstructor(RocksDBConnection.class, RWScheduler.class, int.class);
            constructor.setAccessible(true);
            var service = (RocksDB.Iface) constructor.newInstance(backend, null, 32 * 1024 * 1024);
            var processor = new RocksDB.Processor<>(service);
            wire = (RocksDB.Iface) Proxy.newProxyInstance(RocksDB.Iface.class.getClassLoader(),
                    new Class<?>[]{RocksDB.Iface.class}, (p, m, a) -> {
                        var request = new TMemoryBuffer(1024);
                        var response = new TMemoryBuffer(1024);
                        var client = new RocksDB.Client(new TBinaryProtocol(response), new TBinaryProtocol(request));
                        switch (m.getName()) {
                            case "checkMergeOperator" -> client.send_checkMergeOperator((String) a[0], (ByteBuffer) a[1],
                                    (it.cavallium.rockserver.core.common.api.RequestContext) a[2]);
                            case "uploadMergeOperator" -> client.send_uploadMergeOperator((String) a[0], (String) a[1],
                                    (ByteBuffer) a[2], (it.cavallium.rockserver.core.common.api.RequestContext) a[3]);
                            default -> throw new AssertionError(m.getName());
                        }
                        processor.process(new TBinaryProtocol(request), new TBinaryProtocol(response));
                        return m.getName().equals("checkMergeOperator")
                                ? client.recv_checkMergeOperator() : client.recv_uploadMergeOperator();
                    });
        }

        private Object apply(RocksDBAPICommand<?, ?, ?> command) {
            if (command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator check) {
                assertEquals("operator", check.name());
                calls.add("check");
                return Arrays.equals(storedHash, check.hash()) ? version : null;
            }
            if (command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator upload) {
                assertEquals("operator", upload.name());
                assertEquals("example.Operator", upload.className());
                calls.add("upload");
                storedHash = digest(upload.jarData());
                return ++version;
            }
            throw new AssertionError(command);
        }
    }

    private static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    @Test void ensureChecksBeforeUploadAndReusesMatchingArtifact() throws Exception {
        var fixture = new Fixture();
        var context = new it.cavallium.rockserver.core.common.api.RequestContext(
                it.cavallium.rockserver.core.common.api.WorkloadProfile.INGEST, RequestContext.NO_TIMEOUT);
        // Exercise the shared ensure policy through the generated binary protocol and real server handler.
        var api = new RocksDBSyncAPI() {
            @Override public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> command) {
                throw new AssertionError(command);
            }
            @Override public Long checkMergeOperator(String name, byte[] hash) {
                try {
                    var result = fixture.wire.checkMergeOperator(name, ByteBuffer.wrap(hash), context);
                    return result.isSetValue() ? result.getValue() : null;
                } catch (org.apache.thrift.TException error) { throw new AssertionError(error); }
            }
            @Override public long uploadMergeOperator(String name, String className, byte[] bytes) {
                try { return fixture.wire.uploadMergeOperator(name, className, ByteBuffer.wrap(bytes), context); }
                catch (org.apache.thrift.TException error) { throw new AssertionError(error); }
            }
        };
        var bytes = new byte[]{1, 2, 3};
        assertEquals(1L, api.ensureMergeOperator("operator", "example.Operator", bytes));
        assertEquals(1L, api.ensureMergeOperator("operator", "example.Operator", bytes));
        assertEquals(java.util.List.of("check", "upload", "check"), fixture.calls);
        assertEquals(2L, api.ensureMergeOperator("operator", "example.Operator", new byte[]{4}));
        assertEquals(java.util.List.of("check", "upload", "check", "check", "upload"), fixture.calls);
    }

    @Test void checkPreservesMissingMatchMismatchAndContextThroughBinaryProtocol() throws Exception {
        var fixture = new Fixture();
        var context = new it.cavallium.rockserver.core.common.api.RequestContext(
                it.cavallium.rockserver.core.common.api.WorkloadProfile.INGEST, RequestContext.NO_TIMEOUT);
        var bytes = new byte[]{1, 2, 3};
        assertFalse(fixture.wire.checkMergeOperator("operator", ByteBuffer.wrap(digest(bytes)), context).isSetValue());
        assertEquals(1L, fixture.wire.uploadMergeOperator("operator", "example.Operator", ByteBuffer.wrap(bytes), context));
        assertEquals(1L, fixture.wire.checkMergeOperator("operator", ByteBuffer.wrap(digest(bytes)), context).getValue());
        assertFalse(fixture.wire.checkMergeOperator("operator", ByteBuffer.wrap(digest(new byte[]{4})), context).isSetValue());
        assertEquals(java.util.List.of("check", "upload", "check", "check"), fixture.calls);
    }
}
