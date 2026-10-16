package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.server.*;
import java.net.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.junit.jupiter.api.Assertions.*;

class RatisPhysicalDispatchTest {
    enum Transport { LOCAL,GRPC,THRIFT }
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static final RequestContext CONTEXT=RequestContext.batch(Duration.ofSeconds(15));
    private static List<RocksDBAPICommand.RocksDBAPICommandSingle<?>> physical(long column) {
        return List.of(new RocksDBAPICommand.RocksDBAPICommandSingle.EstimateNumKeys(column),
                new RocksDBAPICommand.GetTableProperties(column),new RocksDBAPICommand.GetSstMetadata(column,-1),
                new RocksDBAPICommand.Flush(),new RocksDBAPICommand.Compact(),
                new RocksDBAPICommand.CompactFiles(new SstMaintenance.Request(column,"unqualified",List.of("000001.sst"),1,0,1,1,1,false)));
    }
    private static void refused(RocksDBSyncAPI api) {
        for(long column:new long[]{71,Long.MAX_VALUE})for(var request:physical(column)) {
            var failure=assertThrows(RocksDBException.class,()->api.requestSync(request));
            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,failure.getErrorUniqueId());
            assertFalse(failure.getMessage().isBlank());
        }
    }
    @ParameterizedTest @EnumSource(Transport.class)
    void typedRefusalCrossesRealTransportsWithoutDataDispatchOrCapture(Transport transport)throws Exception {
        var helper=new RatisRocksDBConnectionTest();helper.root=root;
        try(var credits=helper.pointBudget;var fixture=helper.new Fixture();var connection=fixture.open(8)) {
            var nativeState=fixture.direct.f;
            RatisRocksDBConnectionTest.point(connection,71,RatisAtomicClientTest.key(2),RatisAtomicClientTest.value(9),RequestType.RequestTypeId.NOTHING);
            int requests=nativeState.rpcs.get(),captures=nativeState.captureRequests.get();long nextEpoch=nativeState.catalogue.epochs.read().nextEpoch();
            if(transport==Transport.LOCAL)refused(connection.getSyncApi(CONTEXT));
            else if(transport==Transport.GRPC) {
                try(var server=new GrpcServer(connection,new InetSocketAddress("127.0.0.1",0))) {
                    server.start();try(var remote=GrpcConnection.forHostAndPort("physical-refusal",new Utils.HostAndPort("127.0.0.1",server.getPort()))){refused(remote.getSyncApi(CONTEXT));}
                }
            } else {
                // Thrift already refuses its unqualified SST methods locally; the other methods cross the socket.
                int port;try(var reservation=new ServerSocket(0)){port=reservation.getLocalPort();}
                try(var server=new ThriftServer(connection,"127.0.0.1",port)) {
                    server.start();try(var remote=new ThriftConnection("physical-refusal","127.0.0.1",port)){refused(remote.getSyncApi(CONTEXT));}
                }
            }
            assertEquals(requests,nativeState.rpcs.get());assertEquals(captures,nativeState.captureRequests.get());assertEquals(nextEpoch,nativeState.catalogue.epochs.read().nextEpoch());
            assertEquals(RatisAtomicClientTest.value(9),connection.getSyncApi(CONTEXT).get(0,71,RatisAtomicClientTest.key(2),RequestType.current()));
        }
    }
    @Test void directAdapterRefusesBeforeAllocatingTransactionIdentity()throws Exception {
        try(var fixture=new RatisAtomicClientTest.Fixture(root.resolve("adapter"),true);var transactions=RatisRocksDBAdapterTest.client(fixture,fixture.data)) {
            var allocations=new RatisRocksDBAdapterTest.Allocations(fixture);
            try(var adapter=new RatisRocksDBAdapter(fixture.schema,fixture.directory,transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT);int requests=fixture.rpcs.get();
                for(var request:physical(Long.MAX_VALUE)) {
                    CompletableFuture<?> result=api.requestAsync(request);
                    var failure=assertThrows(ExecutionException.class,()->result.get(10,TimeUnit.SECONDS));
                    assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,assertInstanceOf(RocksDBException.class,failure.getCause()).getErrorUniqueId());
                }
                assertEquals(0,allocations.count.get());assertEquals(0,adapter.stats().transactions());assertEquals(requests,fixture.rpcs.get());
            }
        }
    }
}
