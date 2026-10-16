package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.schema.*;
import it.cavallium.rockserver.core.common.schema.SchemaOperations;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import it.cavallium.rockserver.core.server.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SchemaOperationsTransportTest {
    @TempDir Path root;
    static final UUID ID=new UUID(1,1);
    static SchemaOperations.Status paused(){return new SchemaOperations.Status(ID,SchemaOperations.Kind.CREATE,SchemaOperations.State.PAUSED,SchemaOperations.Phase.FENCE,null);}
    @Test void strictCurrentCodecsValidateEnumsIdentityOutcomesAndInventory()throws Exception {
        var status=paused();assertEquals(status,SchemaOperationsWire.status(SchemaOperationsWire.status(status),ID));
        assertEquals(status,ThriftSchemaOperationsWire.status(ThriftSchemaOperationsWire.status(status),ID));
        var wire=SchemaOperationsWire.status(status);
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire.toBuilder().setKindValue(99).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire.toBuilder().setStateValue(99).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire.toBuilder().setPhaseValue(99).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire.toBuilder().setRequestId(com.google.protobuf.ByteString.copyFrom(new byte[17])).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire.toBuilder().setStateValue(4).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire,new UUID(2,2)));
        var wrong=SchemaOperationResult.newBuilder().setRequestId(DistributedCdcWire.uuid(new UUID(2,2))).setKindValue(0).setChanged(true).build();
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.status(wire.toBuilder().setStateValue(4).setPhaseValue(14).setResult(wrong).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.inventory(SchemaOperationInventory.newBuilder().addAllOperations(Collections.nCopies(129,wire)).build()));
        assertThrows(IllegalArgumentException.class,()->SchemaOperationsWire.inventory(SchemaOperationInventory.newBuilder().addOperations(wire).addOperations(wire).build()));
        var thrift=ThriftSchemaOperationsWire.status(status);thrift.setRequestId(new byte[15]);assertThrows(IllegalArgumentException.class,()->ThriftSchemaOperationsWire.status(thrift));
        var missing=ThriftSchemaOperationsWire.status(new SchemaOperations.Status(ID,SchemaOperations.Kind.CREATE,SchemaOperations.State.DONE,SchemaOperations.Phase.DONE,new SchemaOperations.Result(ID,SchemaOperations.Kind.CREATE,7,true,false)));missing.result.unsetChanged();
        assertThrows(IllegalArgumentException.class,()->ThriftSchemaOperationsWire.status(missing));
    }
    @Test void standaloneStartsBothServersAndRefusesOnlyLifecycleCalls()throws Exception {
        var config=root.resolve("database.conf");Files.writeString(config,"database: { global: { optimistic: false } }");
        try(var local=new EmbeddedConnection(root.resolve("db"),"schema-unsupported",config)){
            assertNotNull(local.getSchemaOperationsApi());
            for(boolean thrift:new boolean[]{false,true}){
                if(thrift){int port;try(var socket=new ServerSocket(0)){port=socket.getLocalPort();}
                    try(var server=new ThriftServer(local,"127.0.0.1",port)){server.start();try(var remote=new ThriftConnection("schema-thrift","127.0.0.1",port)){unsupported(remote);}}
                }else try(var server=new GrpcServer(local,new InetSocketAddress("127.0.0.1",0))){server.start();try(var remote=GrpcConnection.forHostAndPort("schema-grpc",new Utils.HostAndPort("127.0.0.1",server.getPort()))){unsupported(remote);}}
            }
        }
    }
    private static void unsupported(RocksDBConnection remote)throws Exception {
        var api=remote.getSchemaOperationsApi();
        var error=assertThrows(ExecutionException.class,()->api.inventory(it.cavallium.rockserver.core.common.RequestContext.batch()).get(10,TimeUnit.SECONDS));
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,assertInstanceOf(RocksDBException.class,error.getCause()).getErrorUniqueId());
        assertThrows(Exception.class,()->api.status(ID,null).get(10,TimeUnit.SECONDS));
    }
    @Test void cancelledGrpcObservationNeverCancelsOwnedBackendFuture() {
        var actual=new CompletableFuture<SchemaOperations.Status>();var backend=new SchemaOperationsAsyncAPI(){
            public CompletableFuture<List<SchemaOperations.Status>> inventory(it.cavallium.rockserver.core.common.RequestContext c){return CompletableFuture.completedFuture(List.of(paused()));}
            public CompletableFuture<SchemaOperations.Status> status(UUID id,it.cavallium.rockserver.core.common.RequestContext c){return actual;}
            public CompletableFuture<SchemaOperations.Status> resume(UUID id,it.cavallium.rockserver.core.common.RequestContext c){return actual;}
            public CompletableFuture<SchemaOperations.Status> abort(UUID id,it.cavallium.rockserver.core.common.RequestContext c){return actual;}
        };
        var service=new SchemaOperationsGrpcService(()->backend);var delivered=new java.util.concurrent.atomic.AtomicBoolean();var context=io.grpc.Context.current().withCancellation();
        context.run(()->service.resume(SchemaOperationRequest.newBuilder().setRequestId(DistributedCdcWire.uuid(ID)).setContext(DistributedCdcWire.context(it.cavallium.rockserver.core.common.RequestContext.batch(),Long.MAX_VALUE)).build(),new io.grpc.stub.StreamObserver<SchemaOperationStatus>(){public void onNext(SchemaOperationStatus s){delivered.set(true);}public void onError(Throwable e){fail(e);}public void onCompleted(){}}));
        context.cancel(null);assertFalse(actual.isCancelled());assertTrue(actual.complete(paused()));assertFalse(delivered.get());
    }
}
