package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.server.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.junit.jupiter.api.Assertions.*;

/** Current public clients and actual socket servers preserve eager scope and snapshot semantics. */
class TransactionScopeTransportTest {
    enum Transport { GRPC,THRIFT }
    @TempDir Path root;
    private static final Duration TTL=Duration.ofSeconds(30);
    private static final Keys KEY=new Keys(Buf.wrap(new byte[]{1}));
    private static Buf value(int number){return Buf.wrap(new byte[]{(byte)number});}

    @ParameterizedTest @EnumSource(Transport.class)
    void explicitScopeRollbackCommitReopenAndNullScopeCrossRealTransport(Transport transport)throws Exception {
        var config=root.resolve("database.conf");Files.writeString(config,"database: { global: { optimistic: false } }");
        try(var local=new EmbeddedConnection(root.resolve("database"),"scope-transport",config)) {
            var api=local.getSyncApi(RequestContext.batch());var schema=ColumnSchema.of(IntList.of(1),ObjectList.of(),true);
            long first=api.createColumn("first",schema),other=api.createColumn("other",schema);
            if(transport==Transport.GRPC) {
                try(var server=new GrpcServer(local,new InetSocketAddress("127.0.0.1",0))) {
                    server.start();
                    try(var remote=GrpcConnection.forHostAndPort("scope-grpc",new Utils.HostAndPort("127.0.0.1",server.getPort()))) {exercise(local,remote,first,other);}
                }
            }else {
                int port;try(var reservation=new ServerSocket(0)){port=reservation.getLocalPort();}
                try(var server=new ThriftServer(local,"127.0.0.1",port)) {
                    server.start();
                    try(var remote=new ThriftConnection("scope-thrift","127.0.0.1",port)){exercise(local,remote,first,other);}
                }
            }
        }
    }
    private static void exercise(EmbeddedConnection local,RocksDBConnection remote,long first,long other) {
        var writer=local.getSyncApi(RequestContext.batch(TTL));var api=remote.getSyncApi(RequestContext.batch(TTL));
        var scope=new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(first,KEY)));
        writer.put(0,first,KEY,value(1),RequestType.none());
        long tx=api.openTransaction(scope,TTL);
        try {
            writer.put(0,first,KEY,value(2),RequestType.none());
            assertEquals(value(1),api.get(tx,first,KEY,RequestType.current()));
            long scoped=tx;
            assertEquals(RocksDBException.RocksDBErrorType.RESTRICTED_TRANSACTION,
                    assertThrows(RocksDBException.class,()->api.get(scoped,other,KEY,RequestType.current())).getErrorUniqueId());
        }finally{assertTrue(api.closeTransaction(tx,false));}
        tx=api.openTransaction(scope,TTL);boolean committed=false;
        try {
            assertEquals(value(2),api.get(tx,first,KEY,RequestType.current()));
            api.put(tx,first,KEY,value(3),RequestType.none());
            assertTrue(api.closeTransaction(tx,true));committed=true;
        }finally{if(!committed)api.closeTransaction(tx,false);}
        tx=api.openTransaction(null,TTL);
        try {
            writer.put(0,first,KEY,value(4),RequestType.none());
            assertEquals(value(3),api.get(tx,first,KEY,RequestType.current()));
        }finally{assertTrue(api.closeTransaction(tx,false));}
        assertEquals(0,local.getEmbeddedDB().getOpenTransactionsCount());
    }
}
