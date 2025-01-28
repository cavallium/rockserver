package it.cavallium.rockserver.core;

import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;
import it.cavallium.buffer.Buf;
import java.net.InetSocketAddress;

public class TestGrpcLoop {
    public static void main(String[] args) throws IOException, InterruptedException {
        var embeddedDB = new EmbeddedConnection(null, "main", null);
        var server = new GrpcServer(embeddedDB, new InetSocketAddress("localhost", 12345));
        server.start();
        var clientB = new ClientBuilder();
        clientB.setHttpAddress(new Utils.HostAndPort("localhost", 12345));
        clientB.setName("local");
        clientB.setUseThrift(false);
        var client = clientB.build();
        var col = client.getSyncApi().createColumn("test", ColumnSchema.of(IntList.of(15), ObjectList.of(), true));
        var parallelism = 4;
        for (int i = 0; i < parallelism; i++) {
            var t = Thread.ofPlatform().daemon().name("test-requests-thread-" + i).start(() -> {
                while (true) {
                    var delta = client.getSyncApi().put(0, col,
                        new Keys(new Buf[]{Buf.wrap(new byte[15])}),
                        Buf.wrap(new byte[15]),
                        RequestType.delta());
                }
            });
            if (i + 1 == parallelism) {
                t.join();
            }
        }
    }
}
