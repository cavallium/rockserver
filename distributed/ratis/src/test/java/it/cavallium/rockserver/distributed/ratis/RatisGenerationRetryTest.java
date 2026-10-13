package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisGenerationRetryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void failedFactoryReopensExactForcedBindingWithoutLeakingConnectionOwnership() throws Exception {
        var tests=new RatisRocksDBConnectionTest(); tests.root=root;
        try (var f=tests.new Fixture(); var replicas=new RatisSchemaReplicaClient(1,(range,owner) -> { throw new IOException("No replica query during binding retry"); })) {
            var calls=new AtomicInteger();
            RatisRocksDBConnection.GenerationFactory factory=(schema,directory,path,ids) -> {
                if (calls.getAndIncrement()==0) throw new IOException("Controlled generation factory failure");
                return f.factory.create(schema,directory,path,ids);
            };
            var limits=RatisRocksDBConnection.Limits.defaults();
            assertThrows(IOException.class,() -> new RatisRocksDBConnection(URI.create("ratis://controlled/retry"),f.connectionRoot,java.util.Map.of(f.direct.f.schema.domainId(),f.direct.routed),(name,shape)->f.direct.f.schema.domainId(),f.direct.f.scheduler,f.binding,f.direct.f.catalogue.client,replicas,null,null,factory,limits,List.of()));
            Path binding=f.connectionRoot.resolve("generations").resolve(HexFormat.of().formatHex(f.direct.f.schema.digest())).resolve("binding");
            byte[] original=Files.readAllBytes(binding);
            try (var recovered=new RatisRocksDBConnection(URI.create("ratis://controlled/retry"),f.connectionRoot,java.util.Map.of(f.direct.f.schema.domainId(),f.direct.routed),(name,shape)->f.direct.f.schema.domainId(),f.direct.f.scheduler,f.binding,f.direct.f.catalogue.client,replicas,null,null,factory,limits,List.of())) {
                assertEquals(71,recovered.getSyncApi(RequestContext.batch()).getColumnId("atomic-0"));
                assertArrayEquals(original,Files.readAllBytes(binding)); assertEquals(2,calls.get());
            }
            assertEquals(0,f.direct.f.catalogue.closes.get(),"Failed construction and recovery leave borrowed catalogue open");
        }
    }
}
