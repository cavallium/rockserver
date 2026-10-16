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
            RatisRocksDBConnection.GenerationFactory factory=(generationBindings,path,ids)->{if(generationBindings.domains().size()!=1)throw new java.io.IOException("This singleton fixture requires one domain");var selectedDomain=generationBindings.domains().getFirst();var schema=selectedDomain.schema();var directory=selectedDomain.directory();
                if (calls.getAndIncrement()==0) throw new IOException("Controlled generation factory failure");
                return f.factory.create(generationBindings,path,ids);
            };
            var limits=RatisRocksDBConnection.Limits.defaults();
            try(var initial=new RatisRocksDBConnection(URI.create("ratis://controlled/retry"),f.connectionRoot,java.util.Map.of(f.direct.f.schema.domainId(),f.direct.routed),(name,shape)->f.direct.f.schema.domainId(),f.direct.f.scheduler,f.binding,f.direct.f.catalogue.client,replicas,null,null,factory,limits,List.of(),RatisJournalAccess.standalone())){
                assertEquals(0,calls.get(),"Opening validates authority without creating absent generation owners");
                assertThrows(java.util.concurrent.ExecutionException.class,()->initial.remoteMutationOwners(71,RequestContext.batch()).get(10,java.util.concurrent.TimeUnit.SECONDS));
            }
            Path binding=f.connectionRoot.resolve("generations").resolve(RatisGenerationRegistry.key(f.direct.f.schema,f.direct.routed.directory())).resolve("binding");
            byte[] original=Files.readAllBytes(binding);
            try (var recovered=new RatisRocksDBConnection(URI.create("ratis://controlled/retry"),f.connectionRoot,java.util.Map.of(f.direct.f.schema.domainId(),f.direct.routed),(name,shape)->f.direct.f.schema.domainId(),f.direct.f.scheduler,f.binding,f.direct.f.catalogue.client,replicas,null,null,factory,limits,List.of(), RatisJournalAccess.standalone())) {
                assertEquals(71,recovered.getSyncApi(RequestContext.batch()).getColumnId("atomic-0"));
                assertArrayEquals(original,Files.readAllBytes(binding)); assertEquals(2,calls.get());
            }
            assertEquals(0,f.direct.f.catalogue.closes.get(),"Failed construction and recovery leave borrowed catalogue open");
        }
    }
}
