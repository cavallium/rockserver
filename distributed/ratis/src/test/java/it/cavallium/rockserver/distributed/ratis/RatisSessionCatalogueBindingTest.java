package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisSessionCatalogueBindingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void borrowedCatalogueReadersUseDirectoryCfAndLeaveEpochLedgerInSessionsCf() throws Exception {
        var helper=new RatisSessionEpochsTest();
        try(var f=helper.new Fixture(root)) {
            var ledger=SessionEpochLedger.initial(helper.cluster,helper.database,10);
            assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(f.epochs.prepare(helper.operation(0),null,ledger)).status());
            var source=helper.directory(helper.cluster);var replicas=source.ranges().getFirst().replicas();byte[] boundary={(byte)128};
            var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),1,source.columns(),0,List.of(
                    new RangeDescriptor(new UUID(9,1),new UUID(9,2),1,0,null,boundary,RangeDescriptor.TopologyState.ACTIVE,replicas),
                    new RangeDescriptor(new UUID(9,3),new UUID(9,4),1,0,boundary,null,RangeDescriptor.TopologyState.ACTIVE,replicas)));
            var definition=new TopologyChange.Definition(new UUID(9,5),source,target);var state=TopologyChange.State.begin(definition.header()).beginFencing();
            var command=new DistributedCommand(helper.catalogue,1,new byte[32],new OperationId(new UUID(9,6),0,0),List.of(
                    new Mutation(Mutation.Kind.PUT,helper.directoryColumn,RatisSessionEpochsTest.bytes(source.domainId()),RangeDirectoryCodec.encode(source)),
                    new Mutation(Mutation.Kind.PUT,helper.directoryColumn,RatisTopologyCatalogue.definitionKey(definition.digest()),definition.encode()),
                    new Mutation(Mutation.Kind.PUT,helper.directoryColumn,RatisTopologyCatalogue.stateKey(definition.intentId()),state.encode())));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(1,f.next.getAndIncrement()),command)).status());
            var topology=f.epochs.topologyCatalogue(helper.directoryColumn);
            assertArrayEquals(definition.encode(),topology.readDefinition(definition.digest()).encode());
            assertArrayEquals(state.encode(),topology.readIntent(definition.intentId()).encode());
            assertArrayEquals(RangeDirectoryCodec.encode(source),RangeDirectoryCodec.encode(f.epochs.catalogue(helper.directoryColumn).read(source.domainId())));
            assertEquals(ledger,f.epochs.read());assertEquals(0,f.closes.get());
        }
    }
}
