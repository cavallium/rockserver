package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDatabaseBackupFactoryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void declaredFutureGlobalAndExportJournalsRejectSharedQuotaBeforeLayoutOrGates()throws Exception{
        try(var fixture=new RatisDatabaseBackupRealCohortFixture(root.resolve("factory-budget"),1,1);var factory=new RatisDatabaseBackupFactory(root.resolve("owned"),new RatisDatabaseBackupFactory.Limits(1,2,1024*1024))){
            UUID session=UUID.randomUUID();var ledger=SessionEpochLedger.initial(fixture.base.cluster,fixture.base.database,0).issue(session,300000);var grant=ledger.grants().getFirst();var domains=new ArrayList<DatabaseBackupDefinition.Domain>();var access=new HashMap<UUID,RatisDatabaseBackupFactory.Domain>();var reservations=new HashMap<UUID,RatisDatabaseBackupFactory.Reservation>();
            int index=0;for(var endpoint:fixture.endpoints){var state=endpoint.state;long sequence=1000L*index++;var identity=new RatisSnapshotClient.Identity(UUID.randomUUID(),new OperationId(session,1,sequence),new OperationId(session,1,sequence+1));UUID homeColumn=endpoint.homeRouted.directory().columns().iterator().next();var homes=new RatisSnapshotFences(endpoint.homeRouted,homeColumn);
                var scope=new TransactionScope(List.of(TransactionScope.Span.range(state.column,null,null)));var plan=RatisSnapshotPlan.create(state.directory,scope,identity.fence(),identity.business(),state.fence.homeGroup(),1,identity.firstControl());domains.add(new DatabaseBackupDefinition.Domain(state.directory,state.schema,List.of(plan.fence())));access.put(state.domain,new RatisDatabaseBackupFactory.Domain(homes,endpoint.routed,endpoint.scheduler));reservations.put(identity.fence(),new RatisDatabaseBackupFactory.Reservation(grant,identity));}
            var definition=new DatabaseBackupDefinition(UUID.randomUUID(),fixture.base.definition.roster(),domains,new DatabaseBackupDefinition.Limits(2,2,4096,64L*1024*1024,64*1024*1024));
            assertThrows(IOException.class,()->factory.create(definition,access,reservations,fixture.base.destination,fixture.base.policy,RatisExportJournal.Limits.defaults(),()->new RatisDatabaseBackupDriver.Vector(definition.roster(),definition.domains()),op->{throw new IOException("unused");}));
            assertFalse(Files.exists(root.resolve("owned").resolve(definition.backupId().toString())));for(var pool:fixture.base.pools){assertEquals(0,pool.gateChecks);assertEquals(0,pool.nativePool.snapshotAdmissionStats().pins());}
        }
    }
    @Test void publicFactoryCompletesOwnedNativeWorkflowClosesAndColdReopensSameArchive()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("factory-native"))){
            Path owned=root.resolve("public-owned");var limits=new RatisDatabaseBackupFactory.Limits(1,2,1024L*1024*1024);byte[] original;
            try(var factory=new RatisDatabaseBackupFactory(owned,limits)){
                var operation=factory.create(fixture.definition,fixture.access,fixture.reservations,fixture.real.base.destination,fixture.real.base.policy,RatisExportJournal.Limits.defaults(),()->new RatisDatabaseBackupDriver.Vector(fixture.definition.roster(),fixture.definition.domains()),op->{throw new IOException("unused");});
                assertThrows(IOException.class,operation::close);assertThrows(IOException.class,factory::close);
                RatisDatabaseBackupDriver.Progress progress=null;for(int i=0;i<128;i++){progress=operation.step(it.cavallium.rockserver.core.common.RequestContext.batch());if(progress.complete())break;}assertNotNull(progress);assertTrue(progress.complete());assertFalse(progress.cleanupPending());
                original=operation.snapshot().captured().getFirst().encode();operation.close();for(var pool:fixture.real.base.pools)assertEquals(0,pool.nativePool.snapshotAdmissionStats().pins());
            }
            try(var factory=new RatisDatabaseBackupFactory(owned,limits);var operation=factory.reopen(fixture.definition,fixture.access,fixture.reservations,fixture.real.base.destination,fixture.real.base.policy,RatisExportJournal.Limits.defaults(),()->new RatisDatabaseBackupDriver.Vector(fixture.definition.roster(),fixture.definition.domains()),op->{throw new IOException("unused");})){
                var progress=operation.step(it.cavallium.rockserver.core.common.RequestContext.batch());assertTrue(progress.complete());assertArrayEquals(original,operation.snapshot().captured().getFirst().encode());for(var pool:fixture.real.base.pools)assertEquals(0,pool.nativePool.snapshotAdmissionStats().pins());
            }
        }
    }

}
