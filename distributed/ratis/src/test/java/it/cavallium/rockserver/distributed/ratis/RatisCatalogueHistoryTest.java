package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Arithmetic policy evidence; native atomic persistence wiring is separately exercised by the native owner. */
class RatisCatalogueHistoryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void originalMixedGenesisIsValidatedAndOnlyHistoryColumnEffectsAreAccounted() throws Exception {
        var importTest=new RatisRangeImportCatalogueTest(); importTest.root=root;
        var genesisTest=new RatisCatalogueGenesisTest(); byte[] original; CatalogueHistory.State capacity;
        try(var f=importTest.new Fixture()) {
            f.enableHistory(); var before=f.nativeCatalogue.store.catalogueHistoryCapacity(); var plan=genesisTest.plan(f.outer);
            try(var genesis=new RatisCatalogueGenesis(root.resolve("history-genesis.bin"),f.client,plan,plan.domains().getFirst().directory().domainId(),10,null,null,RatisCatalogueGenesisTest.roster(plan))) {
                var command=genesis.preparedCommand(); original=DistributedCommandCodec.encode(command);
                assertTrue(command.mutations().stream().anyMatch(m -> m.columnId().equals(f.outer.sessions)));
                assertEquals(CatalogueHistory.Action.UNRESERVED,RatisSchemaCatalogue.historyPolicy(f.outer.directoryColumn,f.pure,f.immutable::read).apply(command).action());
                var mutations=new ArrayList<>(command.mutations()); var ledger=mutations.get(1); mutations.set(1,new Mutation(Mutation.Kind.PUT,new UUID(999,999),ledger.key(),ledger.value()));
                var invalid=new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),mutations,command.predicates());
                assertThrows(IllegalArgumentException.class,() -> RatisSchemaCatalogue.historyPolicy(f.outer.directoryColumn,f.pure,f.immutable::read).apply(invalid));
                assertEquals(before,f.nativeCatalogue.store.catalogueHistoryCapacity());
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); capacity=f.nativeCatalogue.store.catalogueHistoryCapacity();
                assertEquals(before,capacity,"Independently bounded directory, roster and session ledger are outside the history subtotal");
                assertNotNull(f.nativeCatalogue.store.readCurrentPoint(f.outer.sessions,RatisSessionEpochsTest.bytes(f.outer.database)));
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); assertEquals(capacity,f.nativeCatalogue.store.catalogueHistoryCapacity());
            }
        }
        try(var f=importTest.new Fixture()) {
            f.enableHistory(); assertEquals(capacity,f.nativeCatalogue.store.catalogueHistoryCapacity()); var plan=genesisTest.plan(f.outer);
            try(var genesis=new RatisCatalogueGenesis(root.resolve("history-genesis.bin"),f.client,plan,plan.domains().getFirst().directory().domainId(),10,null,null,RatisCatalogueGenesisTest.roster(plan))) {
                assertArrayEquals(original,DistributedCommandCodec.encode(genesis.preparedCommand())); assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status());
            }
        }
    }
    @Test void composedNativeCreditsFinishSchemaAndImportAtExactHistoryRowCeilingAndReopen() throws Exception {
        var importTest=new RatisRangeImportCatalogueTest(); importTest.root=root;
        var schemaTest=new RatisSchemaAdministrationTest();
        byte[] schemaOutcome,importOutcome; CatalogueHistory.State finalCapacity;
        RangeImportDefinition imported; SchemaChange.Definition schemaChange;
        try(var f=importTest.new Fixture()) {
            imported=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(20));
            f.seed(imported.sourceDirectory()); f.enableHistory();
            var schemaClient=new RatisSchemaCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            var admin=new RatisSchemaAdministration(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            var ordinary=new RatisCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            var source=schemaTest.initial(); var original=schemaTest.directory(source,0);
            assertEquals(CommandResult.Status.SUCCESS,schemaClient.execute(schemaClient.prepare(op(0),source)).status());
            assertEquals(CommandResult.Status.SUCCESS,ordinary.execute(ordinary.prepare(op(1),null,original)).status());
            var target=schemaTest.next(source,List.of(source.column(7),schemaTest.column(8,new UUID(777,1),"added")));
            var columns=new HashMap<UUID,byte[]>(); var apis=new HashMap<Long,byte[]>();
            columns.put(source.column(7).columnId(),admin.readColumnIdentity(source.column(7).columnId())); apis.put(7L,admin.readApiIdentity(7));
            assertEquals(CommandResult.Status.SUCCESS,schemaClient.execute(schemaClient.prepare(op(2),target,admin.readAllocationCounter(),columns,apis,Map.of())).status());
            schemaChange=new SchemaChange.Definition(new UUID(777,2),op(3),original,source,target);
            var definitionCommand=admin.prepareDefinition(op(4),schemaChange,null,admin.readColumnIdentity(schemaChange.header().changedColumnId()),admin.readApiIdentity(schemaChange.header().changedApiId()));
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(definitionCommand).status());
            var begin=admin.prepareBegin(schemaChange,null,admin.readColumnIdentity(schemaChange.header().changedColumnId()),admin.readApiIdentity(schemaChange.header().changedApiId()));
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(begin).status());
            var installed=RatisRangeImportCatalogueTest.admission(imported,5); f.stage(imported,installed);
            var allocation=new RatisColumnAllocation(f.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            long sequence=1000,api=RatisColumnAllocation.counter(admin.readAllocationCounter());
            while(true) {
                var usage=f.nativeCatalogue.store.catalogueHistoryCapacity(); long free=CatalogueHistory.MAX_ROWS-usage.actualRows()-usage.promisedRows(); if(free==0)break;
                int count=(int)Math.min(RangeDirectoryCodec.MAX_COLUMNS,free); var reservations=new ArrayList<RatisColumnAllocation.Reservation>();
                for(int i=0;i<count;i++) { long value=++api; reservations.add(new RatisColumnAllocation.Reservation(new UUID(888,value),original.domainId(),new UUID(889,value),value)); }
                assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepareReservations(op(sequence++),reservations,admin.readAllocationCounter())).status());
            }
            var full=f.nativeCatalogue.store.catalogueHistoryCapacity(); assertEquals(CatalogueHistory.MAX_ROWS,full.actualRows()+full.promisedRows());
            var rejected=allocation.prepare(op(sequence++),new UUID(888,++api),original.domainId(),new UUID(889,api),admin.readAllocationCounter());
            assertEquals(CommandResult.Status.CONFLICT,allocation.execute(rejected).status()); assertEquals(full,f.nativeCatalogue.store.catalogueHistoryCapacity());
            assertEquals(CommandResult.Status.CONFLICT,allocation.execute(rejected).status()); assertEquals(full,f.nativeCatalogue.store.catalogueHistoryCapacity());
            var failedCas=allocation.prepare(op(sequence++),new UUID(890,api),original.domainId(),new UUID(891,api),RatisColumnAllocation.counterValue(api-2));
            assertEquals(CommandResult.Status.CONFLICT,allocation.execute(failedCas).status()); assertEquals(full,f.nativeCatalogue.store.catalogueHistoryCapacity(),"A failed counter predicate charges zero");
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(begin).status()); assertEquals(full,f.nativeCatalogue.store.catalogueHistoryCapacity(),"Original BEGIN retry must not duplicate promised capacity");
            var completedSchema=schemaTest.complete(admin,schemaChange,admin.readHead(original.domainId()));
            assertTrue(completedSchema.terminal()); schemaOutcome=completedSchema.encode();
            var prepared=f.catalogue.readHead(imported.sourceDirectory().domainId());
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.preparePublication(imported,prepared,installed,null)).status());
            var published=f.catalogue.readHead(imported.sourceDirectory().domainId()); var entries=new ArrayList<RangeImportActivationAdmission.OwnerActivation>();
            for(var owner:installed.owners()) entries.add(new RangeImportActivationAdmission.OwnerActivation(owner.groupId(),owner.rangeId(),owner.configurationVersion(),owner.owner(),new RangeImportActivationAdmission.Activation(owner.receipt(),f.outer.catalogue,new ApplyPosition(1,f.nativeCatalogue.next.get()-1),RangeImportAdmission.sha256(published.encode()),new ApplyPosition(2,6)),new ApplyPosition(2,10)));
            f.activations=new RangeImportActivationAdmission(imported,installed,f.outer.catalogue,RangeImportAdmission.sha256(published.encode()),entries);
            for(int i=0;i<RatisRangeImportCatalogue.activationArtifact(f.activations).chunks();i++) assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.prepareActivationChunk(op(sequence++),imported,installed,published,f.activations,i)).status());
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.prepareActivationCommit(op(sequence++),imported,installed,published,f.activations)).status());
            var completion=f.catalogue.prepareCompletion(op(sequence++),imported,installed,published,f.activations,null);
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(completion).status()); importOutcome=f.catalogue.readHead(imported.sourceDirectory().domainId()).encode();
            finalCapacity=f.nativeCatalogue.store.catalogueHistoryCapacity(); assertEquals(0,finalCapacity.promisedRows()); assertEquals(0,finalCapacity.promisedBytes());
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(completion).status()); assertEquals(finalCapacity,f.nativeCatalogue.store.catalogueHistoryCapacity());
        }
        try(var f=importTest.new Fixture()) {
            f.enableHistory(); assertEquals(finalCapacity,f.nativeCatalogue.store.catalogueHistoryCapacity());
            var admin=new RatisSchemaAdministration(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertArrayEquals(schemaOutcome,admin.readOutcome(schemaChange.header().domainId(),schemaChange.digest()).encode());
            assertArrayEquals(importOutcome,f.catalogue.readHead(imported.sourceDirectory().domainId()).encode());
            assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,f.catalogue.readImport(imported.importId()).phase());
            assertArrayEquals(imported.encode(),f.catalogue.readDefinition(imported.digest()).encode());
        }
    }
    static OperationId op(long sequence) { return new OperationId(new UUID(777,99),1,sequence); }
    @Test void acceptedBeginProtectsCompletionFromOrphanHistoryAndTerminalReleasesUnusedCredit() {
        byte[] definition=new byte[32]; definition[0]=1;
        var initial=new CatalogueHistory.State(CatalogueHistory.MAX_BYTES-5000,100,0,0);
        var begun=CatalogueHistory.apply(initial,null,CatalogueHistory.Directive.begin(definition,4000,3),new CatalogueHistory.Delta(200,1));
        assertEquals(4000,begun.state().promisedBytes());
        assertThrows(IllegalArgumentException.class,() -> CatalogueHistory.apply(begun.state(),null,CatalogueHistory.Directive.unreserved(),new CatalogueHistory.Delta(720,1)));
        var spent=CatalogueHistory.apply(begun.state(),begun.credit(),CatalogueHistory.Directive.spend(definition),new CatalogueHistory.Delta(3000,2));
        assertEquals(1000,spent.credit().bytes()); assertEquals(1,spent.credit().rows());
        var ended=CatalogueHistory.apply(spent.state(),spent.credit(),CatalogueHistory.Directive.terminal(definition),new CatalogueHistory.Delta(500,1));
        assertNull(ended.credit()); assertEquals(0,ended.state().promisedBytes()); assertEquals(0,ended.state().promisedRows());
        assertDoesNotThrow(() -> CatalogueHistory.apply(ended.state(),null,CatalogueHistory.Directive.unreserved(),new CatalogueHistory.Delta(1000,1)));
        assertArrayEquals(ended.state().encode(),CatalogueHistory.State.decode(ended.state().encode()).encode());
    }
    @Test void classificationDoesNotConfuseArbitraryDirectoryUuidWithHistoryTags() {
        for(int tag=0;tag<256;tag++) { byte[] domain=new byte[16]; domain[0]=(byte)tag; assertFalse(RatisSchemaCatalogue.isHistoryKey(domain)); }
        assertTrue(RatisSchemaCatalogue.isHistoryKey(new byte[]{0x61}));
        byte[] schema=new byte[33]; schema[0]=0x53; assertTrue(RatisSchemaCatalogue.isHistoryKey(schema));
        byte[] operator=new byte[33]; operator[0]=0x70; assertFalse(RatisSchemaCatalogue.isHistoryKey(operator));
        assertFalse(RatisSchemaCatalogue.isHistoryKey(new byte[]{0x53}));
    }
    @Test void rowCeilingAndOriginalCreditIdentityAreIndependentOfByteBudget() {
        byte[] definition=new byte[32],other=new byte[32]; other[0]=1;
        var original=new CatalogueHistory.State(1000,CatalogueHistory.MAX_ROWS-3,0,0);
        assertThrows(IllegalArgumentException.class,() -> CatalogueHistory.apply(original,null,CatalogueHistory.Directive.begin(definition,100,3),new CatalogueHistory.Delta(0,0)));
        var begun=CatalogueHistory.apply(original,null,CatalogueHistory.Directive.begin(definition,100,2),new CatalogueHistory.Delta(0,0));
        assertThrows(IllegalArgumentException.class,() -> CatalogueHistory.apply(begun.state(),begun.credit(),CatalogueHistory.Directive.spend(other),new CatalogueHistory.Delta(1,1)));
        assertThrows(IllegalArgumentException.class,() -> CatalogueHistory.apply(begun.state(),begun.credit(),CatalogueHistory.Directive.spend(definition),new CatalogueHistory.Delta(101,1)));
        assertEquals(begun.state(),CatalogueHistory.apply(begun.state(),begun.credit(),CatalogueHistory.Directive.spend(definition),new CatalogueHistory.Delta(0,0)).state());
        assertArrayEquals(begun.credit().encode(),CatalogueHistory.Credit.decode(begun.credit().encode()).encode());
        assertEquals(new CatalogueHistory.Delta(10,0),CatalogueHistory.Delta.replacement(new byte[5],new byte[2],new byte[12]));
    }
}
