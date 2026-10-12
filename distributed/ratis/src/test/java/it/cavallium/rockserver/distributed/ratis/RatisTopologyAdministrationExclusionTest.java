package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Ordered native metadata CAS, controlled fixtures only; no server or topology execution claim. */
class RatisTopologyAdministrationExclusionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(int n) { return new UUID(95,n); }
    TopologyChange.Definition split(RangeDirectory source) {
        var parent=source.ranges().getFirst(); byte[] boundary={64};
        var left=new RangeDescriptor(id(1),id(2),1,parent.configurationVersion(),parent.lowerInclusive(),boundary,RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
        var right=new RangeDescriptor(id(3),id(4),1,parent.configurationVersion(),boundary,parent.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
        var ranges=new ArrayList<RangeDescriptor>(List.of(left,right)); ranges.addAll(source.ranges().subList(1,source.ranges().size()));
        var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),ranges);
        return new TopologyChange.Definition(id(5),source,target);
    }
    RangeDirectory next(RangeDirectory source) { return new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),source.ranges()); }
    @Test void topologyAndSchemaBeginsExcludeEachOtherAtomicallyAndTerminalHeadsResume() throws Exception {
        for(boolean topologyWins:List.of(true,false)) {
            var support=new RatisSchemaAdministrationTest();
            try(var f=support.outer.new Fixture(root.resolve("schema-"+topologyWins))) {
                var client=support.client(f); var catalogue=support.catalogue(client); var administration=support.admin(client); var sourceSchema=support.initial(); var source=support.directory(sourceSchema,0);
                var columns=new ArrayList<>(sourceSchema.columns()); columns.add(support.column(8,id(20),"added")); var targetSchema=support.next(sourceSchema,columns);
                support.publishSchema(f,sourceSchema); catalogue.execute(catalogue.prepare(support.operation(),null,source)); support.publishSchema(f,targetSchema);
                var schema=new SchemaChange.Definition(id(21),support.operation(),source,sourceSchema,targetSchema); administration.execute(administration.prepareDefinition(support.operation(),schema,null,null,null));
                var topology=new RatisTopologyCatalogue(client,source.clusterId(),source.databaseId(),support.outer.catalogue,1,new byte[32],support.outer.directoryColumn); var definition=split(source);
                topology.execute(topology.prepareDefinition(support.operation(),definition,null));
                var topologyBegin=topology.prepareBegin(support.operation(),definition,null,null,null); var schemaBegin=administration.prepareBegin(schema,null,null,null); var ordinary=catalogue.prepare(support.operation(),source,next(source));
                assertEquals(6,schemaBegin.predicates().size()); var legacySchema=new ArrayList<>(schemaBegin.predicates()); legacySchema.removeLast();
                var schemaRule=RatisSchemaAdministration.validator(source.clusterId(),source.databaseId(),support.outer.catalogue,1,new byte[32],support.outer.directoryColumn);
                assertThrows(IllegalArgumentException.class,()->schemaRule.accept(copy(schemaBegin,legacySchema))); assertArrayEquals(RatisTopologyCatalogue.headKey(source.domainId()),schemaBegin.predicates().getLast().key());
                if(topologyWins) {
                    assertEquals(CommandResult.Status.SUCCESS,topology.execute(topologyBegin).status()); assertEquals(CommandResult.Status.CONFLICT,administration.execute(schemaBegin).status()); assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(ordinary).status());
                    var active=topology.readHead(source.domainId()); assertThrows(IllegalArgumentException.class,()->administration.prepareBegin(schema,null,null,null,null,active));
                    assertThrows(IllegalArgumentException.class,()->catalogue.prepareAfterAdministration(support.operation(),source,next(source),null,null,active));
                    topology.execute(topology.prepareAbort(support.operation(),active)); var aborted=topology.readHead(source.domainId());
                    assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(ordinary).status(),"Saved absent-head conflict is never rebased after abort");
                    assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareAfterAdministration(support.operation(),source,next(source),null,null,aborted)).status());
                    var resumedSchema=new SchemaChange.Definition(id(22),support.operation(),next(source),sourceSchema,targetSchema);
                    administration.execute(administration.prepareDefinition(support.operation(),resumedSchema,null,null,null,null,aborted));
                    assertEquals(CommandResult.Status.SUCCESS,administration.execute(administration.prepareBegin(resumedSchema,null,null,null,null,aborted)).status());
                } else {
                    assertEquals(CommandResult.Status.SUCCESS,administration.execute(schemaBegin).status()); assertEquals(CommandResult.Status.CONFLICT,topology.execute(topologyBegin).status()); assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(ordinary).status());
                    var active=administration.readHead(source.domainId()); administration.execute(administration.prepareAdvance(support.operation(),active,active.status().abort())); var aborted=administration.readHead(source.domainId());
                    assertEquals(CommandResult.Status.SUCCESS,topology.execute(topology.prepareBegin(support.operation(),definition,null,aborted,null)).status());
                }
            }
        }
    }
    @Test void topologyAndImportBeginsRaceOnSameMixedActiveFencedDirectoryWithoutDirectorySideEffects() throws Exception {
        for(boolean topologyWins:List.of(true,false)) {
            var fixtureOwner=new RatisRangeImportCatalogueTest(); fixtureOwner.root=root.resolve("import-"+topologyWins);
            try(var f=fixtureOwner.new Fixture()) {
                var template=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(500)).sourceDirectory(); var old=template.ranges().getFirst();
                var active=new RangeDescriptor(id(30),id(31),1,old.configurationVersion(),null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,old.replicas());
                var fenced=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),new byte[]{(byte)128},null,RangeDescriptor.TopologyState.FENCED,old.replicas());
                var source=new RangeDirectory(template.clusterId(),template.databaseId(),template.domainId(),template.schemaEpoch(),template.schemaDigest(),template.generation(),template.columns(),0,List.of(active,fenced)); f.seed(source);
                var sequence=new AtomicLong(1); var topology=new RatisTopologyCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); var definition=split(source);
                topology.execute(topology.prepareDefinition(f.operation(sequence.getAndIncrement()),definition,null));
                UUID importId=id(32); var manifest=new NativeSstImportManifest(importId,fenced.groupId(),fenced.rangeEpoch(),source.domainId(),source.schemaDigest(),fenced.lowerInclusive(),fenced.upperExclusive(),List.of(new NativeSstImportManifest.Column(source.columns().iterator().next(),"imported",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS);
                var imported=new RangeImportDefinition(importId,f.operation(500),source,List.of(manifest));
                for(int i=0;i<RatisRangeImportCatalogue.definitionArtifact(imported).chunks();i++) f.catalogue.execute(f.catalogue.prepareDefinitionChunk(f.operation(sequence.getAndIncrement()),imported,i)); f.catalogue.execute(f.catalogue.prepareDefinitionCommit(f.operation(sequence.getAndIncrement()),imported));
                var topologyBegin=topology.prepareBegin(f.operation(sequence.getAndIncrement()),definition,null,null,null); var importBegin=f.catalogue.prepareBegin(f.operation(sequence.getAndIncrement()),imported,null,null);
                assertEquals(6,importBegin.predicates().size()); var legacyImport=new ArrayList<>(importBegin.predicates()); legacyImport.removeLast();
                assertThrows(IllegalArgumentException.class,()->f.pure.accept(copy(importBegin,legacyImport))); assertArrayEquals(RatisTopologyCatalogue.headKey(source.domainId()),importBegin.predicates().getLast().key());
                if(topologyWins) {
                    assertEquals(CommandResult.Status.SUCCESS,topology.execute(topologyBegin).status()); assertEquals(CommandResult.Status.CONFLICT,f.catalogue.execute(importBegin).status());
                    var state=topology.readHead(source.domainId()); assertThrows(IllegalArgumentException.class,()->f.catalogue.prepareBegin(f.operation(sequence.getAndIncrement()),imported,null,null,state));
                    topology.execute(topology.prepareAbort(f.operation(sequence.getAndIncrement()),state)); var aborted=topology.readHead(source.domainId());
                    assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.prepareBegin(f.operation(sequence.getAndIncrement()),imported,null,null,aborted)).status());
                } else {
                    assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(importBegin).status()); assertEquals(CommandResult.Status.CONFLICT,topology.execute(topologyBegin).status());
                }
                var ordinary=new RatisCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); assertArrayEquals(RangeDirectoryCodec.encode(source),RangeDirectoryCodec.encode(ordinary.read(source.domainId())),"BEGIN arbitration does not publish either target directory");
            }
        }
    }
    TopologyChange.Proof proof(TopologyChange.State state,TopologyChange.Stage stage,TopologyChange.Participant participant) {
        var owner=participant.owners().stream().filter(o->o.role()==ReplicaDescriptor.Role.VOTER).findFirst().orElseThrow();
        var predecessor=switch(stage) { case FREEZE -> state.header().sourceDirectoryDigest(); case SEED -> state.freezeDigest(); case ACTIVATE -> state.seeded().get(new TopologyChange.Key(participant.groupId(),owner.replicaId())).digest(); };
        return TopologyChange.Proof.of(stage,state.header(),participant.groupId(),owner.replicaId(),stage==TopologyChange.Stage.SEED ? new ApplyPosition(0,0) : new ApplyPosition(1,1),new byte[32],predecessor);
    }
    @Test void everyActiveTopologyPhaseBlocksOrdinaryPublicationAndTerminalGuardIsExact() throws Exception {
        var fixtureOwner=new RatisRangeImportCatalogueTest(); fixtureOwner.root=root.resolve("validation");
        try(var f=fixtureOwner.new Fixture()) {
            var template=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(100)).sourceDirectory(); var old=template.ranges().getFirst();
            var source=new RangeDirectory(template.clusterId(),template.databaseId(),template.domainId(),template.schemaEpoch(),template.schemaDigest(),template.generation(),template.columns(),0,List.of(new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas())));
            var catalogue=new RatisCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); var definition=split(source); var prepared=TopologyChange.State.begin(definition.header()); var states=new ArrayList<TopologyChange.State>(List.of(prepared,prepared.beginFencing()));
            var fenced=states.getLast(); for(var p:fenced.header().parents()) fenced=fenced.frozen(proof(fenced,TopologyChange.Stage.FREEZE,p)); states.add(fenced);
            var seeded=fenced; for(var p:seeded.header().targets()) seeded=seeded.seeded(proof(seeded,TopologyChange.Stage.SEED,p)); states.add(seeded); var published=seeded.publish(); states.add(published);
            var active=published; for(var p:active.header().targets()) active=active.activated(proof(active,TopologyChange.Stage.ACTIVATE,p)); states.add(active);
            for(var state:states) assertThrows(IllegalArgumentException.class,()->catalogue.prepareAfterAdministration(f.operation(200),source,next(source),null,null,state));
            var aborted=prepared.abort(); var command=catalogue.prepareAfterAdministration(f.operation(201),source,next(source),null,null,aborted); assertEquals(4,command.predicates().size()); assertArrayEquals(aborted.encode(),command.predicates().getLast().expectedValue());
            var retired=active.retire(); assertDoesNotThrow(()->catalogue.prepareAfterAdministration(f.operation(202),definition.targetDirectory(),next(definition.targetDirectory()),null,null,retired));
            var validator=RatisCatalogue.validator(f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            var without=new ArrayList<>(command.predicates()); without.removeLast(); assertThrows(IllegalArgumentException.class,()->validator.accept(copy(command,without)));
            var wrongColumn=new ArrayList<>(command.predicates()); wrongColumn.set(3,new KeyPredicate(UUID.randomUUID(),command.predicates().get(3).key(),aborted.encode())); assertThrows(IllegalArgumentException.class,()->validator.accept(copy(command,wrongColumn)));
            var bucket=new ArrayList<>(command.predicates()); bucket.set(3,KeyPredicate.bucketMember(f.outer.directoryColumn,command.predicates().get(3).key(),List.of(new byte[]{1}),aborted.encode())); assertThrows(IllegalArgumentException.class,()->validator.accept(copy(command,bucket)));
            for(var foreign:List.of(
                    new RangeDirectory(UUID.randomUUID(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation(),source.columns(),0,source.ranges()),
                    new RangeDirectory(source.clusterId(),UUID.randomUUID(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation(),source.columns(),0,source.ranges()),
                    new RangeDirectory(source.clusterId(),source.databaseId(),UUID.randomUUID(),source.schemaEpoch(),source.schemaDigest(),source.generation(),source.columns(),0,source.ranges()))) {
                var foreignHead=TopologyChange.State.begin(split(foreign).header()).abort(); assertThrows(IllegalArgumentException.class,()->catalogue.prepareAfterAdministration(f.operation(203),source,next(source),null,null,foreignHead));
            }
            var wrongDomain=new ArrayList<>(command.predicates()); wrongDomain.set(3,new KeyPredicate(f.outer.directoryColumn,RatisTopologyCatalogue.headKey(id(99)),aborted.encode())); assertThrows(IllegalArgumentException.class,()->validator.accept(copy(command,wrongDomain)));
        }
    }
    DistributedCommand copy(DistributedCommand command,List<KeyPredicate> predicates) { return new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),command.mutations(),predicates); }
}
