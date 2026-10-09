package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real catalogue/journal I/O; controlled transport outcomes test ownership, not quorum behavior. */
class RatisSchemaClockReconciliationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void terminalChildSurvivesFailedReconciliationCheckpointAcrossTwoParticipants() throws Exception {
        var helper=new RatisSchemaFenceClientTest(); var single=helper.definition(); UUID second=new UUID(1,7);
        var firstRange=new RangeDescriptor(new UUID(3,1),helper.group,1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(helper.replica));
        var secondRange=new RangeDescriptor(new UUID(3,4),second,1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(helper.replica));
        var directory=new RangeDirectory(helper.cluster,helper.database,helper.domain,1,single.sourceSchema().digest(),0,single.sourceSchema().columnIds(),0,List.of(firstRange,secondRange));
        var definition=new SchemaChange.Definition(single.changeId(),single.beginOperation(),directory,single.sourceSchema(),single.targetSchema());
        var outer=new RatisSessionEpochsTest(); Path journals=Files.createDirectory(root.resolve("journals"));
        var commands=new ArrayList<DistributedCommand>(); var unknown=new java.util.concurrent.atomic.AtomicBoolean(true);
        var poison=new java.util.concurrent.atomic.AtomicBoolean(true);
        var transport=new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                commands.add(command); if (unknown.get()) throw new IOException("Unknown outcome");
                if (Arrays.equals(command.schemaDigest(),definition.sourceSchema().digest())) {
                    if (poison.getAndSet(false)) Files.createDirectory(journals.resolve("schema-reconciliation.bin.pending"));
                    return new CommandResult(CommandResult.Status.STALE_SCHEMA,List.of());
                }
                return new CommandResult(CommandResult.Status.SUCCESS,List.of());
            }
        };
        try (var catalogue=outer.new Fixture(root.resolve("catalogue")); var client=new RatisRoutedClient(directory,transport)) {
            var ledger=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,ledger));
            var originals=new HashMap<UUID,OperationId>();
            try (var driver=new RatisSessionPropagation(journals,catalogue.epochs,client,2)) {
                driver.begin(); assertFalse(driver.step(2).complete());
                for (var command:commands) originals.put(command.groupId(),command.operationId()); assertEquals(2,originals.size());
                var install=new NativeSchemaInstallProof(definition.header(),helper.group,new ApplyPosition(1,5),new ApplyPosition(1,6));
                client.refreshSchema(definition,new NativeSchemaActivationProof(install,outer.catalogue,new ApplyPosition(1,20),new ApplyPosition(1,7)));
                unknown.set(false);
                assertThrows(IOException.class,() -> driver.reconcileSchemaRefresh(1));
                assertThrows(IOException.class,() -> driver.reconcileSchemaRefresh(1),"Failed checkpoint must fence the owner");
            }
            try (var child=new RatisSessionMaintenance(journals.resolve(helper.group+"-1.clock"),catalogue.epochs,client,helper.group)) {
                assertNull(child.pendingCommand(),"Child terminal outcome was durable before checkpoint failure");
            }
            try (var driver=new RatisSessionPropagation(journals,catalogue.epochs,client,2)) {
                assertTrue(driver.schemaRefreshPending()); driver.reconcileSchemaRefresh(1); driver.reconcileSchemaRefresh(1);
                assertFalse(driver.schemaRefreshPending()); assertEquals(2,driver.progress().round());
                assertTrue(driver.step(2).complete());
            }
            for (var group:originals.keySet()) {
                var old=commands.stream().filter(command -> command.groupId().equals(group) && Arrays.equals(command.schemaDigest(),definition.sourceSchema().digest())).toList();
                assertEquals(2,old.size(),"One unknown attempt and one terminal attempt, with no replay of completed child");
                assertTrue(old.stream().allMatch(command -> command.operationId().equals(originals.get(group))));
                var fresh=commands.stream().filter(command -> command.groupId().equals(group) && Arrays.equals(command.schemaDigest(),definition.targetSchema().digest())).findFirst().orElseThrow();
                assertEquals(originals.get(group).sequence()+1,fresh.operationId().sequence());
            }
        }
    }
    @Test void unknownOldControlSurvivesRefreshAndRestartBeforeNewSchemaClockWork() throws Exception {
        var helper=new RatisSchemaFenceClientTest(); var definition=helper.definition();
        var outer=new RatisSessionEpochsTest();
        try (var catalogue=outer.new Fixture(root.resolve("catalogue"))) {
            var ledger=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,ledger)).status());
            var commands=new ArrayList<DistributedCommand>(); var fail=new java.util.concurrent.atomic.AtomicBoolean(true);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                    commands.add(command); if (fail.get()) throw new IOException("Unknown transport outcome");
                    return new CommandResult(Arrays.equals(command.schemaDigest(),definition.sourceSchema().digest()) ? CommandResult.Status.STALE_SCHEMA : CommandResult.Status.SUCCESS,List.of());
                }
            };
            try (var client=new RatisRoutedClient(definition.sourceDirectory(),transport)) {
                Path journals=Files.createDirectory(root.resolve("journals")); byte[] original; OperationId operation;
                try (var driver=new RatisSessionPropagation(journals,catalogue.epochs,client,1)) {
                    driver.begin(); assertFalse(driver.step(1).complete());
                    original=DistributedCommandCodec.encode(commands.getFirst()); operation=commands.getFirst().operationId();
                    var install=new NativeSchemaInstallProof(definition.header(),helper.group,new ApplyPosition(1,5),new ApplyPosition(1,6));
                    var activation=new NativeSchemaActivationProof(install,outer.catalogue,new ApplyPosition(1,20),new ApplyPosition(1,7));
                    client.refreshSchema(definition,activation);
                    assertTrue(driver.schemaRefreshPending()); assertFalse(driver.reconcileSchemaRefresh(1).complete());
                    assertArrayEquals(original,DistributedCommandCodec.encode(commands.getLast()));
                }
                try (var stream=new RatisSessionMaintenance(journals.resolve(helper.group+"-1.clock"),catalogue.epochs,client,helper.group)) {
                    assertEquals(operation,stream.pendingCommand().operationId()); assertArrayEquals(original,DistributedCommandCodec.encode(stream.pendingCommand()));
                }
                fail.set(false);
                try (var driver=new RatisSessionPropagation(journals,catalogue.epochs,client,1)) {
                    assertTrue(driver.schemaRefreshPending()); var next=driver.reconcileSchemaRefresh(1);
                    assertEquals(2,next.round()); assertFalse(next.complete()); assertFalse(driver.schemaRefreshPending());
                    assertArrayEquals(original,DistributedCommandCodec.encode(commands.getLast()));
                    assertTrue(driver.step(1).complete()); var fresh=commands.getLast();
                    assertEquals(operation.sessionId(),fresh.operationId().sessionId()); assertEquals(operation.sequence()+1,fresh.operationId().sequence());
                    assertArrayEquals(definition.targetSchema().digest(),fresh.schemaDigest());
                }
            }
        }
    }
}
