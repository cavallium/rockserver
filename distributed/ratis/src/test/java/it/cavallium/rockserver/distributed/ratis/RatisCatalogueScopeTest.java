package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.ratis.client.RaftClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Catalogue native CAS and leader admission; the Raft server is an identity proxy with no network. */
class RatisCatalogueScopeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void fullSchemaIssuanceAdmitsAppliesAndRetriesFrozenBytesAcrossImageActivation() throws Exception {
        try (var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("catalogue"),RatisRangeStateMachine.RetentionLimits.defaults(),
                command -> {},System::nanoTime,null,machine -> machine.role(RatisNodePlan.Role.CATALOGUE));
             var independent=new RatisRetentionAdmissionTest.Fixture(root.resolve("independent"),RatisRangeStateMachine.RetentionLimits.defaults())) {
            UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID();
            var columns=new ArrayList<DistributedSchema.Column>();
            for (int i=0;i<256;i++) columns.add(new DistributedSchema.Column(i+1,new UUID(0,i+1),"column"+i,
                    ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true),null));
            var schema=new DistributedSchema(cluster,database,domain,1,0,columns);
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> {
                if (method.getName().equals("getGroupId")) return f.group.getGroupId();
                throw new AssertionError("Preparation must not access transport: "+method);
            });
            var catalogue=new RatisSchemaCatalogue(client,cluster,database,f.groupId,1,new byte[32],f.column);
            var command=catalogue.prepare(new OperationId(f.session,1,0),schema);
            assertEquals(513,command.mutations().size()); assertEquals(513,command.predicates().size());
            byte[] frozen=DistributedCommandCodec.encode(command); assertTrue(frozen.length<=DistributedCommandCodec.MAX_ENCODED_BYTES);
            assertArrayEquals(frozen,DistributedCommandCodec.encode(DistributedCommandCodec.decode(frozen)));
            var scope=f.store.commandAccessScope(command); assertEquals(1,scope.spans().size()); assertTrue(scope.containsRange(f.column,null,null));
            assertTrue(f.store.estimateReceiptRetention(command).newReceipt());
            var admitted=f.propose(command); assertNull(admitted.getException()); assertEquals(1,f.machine.proposalFootprintStats().spans());
            var concurrent=f.propose(command); assertNull(concurrent.getException()); assertEquals(1,f.machine.proposalFootprintStats().ordinary());
            var separate=independent.propose(independent.put(0)); assertNull(separate.getException()); independent.machine.cancelTransaction(separate); independent.assertReleased();
            assertEquals(CommandResult.Status.SUCCESS,f.apply(admitted,1));
            assertFalse(f.store.estimateReceiptRetention(command).newReceipt());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(concurrent,2)); f.assertReleased();
            try (var pin=f.store.capture()) {
                for (var mutation:command.mutations()) assertArrayEquals(mutation.value(),pin.get(mutation.columnId(),mutation.key()));
            }
            assertEquals(2,f.machine.takeSnapshot()); f.machine.pause(); f.machine.reinitialize();
            assertEquals(1,f.store.commandAccessScope(command).spans().size());
            var retry=f.propose(DistributedCommandCodec.decode(frozen)); assertNull(retry.getException());
            assertEquals(0,f.machine.proposalFootprintStats().ordinary());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(retry,3)); f.assertReleased();
            assertArrayEquals(frozen,DistributedCommandCodec.encode(command)); assertEquals(1,f.store.receiptUsage().retainedCount());
        }
    }
}
