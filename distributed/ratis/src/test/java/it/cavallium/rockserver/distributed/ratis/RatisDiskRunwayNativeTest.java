package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDiskRunwayNativeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void nativeWholePoolFlushRetainsActualHandlesAndConfiguredWalPathIsInCensus()throws Exception {
        var configured=new it.cavallium.rockserver.core.config.ConfigParser();configured.addSourceBytes(("database.global.wal-path=\""+root.resolve("separate-wal")+"\"").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try(var pool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(root.resolve("native"),configured.parse(),org.slf4j.LoggerFactory.getLogger(getClass()));var worker=Executors.newVirtualThreadPerTaskExecutor()){
            assertTrue(pool.storagePaths().contains(root.resolve("separate-wal")));var entered=new CountDownLatch(1);var release=new CountDownLatch(1);pool.localMaintenanceObserver(column->{entered.countDown();try{assertTrue(release.await(5,TimeUnit.SECONDS));}catch(InterruptedException failed){throw new AssertionError(failed);}});
            var flush=worker.submit(()->{pool.flushWholePool();return null;});assertTrue(entered.await(5,TimeUnit.SECONDS));assertFalse(flush.isDone());release.countDown();flush.get(5,TimeUnit.SECONDS);
            assertFalse(pool.storagePaths().isEmpty());
        }
    }
    @Test void emergencyDeniesNewOrdinaryWhileProtectedProgressAndCommittedReplayContinue()throws Exception {
        var clock=new java.util.concurrent.atomic.AtomicLong(10);var runway=new RatisDiskRunway(RatisDiskRunwayTest.policy(),clock::get);RatisDiskRunwayTest.publish(runway,10,500);
        try(var fixture=new RatisSchemaLifecycleAdmissionTest.Fixture(root,false,java.util.function.UnaryOperator.identity(),machine->machine.diskRunway(runway,RatisDiskRunwayTest.POOL))){
            var command=fixture.put(fixture.source,fixture.column,0,9);RatisDiskRunwayTest.observe(runway,10,50);assertThrows(java.io.IOException.class,()->fixture.propose(it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec.encode(command)));
            var bytes=it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec.encode(command);var entry=org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes))).build();var replay=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(fixture.machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).setLogEntry(entry).build();fixture.machine.applyTransaction(replay).get(5,TimeUnit.SECONDS);assertArrayEquals(new byte[]{9},fixture.store.readCurrentPoint(fixture.column,new byte[]{1}));
            fixture.index=1;
            // A real promised schema FENCE is protected metadata completion and remains admissible.
            var fence=fixture.fence(fixture.create);assertNotNull(fence);
        }
    }
}
