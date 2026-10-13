package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.buffer.Buf;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Resource refusals and ambiguous native completion against actual restore catalogue and DATA owners. */
class RatisDatabaseRestoreQuotaTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private RatisDatabaseRestoreControllerTest fixture()throws IOException {
        var fixture=new RatisDatabaseRestoreControllerTest();fixture.root=Files.createDirectory(root.resolve("fixture"));return fixture;
    }
    private static ReplicaDescriptor member(){return new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"restored","localhost:19010",ReplicaDescriptor.Role.VOTER);}
    private static boolean causedBy(Throwable failure,String text){for(var current=failure;current!=null;current=current.getCause())if(current.getMessage()!=null && current.getMessage().contains(text))return true;return false;}
    private static void complete(RatisDatabaseRestoreController restore)throws IOException {
        for(int i=0;i<1000 && restore.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)restore.step(1);
        assertEquals(RatisDatabaseRestoreController.Phase.READY,restore.phase());
    }
    private static void requireData(RatisDatabaseRestoreController restore,RatisDatabaseRestoreControllerTest.Target target)throws IOException {
        var ledger=restore.issuedLedger();assertEquals(1,ledger.grants().size());
        for(int n=0;n<restore.plan().domains().size();n++){var domain=restore.plan().domains().get(n);var logical=new RatisLogicalClient(domain.schema(),target.routes.get(domain.schema().domainId()),ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());assertArrayEquals(new byte[]{(byte)(11*(n+1))},logical.get(n==0 ? 17 : 18,new Keys(Buf.wrap(new byte[]{7}))).toByteArray());if(n==0)assertNull(logical.get(99,new Keys(Buf.wrap(new byte[]{7}))));}
    }
    @Test void nodeMetadataRefusalPrecedesNativeOwnersAndSameFrozenRestoreRetriesAfterCapacityIncrease()throws Exception {
        var fixture=fixture();Path original=root.resolve("original"),archiveRoot=root.resolve("archive");var source=fixture.twoDomainArchive(original,archiveRoot);assertFalse(Files.exists(original));Path journal=Files.createDirectory(root.resolve("restore"));
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var target=fixture.new Target()) {
            target.separatePools=true;var defaults=target.runtimeResources;target.runtimeResources=new RatisNodeRuntime.ResourceLimits(defaults.node(),defaults.pool(),defaults.maximumSourceImageDiskBytes(),1,defaults.maximumOperatorCacheBytes(),defaults.maximumNativeCacheBytes());byte[] plan;
            try(var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(member()),UUID.randomUUID(),10,target)) {
                plan=restore.plan().encode();var failure=assertThrows(IOException.class,()->restore.step(1));assertTrue(causedBy(failure,"Node durable metadata ceiling exhausted"));assertEquals(RatisDatabaseRestoreController.Phase.PROVISION,restore.phase());assertNull(target.node);assertNull(target.fake.machines);assertFalse(target.fake.created);for(var pool:target.plan.pools().values())assertFalse(Files.exists(pool.nativePath(fixture.root.resolve("fresh-node")).resolve("CURRENT")));assertFalse(Files.exists(fixture.root.resolve("fresh-node/metadata-claims")));assertThrows(OverlappingFileLockException.class,()->RatisDatabaseRestoreController.reopen(journal,archive,target));
            }
            target.runtimeResources=defaults;
            try(var restore=RatisDatabaseRestoreController.reopen(journal,archive,target)){assertArrayEquals(plan,restore.plan().encode());complete(restore);assertEquals(2,target.plan.pools().size());requireData(restore,target);assertTrue(Files.isRegularFile(fixture.root.resolve("fresh-node/metadata-claims")));}
        }
    }
    @Test void unknownNativeDirectoryForceRetainsClaimsAndExactJournalUntilColdRecovery()throws Exception {
        var fixture=fixture();Path original=root.resolve("original"),archiveRoot=root.resolve("archive");var source=fixture.twoDomainArchive(original,archiveRoot);assertFalse(Files.exists(original));Path journal=Files.createDirectory(root.resolve("restore"));
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var target=fixture.new Target()) {
            target.separatePools=true;byte[] plan,claims;UUID catalogue;
            try(var restore=RatisDatabaseRestoreController.create(journal,archive,UUID.randomUUID(),List.of(member()),UUID.randomUUID(),10,target)) {
                plan=restore.plan().encode();catalogue=restore.plan().catalogue().groupId();for(int i=0;i<100 && restore.phase()!=RatisDatabaseRestoreController.Phase.GENESIS;i++)restore.step(1);assertEquals(RatisDatabaseRestoreController.Phase.GENESIS,restore.phase());claims=Files.readAllBytes(fixture.root.resolve("fresh-node/metadata-claims"));var once=new AtomicBoolean();target.store(catalogue).catalogueHistoryWriteObserver(()->{if(once.compareAndSet(false,true))throw new org.rocksdb.RocksDBException("Unknown native directory force after sync write");});
                var failure=assertThrows(IOException.class,()->restore.step(1));assertTrue(causedBy(failure,"Unknown native directory force after sync write"));assertTrue(once.get());assertEquals(RatisDatabaseRestoreController.Phase.GENESIS,restore.phase());assertArrayEquals(claims,Files.readAllBytes(fixture.root.resolve("fresh-node/metadata-claims")));assertThrows(OverlappingFileLockException.class,()->RatisDatabaseRestoreController.reopen(journal,archive,target));try(var lock=FileChannel.open(fixture.root.resolve("fresh-node/node.lock"),StandardOpenOption.WRITE)){assertThrows(OverlappingFileLockException.class,lock::tryLock);}
            }
            target.cold();assertArrayEquals(claims,Files.readAllBytes(fixture.root.resolve("fresh-node/metadata-claims")));assertFalse(Files.exists(original));
            try(var restore=RatisDatabaseRestoreController.reopen(journal,archive,target)){assertArrayEquals(plan,restore.plan().encode());complete(restore);assertEquals(2,target.plan.pools().size());requireData(restore,target);assertArrayEquals(claims,Files.readAllBytes(fixture.root.resolve("fresh-node/metadata-claims")));var c=restore.plan().catalogue();assertEquals(2,new RatisDatabaseDomainRoster(target.client,restore.plan().roster().clusterId(),restore.plan().roster().databaseId(),c).readComplete().directories().size());}
        }
    }
}
