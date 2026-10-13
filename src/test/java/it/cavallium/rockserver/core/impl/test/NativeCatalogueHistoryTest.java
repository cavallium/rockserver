package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.io.IOException;
import it.cavallium.rockserver.core.config.ConfigParser;
import org.slf4j.LoggerFactory;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(120)
class NativeCatalogueHistoryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    static final UUID LEDGER=new UUID(0,900);
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(1024*1024,32L*1024*1024,2);
    final class Fixture implements AutoCloseable {
        final DistributedStoragePool pool;final ReplicatedRangeStore store;
        Fixture(Path path) throws Exception {pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));store=pool.openRange(new RangeOwnership.Owner(helper.group,1),new RangeOwnership.Interval(helper.domain,Set.of(helper.column,LEDGER),null,null),new byte[32],Map.of(helper.column,"directories",LEDGER,"sessions"),Set.of());store.initializeConfiguration(new byte[]{1});if(store.applied()==null)store.applyNoop(new ApplyPosition(1,0));}
        @Override public void close() throws Exception {pool.close();}
    }
    CommandResult apply(Fixture fixture,int index,DistributedCommand command) throws Exception {return CommandResult.decode(fixture.store.apply(new ApplyPosition(1,index),command));}

    static byte[] key(int n){return ByteBuffer.allocate(33).put((byte)0x53).put(new byte[28]).putInt(n).array();}
    static boolean history(byte[] key){return key.length==33 && key[0]==0x53;}
    static final byte[] DEFINITION=new byte[32];
    void configure(Fixture f) throws Exception {
        f.store.enableCatalogueScopeCompaction();f.store.configureCatalogueHistory(helper.column,LEDGER,NativeCatalogueHistoryTest::history,command->{var key=command.mutations().getFirst().key();int id=key.length==33 ? ByteBuffer.wrap(key,29,4).getInt() : -1;return id==70000 ? CatalogueHistory.Directive.begin(DEFINITION,1024,3) : id==70001 ? CatalogueHistory.Directive.spend(DEFINITION) : id==70002 ? CatalogueHistory.Directive.terminal(DEFINITION) : CatalogueHistory.Directive.unreserved();});
    }
    DistributedCommand command(long sequence,List<Mutation> mutations,List<KeyPredicate> predicates){return new DistributedCommand(helper.group,1,new byte[32],new OperationId(helper.user,1,sequence),mutations,predicates);}
    DistributedCommand put(long sequence,int id,byte[] previous,byte[] value){return command(sequence,List.of(new Mutation(Mutation.Kind.PUT,helper.column,key(id),value)),List.of(new KeyPredicate(helper.column,key(id),previous)));}
    @Test void nativeActualAndPromisedCapacityFundsCompletionThenSurvivesColdAndImage() throws Exception {
        var path=root.resolve("source");var image=root.resolve("catalogue.image");int index=0;long sequence=0;CatalogueHistory.State saved;
        try(var f=new Fixture(path)) {
            configure(f);
            for(int first=0;first<65529;first+=256){var mutations=new ArrayList<Mutation>();for(int n=first;n<Math.min(first+256,65529);n++)mutations.add(new Mutation(Mutation.Kind.PUT,helper.column,key(n),new byte[]{1}));assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,command(sequence++,mutations,List.of())).status());}
            var begin=put(sequence++,70000,null,new byte[]{1});assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,begin).status());
            assertEquals(3,f.store.catalogueHistoryCapacity().promisedRows());
            assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,put(sequence++,71000,null,new byte[]{1})).status());
            var rejected=put(sequence++,71001,null,new byte[]{1});var before=f.store.catalogueHistoryCapacity();
            assertEquals(CatalogueHistory.MAX_ROWS,before.actualRows()+before.promisedRows());
            assertEquals(CommandResult.Status.CONFLICT,apply(f,++index,rejected).status());assertEquals(before,f.store.catalogueHistoryCapacity());
            assertEquals(CommandResult.Status.CONFLICT,apply(f,++index,rejected).status());assertNull(f.store.readCurrentPoint(helper.column,key(71001)));
            var failed=put(sequence++,70001,new byte[]{9},new byte[]{2});assertEquals(CommandResult.Status.CONFLICT,apply(f,++index,failed).status());assertEquals(before,f.store.catalogueHistoryCapacity());
            assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,put(sequence++,70001,null,new byte[]{2})).status());assertEquals(2,f.store.catalogueHistoryCapacity().promisedRows());
            assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,put(sequence++,70002,null,new byte[]{3})).status());saved=f.store.catalogueHistoryCapacity();assertEquals(0,saved.promisedRows());
            assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,begin).status());assertEquals(saved,f.store.catalogueHistoryCapacity());
            try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits);}
        }
        NativeRangeImage.inspectCatalogueHistory(image,limits,helper.column,NativeCatalogueHistoryTest::history);
        try(var f=new Fixture(path)){configure(f);assertEquals(saved,f.store.catalogueHistoryCapacity());}
        try(var f=new Fixture(root.resolve("receiver"))){configure(f);f.store.fence();f.store.installSnapshot(image,limits);f.store.activate();assertEquals(saved,f.store.catalogueHistoryCapacity());}
    }
    @Test void uncertainWriteRecoversExactInventoryAndCreditWithoutDoubleCharging() throws Exception {
        var path=root.resolve("unknown");var begin=put(0,70000,null,new byte[]{1});
        try(var f=new Fixture(path)){configure(f);f.store.catalogueHistoryWriteObserver(()->{throw new org.rocksdb.RocksDBException("unknown after native history write");});assertThrows(org.rocksdb.RocksDBException.class,()->apply(f,1,begin));}
        try(var f=new Fixture(path)){configure(f);var original=f.store.catalogueHistoryCapacity();assertEquals(3,original.promisedRows());assertEquals(CommandResult.Status.SUCCESS,apply(f,2,begin).status());assertEquals(original,f.store.catalogueHistoryCapacity());}
    }
    RangeDirectory directory(int id,long generation){var owner=new ReplicaDescriptor(new UUID(0,100),new UUID(0,101),new UUID(0,102),"native-directory","127.0.0.1:9000",ReplicaDescriptor.Role.VOTER);var range=new RangeDescriptor(new UUID(0,103),new UUID(0,104),1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));return new RangeDirectory(new UUID(0,20),new UUID(0,21),new UUID(0,9000+id),1,new byte[32],generation,Set.of(new UUID(0,105)),0,List.of(range));}
    DistributedCommand directoryPut(long sequence,RangeDirectory before,RangeDirectory next){byte[] key=ByteBuffer.allocate(16).putLong(next.domainId().getMostSignificantBits()).putLong(next.domainId().getLeastSignificantBits()).array();return command(sequence,List.of(new Mutation(Mutation.Kind.PUT,helper.column,key,RangeDirectoryCodec.encode(next))),List.of(new KeyPredicate(helper.column,key,before==null ? null : RangeDirectoryCodec.encode(before))));}
    @Test void currentDirectorySlotsHaveIndependentFiniteFullGrowthReservations() throws Exception {
        var path=root.resolve("directories");var image=root.resolve("directories.image");int index=0;CatalogueHistory.State history;
        try(var f=new Fixture(path)) {
            configure(f);for(int n=0;n<DatabaseDomainRoster.MAX_DOMAINS;n++)assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,directoryPut(n,null,directory(n,1))).status());
            history=f.store.catalogueHistoryCapacity();assertEquals(1,history.actualRows());assertEquals(0,history.promisedRows());
            var newSlot=directoryPut(100,null,directory(100,1));assertEquals(CommandResult.Status.CONFLICT,apply(f,++index,newSlot).status());
            assertEquals(CommandResult.Status.SUCCESS,apply(f,++index,directoryPut(101,directory(0,1),directory(0,2))).status());assertEquals(history,f.store.catalogueHistoryCapacity());
            try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits);}
        }
        NativeRangeImage.inspectCatalogueHistory(image,limits,helper.column,NativeCatalogueHistoryTest::history);
        try(var f=new Fixture(path)){configure(f);assertEquals(history,f.store.catalogueHistoryCapacity());assertEquals(CommandResult.Status.CONFLICT,apply(f,++index,directoryPut(102,null,directory(101,1))).status());}
    }

    static int metadataValue(byte[] image,int tag,int valueBytes) {
        byte[] key=ByteBuffer.allocate(tag==0x79 ? 49 : 17).putLong(0).putLong(1).put((byte)tag).array();
        for(int offset=0;offset+9+key.length+valueBytes<=image.length;offset++)if(image[offset]==2 && ByteBuffer.wrap(image,offset+1,4).getInt()==key.length && ByteBuffer.wrap(image,offset+5,4).getInt()==valueBytes && Arrays.equals(image,offset+9,offset+9+key.length,key,0,key.length))return offset+9+key.length;
        throw new AssertionError("Native metadata record absent");
    }
    static byte[] reseal(byte[] bytes) throws Exception {byte[] checksum=MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes,bytes.length-32));System.arraycopy(checksum,0,bytes,bytes.length-32,32);return bytes;}
    @Test void validChecksumLedgerAndCreditForgeriesFailBeforeTargetMutationAndColdExposure() throws Exception {
        var image=root.resolve("valid.image");byte[] original;CatalogueHistory.State ledger;
        try(var f=new Fixture(root.resolve("donor"))){configure(f);apply(f,1,put(0,70000,null,new byte[]{1}));ledger=f.store.catalogueHistoryCapacity();try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,pin.interval(),limits);}original=Files.readAllBytes(image);}
        for(int tag:List.of(0x78,0x79)) {
            byte[] modified=original.clone();int offset=metadataValue(modified,tag,tag==0x78 ? CatalogueHistory.STATE_BYTES : CatalogueHistory.CREDIT_BYTES);
            if(tag==0x78)ByteBuffer.wrap(modified,offset+8,8).putLong(ledger.actualRows()+1);else modified[offset]=1;
            var forged=root.resolve("forged-"+tag+".image");Files.write(forged,reseal(modified));
            assertThrows(IOException.class,()->NativeRangeImage.inspectCatalogueHistory(forged,limits,helper.column,NativeCatalogueHistoryTest::history));
            try(var f=new Fixture(root.resolve("target-"+tag))){configure(f);var marker=f.store.applied();f.store.fence();assertThrows(IOException.class,()->f.store.installSnapshot(forged,limits));assertEquals(marker,f.store.applied());assertFalse(f.store.pendingInstallation());var dbField=ReplicatedRangeStore.class.getDeclaredField("db");dbField.setAccessible(true);var columnsField=ReplicatedRangeStore.class.getDeclaredField("columns");columnsField.setAccessible(true);@SuppressWarnings("unchecked")var columns=(Map<UUID,org.rocksdb.ColumnFamilyHandle>)columnsField.get(f.store);assertNull(((org.rocksdb.RocksDB)dbField.get(f.store)).get(columns.get(helper.column),key(70000)));}
        }
        try(var f=new Fixture(root.resolve("cold-corrupt"))) {
            configure(f);apply(f,1,put(0,70000,null,new byte[]{1}));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db");dbField.setAccessible(true);var stateField=ReplicatedRangeStore.class.getDeclaredField("state");stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store);var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=ByteBuffer.allocate(17).putLong(0).putLong(1).put((byte)0x78).array();var saved=f.store.catalogueHistoryCapacity();db.put(state,key,new CatalogueHistory.State(saved.actualBytes()+1,saved.actualRows(),saved.promisedBytes(),saved.promisedRows()).encode());
        }
        try(var f=new Fixture(root.resolve("cold-corrupt"))){assertThrows(IllegalStateException.class,()->configure(f));}
    }

    @Test void exactCatalogueColumnsQualifyHistoryWhileLedgerOnlyEffectsStayIndependent() throws Exception {
        try(var f=new Fixture(root.resolve("two-columns"))) {
            f.store.enableCatalogueScopeCompaction();
            assertThrows(IllegalArgumentException.class,()->f.store.configureCatalogueHistory(helper.column,helper.column,NativeCatalogueHistoryTest::history,command->CatalogueHistory.Directive.unreserved()));
            assertThrows(IllegalArgumentException.class,()->f.store.configureCatalogueHistory(helper.column,new UUID(0,901),NativeCatalogueHistoryTest::history,command->CatalogueHistory.Directive.unreserved()));
            var calls=new java.util.concurrent.atomic.AtomicInteger();f.store.configureCatalogueHistory(helper.column,LEDGER,NativeCatalogueHistoryTest::history,command->{calls.incrementAndGet();return CatalogueHistory.Directive.unreserved();});
            var before=f.store.catalogueHistoryCapacity();var ledger=SessionEpochLedger.initial(new UUID(0,20),new UUID(0,21),0);byte[] key=ByteBuffer.allocate(16).putLong(0).putLong(21).array();
            var write=command(0,List.of(new Mutation(Mutation.Kind.PUT,LEDGER,key,ledger.encode())),List.of(new KeyPredicate(LEDGER,key,null)));
            assertEquals(CommandResult.Status.SUCCESS,apply(f,1,write).status());assertEquals(0,calls.get());assertEquals(before,f.store.catalogueHistoryCapacity());assertArrayEquals(ledger.encode(),f.store.readCurrentPoint(LEDGER,key));
            assertEquals(CommandResult.Status.SUCCESS,apply(f,2,put(1,10,null,new byte[]{1})).status());assertEquals(1,calls.get());assertEquals(2,f.store.catalogueHistoryCapacity().actualRows());
        }
    }

}
