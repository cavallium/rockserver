package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(120)
class NativeCatalogueHistoryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    static byte[] key(int n){return ByteBuffer.allocate(33).put((byte)0x53).put(new byte[28]).putInt(n).array();}
    static boolean history(byte[] key){return key.length==33 && key[0]==0x53;}
    static final byte[] DEFINITION=new byte[32];
    void configure(NativeHomeRecoveryTest.Fixture f) throws Exception {
        f.store.enableCatalogueScopeCompaction();f.store.configureCatalogueHistory(helper.column,NativeCatalogueHistoryTest::history,command->{var key=command.mutations().getFirst().key();int id=key.length==33 ? ByteBuffer.wrap(key,29,4).getInt() : -1;return id==70000 ? CatalogueHistory.Directive.begin(DEFINITION,1024,3) : id==70001 ? CatalogueHistory.Directive.spend(DEFINITION) : id==70002 ? CatalogueHistory.Directive.terminal(DEFINITION) : CatalogueHistory.Directive.unreserved();});
    }
    DistributedCommand command(long sequence,List<Mutation> mutations,List<KeyPredicate> predicates){return new DistributedCommand(helper.group,1,new byte[32],new OperationId(helper.user,1,sequence),mutations,predicates);}
    DistributedCommand put(long sequence,int id,byte[] previous,byte[] value){return command(sequence,List.of(new Mutation(Mutation.Kind.PUT,helper.column,key(id),value)),List.of(new KeyPredicate(helper.column,key(id),previous)));}
    @Test void nativeActualAndPromisedCapacityFundsCompletionThenSurvivesColdAndImage() throws Exception {
        var path=root.resolve("source");var image=root.resolve("catalogue.image");int index=0;long sequence=0;CatalogueHistory.State saved;
        try(var f=helper.new Fixture(path)) {
            configure(f);
            for(int first=0;first<65529;first+=256){var mutations=new ArrayList<Mutation>();for(int n=first;n<Math.min(first+256,65529);n++)mutations.add(new Mutation(Mutation.Kind.PUT,helper.column,key(n),new byte[]{1}));assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,command(sequence++,mutations,List.of())).status());}
            var begin=put(sequence++,70000,null,new byte[]{1});assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,begin).status());
            assertEquals(3,f.store.catalogueHistoryCapacity().promisedRows());
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,put(sequence++,71000,null,new byte[]{1})).status());
            var rejected=put(sequence++,71001,null,new byte[]{1});var before=f.store.catalogueHistoryCapacity();
            assertEquals(CatalogueHistory.MAX_ROWS,before.actualRows()+before.promisedRows());
            assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,rejected).status());assertEquals(before,f.store.catalogueHistoryCapacity());
            assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,rejected).status());assertNull(f.store.readCurrentPoint(helper.column,key(71001)));
            var failed=put(sequence++,70001,new byte[]{9},new byte[]{2});assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,failed).status());assertEquals(before,f.store.catalogueHistoryCapacity());
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,put(sequence++,70001,null,new byte[]{2})).status());assertEquals(2,f.store.catalogueHistoryCapacity().promisedRows());
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,put(sequence++,70002,null,new byte[]{3})).status());saved=f.store.catalogueHistoryCapacity();assertEquals(0,saved.promisedRows());
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,begin).status());assertEquals(saved,f.store.catalogueHistoryCapacity());
            try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits);}
        }
        NativeRangeImage.inspectCatalogueHistory(image,helper.limits,helper.column,NativeCatalogueHistoryTest::history);
        try(var f=helper.new Fixture(path)){configure(f);assertEquals(saved,f.store.catalogueHistoryCapacity());}
        try(var f=helper.new Fixture(root.resolve("receiver"))){configure(f);f.store.fence();f.store.installSnapshot(image,helper.limits);f.store.activate();assertEquals(saved,f.store.catalogueHistoryCapacity());}
    }
    @Test void uncertainWriteRecoversExactInventoryAndCreditWithoutDoubleCharging() throws Exception {
        var path=root.resolve("unknown");var begin=put(0,70000,null,new byte[]{1});
        try(var f=helper.new Fixture(path)){configure(f);f.store.catalogueHistoryWriteObserver(()->{throw new org.rocksdb.RocksDBException("unknown after native history write");});assertThrows(org.rocksdb.RocksDBException.class,()->helper.apply(f,1,begin));}
        try(var f=helper.new Fixture(path)){configure(f);var original=f.store.catalogueHistoryCapacity();assertEquals(3,original.promisedRows());assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,2,begin).status());assertEquals(original,f.store.catalogueHistoryCapacity());}
    }
    RangeDirectory directory(int id,long generation){var owner=new ReplicaDescriptor(new UUID(0,100),new UUID(0,101),new UUID(0,102),"native-directory","127.0.0.1:9000",ReplicaDescriptor.Role.VOTER);var range=new RangeDescriptor(new UUID(0,103),new UUID(0,104),1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));return new RangeDirectory(new UUID(0,20),new UUID(0,21),new UUID(0,9000+id),1,new byte[32],generation,Set.of(new UUID(0,105)),0,List.of(range));}
    DistributedCommand directoryPut(long sequence,RangeDirectory before,RangeDirectory next){byte[] key=ByteBuffer.allocate(16).putLong(next.domainId().getMostSignificantBits()).putLong(next.domainId().getLeastSignificantBits()).array();return command(sequence,List.of(new Mutation(Mutation.Kind.PUT,helper.column,key,RangeDirectoryCodec.encode(next))),List.of(new KeyPredicate(helper.column,key,before==null ? null : RangeDirectoryCodec.encode(before))));}
    @Test void currentDirectorySlotsHaveIndependentFiniteFullGrowthReservations() throws Exception {
        var path=root.resolve("directories");var image=root.resolve("directories.image");int index=0;CatalogueHistory.State history;
        try(var f=helper.new Fixture(path)) {
            configure(f);for(int n=0;n<DatabaseDomainRoster.MAX_DOMAINS;n++)assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,directoryPut(n,null,directory(n,1))).status());
            history=f.store.catalogueHistoryCapacity();assertEquals(1,history.actualRows());assertEquals(0,history.promisedRows());
            var newSlot=directoryPut(100,null,directory(100,1));assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,newSlot).status());
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,directoryPut(101,directory(0,1),directory(0,2))).status());assertEquals(history,f.store.catalogueHistoryCapacity());
            try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits);}
        }
        NativeRangeImage.inspectCatalogueHistory(image,helper.limits,helper.column,NativeCatalogueHistoryTest::history);
        try(var f=helper.new Fixture(path)){configure(f);assertEquals(history,f.store.catalogueHistoryCapacity());assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,directoryPut(102,null,directory(101,1))).status());}
    }

}
