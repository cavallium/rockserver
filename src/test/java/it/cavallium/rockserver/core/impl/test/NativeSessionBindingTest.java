package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class NativeSessionBindingTest {
    static { RocksDB.loadLibrary(); }
    static final UUID GROUP=new UUID(0,1), DOMAIN=new UUID(0,2), COLUMN=new UUID(0,3), CLUSTER=new UUID(0,4), DB=new UUID(0,5),
            USER=new UUID(0,6), ADMIN=new UUID(0,7), OTHER=new UUID(0,8);
    static final NativeRangeImage.Limits LIMITS=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1);
    @TempDir(factory=DiskTestDirectory.class) Path root;
    static SessionEpochLedger issued() { return SessionEpochLedger.initial(CLUSTER,DB,10).issue(USER,100); }
    static DistributedCommand bind(SessionEpochLedger ledger,long sequence) {
        return control(new OperationId(ADMIN,ledger.nextEpoch(),sequence),RangeSessionControl.bind(ledger,ledger.grants().getFirst(),2));
    }
    static DistributedCommand control(OperationId issuer,RangeSessionControl control) { return DistributedCommand.sessionControl(GROUP,1,new byte[32],issuer,control,-1); }
    static DistributedCommand put(UUID user,long epoch,long sequence,long ack) {
        var mutations=List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{(byte)sequence},new byte[]{7}));
        return ack<0 ? new DistributedCommand(GROUP,1,new byte[32],new OperationId(user,epoch,sequence),mutations)
                : new DistributedCommand(GROUP,1,new byte[32],new OperationId(user,epoch,sequence),mutations,List.of(),ack);
    }
    @Test void boundWindowUsesEffectiveAckAndInvalidCommittedOutcomesRemainStable() throws Exception {
        try (var f=new Fixture(root.resolve("window"))) {
            var ledger=issued(); f.store.enableSessionControls();
            assertEquals(CommandResult.Status.SUCCESS,f.apply(bind(ledger,0)));
            var unknown=put(OTHER,1,0,-1);
            assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(unknown));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(unknown));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(USER,1,0,-1)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(USER,1,1,-1)));
            var beyond=put(USER,1,2,-1);
            assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(beyond));
            assertEquals(CommandResult.Status.CONFLICT,f.apply(beyond));
            assertNull(f.db.get(f.handles.get(1),new byte[]{2}));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(USER,1,3,1)));
            assertEquals(CommandResult.Status.CONFLICT,f.apply(beyond),"Invalid committed sequence cannot execute after its window later opens");
            assertArrayEquals(new byte[]{7},f.db.get(f.handles.get(1),new byte[]{3}));
            assertNull(f.db.get(f.handles.get(1),new byte[]{2}));
            var duplicateEpoch=ledger.issue(OTHER,200);
            var altered=new RangeSessionControl(SessionAuthority.from(duplicateEpoch),new SessionEpochLedger.Grant(1,OTHER,10,100),2);
            assertEquals(CommandResult.Status.CONFLICT,f.apply(control(new OperationId(ADMIN,3,1),altered)));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(unknown));
            var notYetBound=put(OTHER,2,0,-1);
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(notYetBound));
            var lateBinding=control(new OperationId(ADMIN,3,2),RangeSessionControl.bind(duplicateEpoch,duplicateEpoch.grants().getLast(),2));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(lateBinding));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(notYetBound),"A later binding cannot resurrect a terminal unbound outcome");
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(OTHER,2,1,-1)));
        }
    }
    @Test void orderedClockExpiresGrantHolesAndOldBindingsCannotReactivateThem() throws Exception {
        try (var f=new Fixture(root.resolve("expiry"))) {
            f.store.enableSessionControls(); var ledger=issued().issue(OTHER,50);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(bind(ledger,0)));
            var second=control(new OperationId(ADMIN,3,1),RangeSessionControl.bind(ledger,ledger.grants().getLast(),2));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(second));
            var original=put(OTHER,2,0,-1); assertEquals(CommandResult.Status.SUCCESS,f.apply(original));
            var expiredHole=ledger.advanceTime(50); assertEquals(1,expiredHole.minimumEpoch());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(control(new OperationId(ADMIN,3,2),RangeSessionControl.advance(expiredHole))));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(original),"Retained outcomes survive lifetime expiry until coarse fence/ACK");
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(put(OTHER,2,1,-1)));
            var expiredAck=new DistributedCommand(GROUP,1,new byte[32],new OperationId(OTHER,2,2),List.of(),List.of(),1);
            assertTrue(f.store.estimateReceiptRetention(expiredAck).maintenance());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(expiredAck),"Bound expired sessions can still drain only their receipts");
            var staleBind=control(new OperationId(ADMIN,3,3),RangeSessionControl.bind(ledger,ledger.grants().getLast(),2));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(staleBind));
            var rollback=control(new OperationId(ADMIN,3,4),RangeSessionControl.advance(ledger));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(rollback),"Dominated clock retry succeeds without lowering authority");
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(put(OTHER,2,3,1)),"A new identity remains expired after a dominated clock retry");
            assertNull(f.db.get(f.handles.get(1),new byte[]{3}));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(USER,1,0,-1)));
            var closed=expiredHole.advanceTime(100);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(control(new OperationId(ADMIN,3,5),RangeSessionControl.advance(closed))));
            assertNull(f.db.get(f.state(),f.bindingKey(1))); assertNull(f.db.get(f.state(),f.bindingKey(2)));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(original));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(put(USER,1,1,-1)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(control(new OperationId(ADMIN,3,6),RangeSessionControl.advance(closed.advanceTime(200)))));
        }
    }
    @Test void restartAndInstalledImagePreserveActivationWithoutRuntimeEnableFlag() throws Exception {
        Path db=root.resolve("restart"), image=root.resolve("session.image");
        var ledger=issued();
        try (var f=new Fixture(db)) {
            assertThrows(UnsupportedOperationException.class,() -> f.apply(bind(ledger,0)));
            assertNull(f.store.applied()); f.store.enableSessionControls();
            assertEquals(CommandResult.Status.SUCCESS,f.apply(bind(ledger,0)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(USER,1,0,-1)));
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,new RangeOwnership.Interval(DOMAIN,Set.of(COLUMN),null,null),LIMITS); }
            NativeRangeImage.inspect(image,LIMITS); f.db.flushWal(true);
        }
        try (var f=new Fixture(db)) {
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(put(OTHER,1,1,-1)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(put(USER,1,1,-1)));
        }
        try (var target=new Fixture(root.resolve("installed"))) {
            target.store.fence(); target.store.installSnapshot(image,LIMITS); target.store.activate();
            assertEquals(CommandResult.Status.SESSION_EXPIRED,target.apply(put(OTHER,1,1,-1)));
            assertEquals(CommandResult.Status.SUCCESS,target.apply(put(USER,1,1,-1)));
            assertEquals(CommandResult.Status.SUCCESS,target.apply(put(USER,1,0,-1)));
        }
    }
    @Test void epochGcPhysicallyDeletesExpiredNamespacesAndReceipts() throws Exception {
        try(var f=new Fixture(root)) {
            f.store.enableSessionControls();var ledger=SessionEpochLedger.initial(CLUSTER,DB,0);
            for(int n=0;n<16;n++) {
                ledger=ledger.issue(new UUID(0,1000+n),ledger.logicalTimeMillis()+1);long epoch=ledger.grants().getLast().epoch();var issuer=new UUID(0,2000+n);
                assertEquals(CommandResult.Status.SUCCESS,f.apply(control(new OperationId(issuer,epoch,0),RangeSessionControl.advance(ledger))));
                assertEquals(CommandResult.Status.SUCCESS,f.apply(DistributedCommand.sessionControl(GROUP,1,new byte[32],new OperationId(issuer,epoch,1),RangeSessionControl.advance(ledger),0)));
                ledger=ledger.advanceTime(ledger.logicalTimeMillis()+1);
                assertEquals(CommandResult.Status.SUCCESS,f.apply(control(new OperationId(ADMIN,ledger.nextEpoch(),n),RangeSessionControl.advance(ledger))));
                assertTrue(f.store.receiptUsage().retainedCount()<=2);
            }
            int acknowledgements=0,receipts=0;
            try(var iterator=f.db.newIterator(f.state())){for(iterator.seekToFirst();iterator.isValid();iterator.next()){byte[] key=iterator.key();if(key.length==41 && key[16]==2)acknowledgements++;}iterator.status();}
            try(var iterator=f.db.newIterator(f.handles.get(3))){for(iterator.seekToFirst();iterator.isValid();iterator.next())receipts++;iterator.status();}
            assertEquals(0,acknowledgements);assertEquals(f.store.receiptUsage().retainedCount(),receipts);assertTrue(receipts<=2);
        }
    }

    static final class Fixture implements AutoCloseable {
        final DBOptions options=new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions cfOptions=new ColumnFamilyOptions(); final List<ColumnFamilyHandle> handles=new ArrayList<>();
        final RocksDB db; final ReplicatedRangeStore store;
        Fixture(Path path) throws Exception {
            db=RocksDB.open(options,path.toString(),List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,cfOptions),
                    new ColumnFamilyDescriptor(new byte[]{1},cfOptions),new ColumnFamilyDescriptor(new byte[]{2},cfOptions),new ColumnFamilyDescriptor(new byte[]{3},cfOptions)),handles);
            var ownership=new RangeOwnership(); var owner=new RangeOwnership.Owner(GROUP,1);
            ownership.register(owner,new RangeOwnership.Interval(DOMAIN,Set.of(COLUMN),null,null),RangeOwnership.State.ACTIVE);
            store=new ReplicatedRangeStore(db,ownership,owner,new byte[32],Map.of(COLUMN,handles.get(1)),state(),handles.get(3),Set.of());
        }
        CommandResult.Status apply(DistributedCommand command) throws Exception {
            var previous=store.applied();
            return CommandResult.decode(store.apply(new ApplyPosition(1,previous==null ? 0 : previous.index()+1),command)).status();
        }
        ColumnFamilyHandle state() { return handles.get(2); }
        byte[] bindingKey(long epoch) { return ByteBuffer.allocate(25).putLong(0).putLong(1).put((byte)0x14).putLong(epoch).array(); }
        public void close() { handles.forEach(ColumnFamilyHandle::close); db.close(); cfOptions.close(); options.close(); }
    }
}
