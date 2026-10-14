package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Disk journal and controlled immutable routes only; no network or native server activity. */
class SessionWindowJournalTest {
    private static final UUID CLUSTER=new UUID(0,1), DATABASE=new UUID(0,2), DOMAIN=new UUID(0,3), COLUMN=new UUID(0,4), SESSION=new UUID(0,5);
    private static final UUID GROUP=new UUID(0,6), RANGE=new UUID(0,7);
    private static final SessionEpochLedger LEDGER=SessionEpochLedger.initial(CLUSTER,DATABASE,10).issue(SESSION,100);
    private static final SessionEpochLedger.Grant GRANT=LEDGER.grants().getFirst();
    private static final RatisSessionClient.Limits LIMITS=new RatisSessionClient.Limits(4,1024*1024,1024*1024);
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static RangeDirectory directory(UUID database,long generation,String address) {
        var peer=new ReplicaDescriptor(new UUID(1,1),new UUID(1,2),new UUID(1,3),"peer",address,ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(RANGE,GROUP,1,generation,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        return new RangeDirectory(CLUSTER,database,DOMAIN,1,new byte[32],generation,Set.of(COLUMN),0,List.of(range));
    }
    private static RatisRoutedClient routed(RangeDirectory directory) {
        return new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError("No network allowed"); }
            public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("No network allowed"); }
        });
    }
    private static Mutation put(int key) { return new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {(byte)key},new byte[] {(byte)(key+1)}); }
    private static RatisSessionClient.WindowState pending(RatisRoutedClient routed) {
        var client=new RatisSessionClient(routed,LEDGER,GRANT,LIMITS); client.prepare(List.of(put(1))); return client.snapshot();
    }

    @Test void createReopenPreservesExactCommandRouteOutOfOrderConsumptionAndExpiredGrant() throws Exception {
        Path path=root.resolve("window.bin");
        try (var routed=routed(directory(DATABASE,0,"127.0.0.1:9001"))) {
            var client=new RatisSessionClient(routed,LEDGER,GRANT,LIMITS); client.prepare(List.of(put(1))); client.prepare(List.of(put(2)));
            var entries=client.snapshot().entries();
            var state=new RatisSessionClient.WindowState(2,-1,false,List.of(entries.getFirst(),
                    new RatisSessionClient.WindowEntry(entries.getLast().prepared(),new CommandResult(CommandResult.Status.CONFLICT,List.of()),true)));
            byte[] frozen=DistributedCommandCodec.encode(entries.getFirst().prepared().command());
            try (var journal=SessionWindowJournal.open(path,routed,true)) {
                assertThrows(IOException.class,journal::read); journal.write(GRANT,LIMITS,state);
                assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false));
                assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,true));
            }
            routed.refresh(directory(DATABASE,1,"127.0.0.1:9011"));
            try (var journal=SessionWindowJournal.open(path,routed,false)) {
                var saved=journal.read(); assertEquals(GRANT,saved.grant()); assertEquals(LIMITS,saved.limits());
                assertArrayEquals(frozen,DistributedCommandCodec.encode(saved.state().entries().getFirst().prepared().command()));
                assertEquals("127.0.0.1:9001",saved.state().entries().getFirst().prepared().descriptor().replicas().getFirst().address());
                assertEquals(CommandResult.Status.CONFLICT,saved.state().entries().getLast().result().status()); assertTrue(saved.state().entries().getLast().consumed());
                assertThrows(UnsupportedOperationException.class,() -> saved.state().entries().clear());
                byte[] before=Files.readAllBytes(path); journal.write(GRANT,LIMITS,saved.state()); assertArrayEquals(before,Files.readAllBytes(path));
                var restored=RatisSessionClient.restore(routed,saved.grant(),saved.limits(),saved.state()); assertEquals(2,restored.outstanding());
                assertFalse(LEDGER.advanceTime(100).isLive(new OperationId(GRANT.sessionId(),GRANT.epoch(),0)));
            }
            assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,true));
            assertThrows(IOException.class,() -> SessionWindowJournal.open(root.resolve("absent.bin"),routed,false));
        }
    }

    @Test void checksumWrongIdentityTruncationAndChangedCanonicalBytesAreRejectedWithoutOverwrite() throws Exception {
        Path path=root.resolve("window.bin");
        try (var routed=routed(directory(DATABASE,0,"127.0.0.1:9001"))) {
            try (var journal=SessionWindowJournal.open(path,routed,true)) { journal.write(GRANT,LIMITS,pending(routed)); }
            byte[] valid=Files.readAllBytes(path), body=SessionJournalFiles.read(path,SessionWindowJournal.MAX_BYTES);
            try (var foreign=routed(directory(new UUID(9,9),0,"127.0.0.1:9001"))) {
                assertThrows(IOException.class,() -> SessionWindowJournal.open(path,foreign,false));
            }
            assertArrayEquals(valid,Files.readAllBytes(path));
            byte[] corrupted=valid.clone(); corrupted[corrupted.length/2]^=1; Files.write(path,corrupted);
            assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false)); assertArrayEquals(corrupted,Files.readAllBytes(path));
            for (int size:new int[] {0,31,32,120,valid.length-1}) {
                Files.write(path,Arrays.copyOf(valid,size)); assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false));
            }
            for (int offset:new int[] {0,4,113,117}) {
                byte[] changed=body.clone(); ByteBuffer.wrap(changed).putInt(offset,Integer.MAX_VALUE);
                SessionJournalFiles.write(path,changed,SessionWindowJournal.MAX_BYTES);
                assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false));
            }
            byte[] badFlag=body.clone(); badFlag[112]=2; SessionJournalFiles.write(path,badFlag,SessionWindowJournal.MAX_BYTES);
            assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false));
            byte[] badConsumed=body.clone(); badConsumed[badConsumed.length-2]=2; SessionJournalFiles.write(path,badConsumed,SessionWindowJournal.MAX_BYTES);
            assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false));
            var table=ByteBuffer.wrap(body);table.position(117);assertEquals(1,table.getInt());int directoryBytes=table.getInt(),directoryStart=table.position();var route=ByteBuffer.wrap(body,directoryStart,directoryBytes).slice();route.position(100);int columns=route.getInt();route.position(route.position()+16*columns);assertEquals(1,route.getInt());route.position(route.position()+48);for(int n=0;n<2;n++){int length=route.getInt();if(length>=0)route.position(route.position()+length);}route.get();assertEquals(1,route.getInt());route.position(route.position()+48);route.get();int peerBytes=route.getInt();assertTrue(peerBytes>0);
            byte[] nonAscii=body.clone();nonAscii[directoryStart+route.position()]=(byte)0xff;SessionJournalFiles.write(path,nonAscii,SessionWindowJournal.MAX_BYTES);assertThrows(IOException.class,()->SessionWindowJournal.open(path,routed,false));
            byte[] changedSequence=body.clone(); ByteBuffer.wrap(changedSequence).putLong(96,2); SessionJournalFiles.write(path,changedSequence,SessionWindowJournal.MAX_BYTES);
            assertThrows(IOException.class,() -> SessionWindowJournal.open(path,routed,false));
            Files.write(path,valid); try (var journal=SessionWindowJournal.open(path,routed,false)) { assertEquals(1,journal.read().state().entries().size()); }
        }
    }

    @Test void writeFailureAndBoundedResourceFailureFenceUntilReopen() throws Exception {
        Path path=root.resolve("window.bin");
        try (var routed=routed(directory(DATABASE,0,"127.0.0.1:9001"))) {
            byte[] valid;
            try (var journal=SessionWindowJournal.open(path,routed,true)) {
                var state=pending(routed); journal.write(GRANT,LIMITS,state); valid=Files.readAllBytes(path);
                Files.delete(path); Files.createDirectory(path); Files.writeString(path.resolve("block"),"fixture");
                assertThrows(IOException.class,() -> journal.write(GRANT,LIMITS,state)); assertThrows(IOException.class,journal::read);
            }
            Files.delete(path.resolve("block")); Files.delete(path); Files.write(path,valid);
            try (var journal=SessionWindowJournal.open(path,routed,false)) {
                var tooLarge=new RatisSessionClient.Limits(4,SessionWindowJournal.MAX_WINDOW_BYTES+1,1024);
                assertThrows(IOException.class,() -> journal.write(GRANT,tooLarge,journal.read().state()));
                assertThrows(IOException.class,journal::read); assertArrayEquals(valid,Files.readAllBytes(path));
            }
            try (var journal=SessionWindowJournal.open(path,routed,false)) {
                var empty=new RatisSessionClient.WindowState(0,-1,false,List.of());
                var changed=new SessionEpochLedger.Grant(GRANT.epoch(),UUID.randomUUID(),GRANT.issuedAtMillis(),GRANT.expiresAtMillis());
                assertThrows(IOException.class,() -> journal.write(changed,LIMITS,empty)); assertThrows(IOException.class,journal::read);
            }
            try (var journal=SessionWindowJournal.open(path,routed,false)) { assertEquals(GRANT,journal.read().grant()); }
        }
    }

    @Test void durableByteCapsAndOversizedFilesFailBeforeJournalAllocation() throws Exception {
        try (var routed=routed(directory(DATABASE,0,"127.0.0.1:9001"))) {
            for (boolean command:List.of(false,true)) {
                Path path=root.resolve("limit-"+command+".bin");
                var limits=new RatisSessionClient.Limits(1,command ? SessionWindowJournal.MAX_WINDOW_BYTES+1 : 1024,
                        command ? 1024 : SessionWindowJournal.MAX_WINDOW_BYTES+1);
                try (var journal=SessionWindowJournal.open(path,routed,true)) {
                    var empty=new RatisSessionClient.WindowState(0,-1,false,List.of());
                    assertThrows(IOException.class,() -> journal.write(GRANT,limits,empty)); assertThrows(IOException.class,journal::read);
                    assertFalse(Files.exists(path));
                }
            }
            Path oversized=root.resolve("oversized.bin");
            try (var file=java.nio.channels.FileChannel.open(oversized,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {
                file.position(SessionWindowJournal.MAX_BYTES); file.write(ByteBuffer.wrap(new byte[] {0}));
            }
            assertThrows(IOException.class,() -> SessionWindowJournal.open(oversized,routed,false));
        }
    }

    @Test void invalidWindowOwnershipResultAndReservationsNeverOverwriteDurableState() throws Exception {
        Path path=root.resolve("window.bin");
        try (var routed=routed(directory(DATABASE,0,"127.0.0.1:9001"))) {
            var state=pending(routed);
            try (var journal=SessionWindowJournal.open(path,routed,true)) { journal.write(GRANT,LIMITS,state); }
            byte[] valid=Files.readAllBytes(path);
            var entry=state.entries().getFirst();
            for (var invalid:List.of(new RatisSessionClient.WindowState(2,-1,false,state.entries()),
                    new RatisSessionClient.WindowState(1,-1,false,List.of(new RatisSessionClient.WindowEntry(entry.prepared(),null,true))),
                    new RatisSessionClient.WindowState(1,-1,false,List.of(new RatisSessionClient.WindowEntry(entry.prepared(),new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[] {1})),false))))) {
                try (var journal=SessionWindowJournal.open(path,routed,false)) {
                    assertThrows(IOException.class,() -> journal.write(GRANT,LIMITS,invalid)); assertThrows(IOException.class,journal::read);
                }
                assertArrayEquals(valid,Files.readAllBytes(path));
            }
            try (var journal=SessionWindowJournal.open(path,routed,false)) {
                var command=entry.prepared().command(); var descriptor=entry.prepared().descriptor();
                var foreign=new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),new OperationId(UUID.randomUUID(),GRANT.epoch(),0),command.mutations(),command.predicates());
                var wrong=routed.restoreSessionCommand(foreign,descriptor);
                var invalid=new RatisSessionClient.WindowState(1,-1,false,List.of(new RatisSessionClient.WindowEntry(wrong,null,false)));
                assertThrows(IOException.class,() -> journal.write(GRANT,LIMITS,invalid));
            }
        }
    }
}
