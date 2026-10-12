package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Full local grant/window/logical/binding lifecycle on native stores; no sockets or quorum acceptance. */
class RatisDurableLogicalSessionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    DistributedSchema schema() {
        var o=new RatisSessionEpochsTest(); return new DistributedSchema(o.cluster,o.database,new UUID(6,1),1,0,List.of(
                new DistributedSchema.Column(71,o.data,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
    }
    Keys key(int key) { return new Keys(Buf.wrap(new byte[]{(byte)key})); }
    RatisDurableLogicalSession create(Path path,RatisSessionPropagationTest.Fixture f,DistributedSchema schema,long expiry) throws IOException {
        return RatisDurableLogicalSession.create(path,schema,f.catalogue.epochs,f.routed,f.logicalHomes(),f.logicalScheduler(),f.logicalDirectories(),f.logicalAcknowledgements(),expiry,RatisSessionClient.Limits.defaults());
    }
    RatisDurableLogicalSession reopen(Path path,RatisSessionPropagationTest.Fixture f,DistributedSchema schema) throws IOException {
        return RatisDurableLogicalSession.reopen(path,schema,f.catalogue.epochs,f.routed,f.logicalHomes(),f.logicalScheduler(),f.logicalDirectories(),f.logicalAcknowledgements());
    }
    @Test void issuedLogicalWindowLostBindingAndDataRepliesReopenConsumedHolesWithoutAnotherGrant() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest())) {
            Path wrong=Files.createDirectory(root.resolve("wrong-authority"));
            var foreign=new RatisSessionEpochs(f.catalogue.client,UUID.randomUUID(),f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.sessions);
            assertThrows(IllegalArgumentException.class,() -> RatisDurableLogicalSession.create(wrong,schema,foreign,f.routed,f.logicalHomes(),f.logicalScheduler(),f.logicalDirectories(),f.logicalAcknowledgements(),1000,RatisSessionClient.Limits.defaults()));
            try (var files=Files.list(wrong)) { assertEquals(0,files.count()); }
            Path path=Files.createDirectory(root.resolve("logical")); SessionEpochLedger.Grant grant; byte[] frozen;
            try (var session=create(path,f,schema,1000)) {
                assertEquals(RatisDurableLogicalSession.Phase.PREPARING,session.progress().phase());
                assertThrows(IOException.class,() -> session.preparePut(71,key(7),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.NONE));
                assertEquals(RatisDurableLogicalSession.Phase.ACTIVE,session.advance().phase()); grant=session.progress().grant();
                var first=session.preparePut(session.getColumnId("data"),key(7),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.PREVIOUS);
                session.preparePut(71,key(200),Buf.wrap(new byte[]{8}),Mutation.ReturnMode.NONE); frozen=DistributedCommandCodec.encode(first.command());
                f.lost.add(first.descriptor().groupId()); assertThrows(IOException.class,() -> session.ensureBinding(first));
                assertThrows(IOException.class,() -> reopen(path,f,schema));
            }
            long catalogueCommands=f.catalogue.next.get();
            try (var session=reopen(path,f,schema)) {
                assertEquals(grant,session.progress().grant()); assertEquals(catalogueCommands,f.catalogue.next.get());
                var first=session.pending().getFirst(); var second=session.pending().getLast();
                assertEquals(CommandResult.Status.SUCCESS,session.ensureBinding(first).status()); assertEquals(CommandResult.Status.SUCCESS,session.ensureBinding(second).status());
                assertArrayEquals(frozen,DistributedCommandCodec.encode(first.command())); f.lost.add(first.descriptor().groupId());
                assertThrows(IOException.class,() -> session.execute(first)); session.execute(second); session.acknowledge(second); assertTrue(second.consumed());
                assertEquals(-1,session.acknowledgedThrough());
            }
            try (var session=reopen(path,f,schema)) {
                assertEquals(grant,session.progress().grant()); assertEquals(catalogueCommands,f.catalogue.next.get());
                var first=session.pending().getFirst(); assertTrue(session.pending().getLast().consumed());
                int reads=f.catalogue.reads.get(); assertNull(session.execute(first).value(0)); session.acknowledge(first);
                assertEquals(1,session.acknowledgedThrough()); assertTrue(session.pending().isEmpty());
                assertArrayEquals(new byte[]{9},session.get(71,key(7)).toByteArray()); assertEquals(reads,f.catalogue.reads.get(),"Ordinary retries/read/ACK use cached schema and installed bindings");
            }
        }
    }
    @Test void failedActiveMarkerFencesAndPristineHandoffResumesWithoutRepeatingIssuance() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest())) {
            Path path=Files.createDirectory(root.resolve("handoff")), temporary=path.resolve("session.bin.pending");
            try (var session=create(path,f,schema,1000)) {
                Files.createDirectory(temporary); Files.writeString(temporary.resolve("block"),"fixture");
                assertThrows(IOException.class,session::advance); assertTrue(Files.exists(path.resolve("window.bin")));
                assertThrows(IOException.class,() -> session.pending()); assertThrows(IOException.class,session::advance);
            }
            long commands=f.catalogue.next.get(); Files.delete(temporary.resolve("block")); Files.delete(temporary);
            try (var session=reopen(path,f,schema)) {
                assertEquals(RatisDurableLogicalSession.Phase.PREPARING,session.progress().phase());
                assertEquals(RatisDurableLogicalSession.Phase.ACTIVE,session.advance().phase()); assertEquals(commands,f.catalogue.next.get()); assertTrue(session.pending().isEmpty());
            }
        }
    }
    @Test void missingActiveFilesAndUsedEmptyPreparingWindowFailClosedWhileInitialStateCanResume() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest())) {
            Path path=Files.createDirectory(root.resolve("missing"));
            try (var session=create(path,f,schema,1000)) { /* No issuance RPC yet. */ }
            byte[] initial=SessionJournalFiles.read(path.resolve("session.bin"),256); initial[8]=0;
            SessionJournalFiles.write(path.resolve("session.bin"),initial,256); Files.delete(path.resolve("issuer.bin"));
            try (var session=reopen(path,f,schema)) {
                session.advance(); var pending=session.preparePut(71,key(7),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.NONE);
                session.ensureBinding(pending); session.execute(pending); session.acknowledge(pending); assertTrue(session.pending().isEmpty());
            }
            long commands=f.catalogue.next.get(); byte[] window=Files.readAllBytes(path.resolve("window.bin")); Files.delete(path.resolve("window.bin"));
            assertThrows(IOException.class,() -> reopen(path,f,schema)); assertFalse(Files.exists(path.resolve("window.bin"))); assertEquals(commands,f.catalogue.next.get());
            Files.write(path.resolve("window.bin"),window); byte[] activeBody=SessionJournalFiles.read(path.resolve("session.bin"),256); byte[] body=activeBody.clone(); body[8]=1; Arrays.fill(body,body.length-40,body.length,(byte)0);
            SessionJournalFiles.write(path.resolve("session.bin"),body,256);
            try (var session=reopen(path,f,schema)) { assertThrows(IOException.class,session::advance); assertEquals(RatisDurableLogicalSession.Phase.PREPARING,session.progress().phase()); }
            assertEquals(commands,f.catalogue.next.get()); SessionJournalFiles.write(path.resolve("session.bin"),activeBody,256);
            byte[] issuer=Files.readAllBytes(path.resolve("issuer.bin")); Files.delete(path.resolve("issuer.bin"));
            assertThrows(IOException.class,() -> reopen(path,f,schema)); assertFalse(Files.exists(path.resolve("issuer.bin"))); Files.write(path.resolve("issuer.bin"),issuer);
            try (var session=reopen(path,f,schema)) { assertEquals(0,session.acknowledgedThrough()); }
            Path other=Files.createDirectory(root.resolve("other-session"));
            try (var session=create(other,f,schema,1000)) { session.advance(); }
            Files.write(path.resolve("issuer.bin"),Files.readAllBytes(other.resolve("issuer.bin")));
            Files.write(path.resolve("window.bin"),Files.readAllBytes(other.resolve("window.bin")));
            assertThrows(IOException.class,() -> reopen(path,f,schema),"ACTIVE also binds the exact issued grant when both children are swapped");
            Files.write(path.resolve("issuer.bin"),issuer); Files.write(path.resolve("window.bin"),window);
            try (var session=reopen(path,f,schema)) { assertEquals(0,session.acknowledgedThrough()); }
        }
    }
    @Test void bindingRpcOwnsRootUntilCompletionAndExpiredIntentNeverReissues() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest()); var executor=Executors.newSingleThreadExecutor()) {
            Path path=Files.createDirectory(root.resolve("busy"));
            try (var session=create(path,f,schema,1000)) {
                session.advance(); var pending=session.preparePut(71,key(7),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.NONE);
                f.started=new CountDownLatch(1); f.release=new CountDownLatch(1); var running=executor.submit(() -> session.ensureBinding(pending));
                try { assertTrue(f.started.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,session::close); assertThrows(IOException.class,() -> reopen(path,f,schema)); }
                finally { f.release.countDown(); }
                assertEquals(CommandResult.Status.SUCCESS,running.get(5,TimeUnit.SECONDS).status()); f.started=null;
            }
            Path expired=Files.createDirectory(root.resolve("expired")); long commands=f.catalogue.next.get();
            try (var session=create(expired,f,schema,5)) { assertEquals(RatisGrantIssuer.Phase.EXPIRED,session.advance().issuance()); assertFalse(Files.exists(expired.resolve("window.bin"))); }
            try (var session=reopen(expired,f,schema)) { assertEquals(RatisGrantIssuer.Phase.EXPIRED,session.advance().issuance()); assertThrows(IOException.class,() -> session.pending()); }
            assertEquals(commands,f.catalogue.next.get());
        }
    }
}
