package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.RemoteMutationOwnerLimits;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisBackendJournalInventoryTest {
    @TempDir Path root;
    @Test void coldInventoryIncludesPendingOnlyOldGenerationsAndRejectsBeforeOpeningFactory()throws Exception {
        var old=Files.createDirectories(root.resolve("generations/old/points/owner"));
        Files.write(old.resolve("intent.pending"),new byte[4096]);Files.write(old.resolve("input.released"),new byte[512]);Files.write(old.resolve("input.lock"),new byte[0]);
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            RatisBackendJournalInventory.inventory(root,budget);assertEquals(4608,budget.chargedDiskBytes());
            assertThrows(IOException.class,()->budget.write(old.resolve("intent"),new byte[10],8192));assertEquals(4096,Files.size(old.resolve("intent.pending")));assertFalse(Files.exists(old.resolve("intent")));
            assertEquals(4608,budget.chargedDiskBytes());
        }
    }
    @Test void mandatoryPromisesTransferToWritesAndOnlyForcedInputRemovalRefundsDebt()throws Exception {
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            Path input=root.resolve("input"),history=root.resolve("released");var paths=List.of(new RatisJournalAccess.Reservation(input,4096,2048),new RatisJournalAccess.Reservation(history,4096,1024));budget.preflight(paths);assertEquals(3072,budget.chargedDiskBytes());
            byte[] body=new byte[512];budget.write(input,body,4096);budget.write(history,new byte[64],4096);assertEquals(3072,budget.chargedDiskBytes());budget.fulfilled(paths);assertEquals(640,budget.chargedDiskBytes());
            assertThrows(IOException.class,()->budget.remove(input,body,4096,new SessionJournalFiles.Removal(){public void delete(Path file)throws IOException{Files.delete(file);}public void force(Path directory)throws IOException{throw new IOException("Unknown unlink force");}}));assertEquals(640,budget.chargedDiskBytes());assertFalse(Files.exists(input));
            budget.deleted(input);assertEquals(96,budget.chargedDiskBytes());assertTrue(Files.exists(history));
        }
    }
    @Test void sameCapabilityBorrowsRootButOtherCapabilityWaitsForEveryActualOwner()throws Exception {
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var competitor=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var factory=budget.backendRoot(root);var connection=budget.backendRoot(root);var owner=budget.owner(it.cavallium.rockserver.core.common.distributed.RemoteMutationToken.Kind.POINT,0,0);owner.opened(0,false);
            try{assertThrows(IOException.class,()->competitor.backendRoot(root));assertEquals(1,probe(root.resolve("backend.lock")));
                Path alias=Files.createDirectory(root.resolve("inode-alias"));Files.createLink(alias.resolve("backend.lock"),root.resolve("backend.lock"));assertThrows(IOException.class,()->competitor.backendRoot(alias));assertEquals(1,probe(root.resolve("backend.lock")));connection.close();assertThrows(IOException.class,budget::close);assertThrows(IOException.class,()->competitor.backendRoot(root));owner.close();budget.close();assertThrows(IOException.class,()->competitor.backendRoot(root));}finally{owner.close();connection.close();factory.close();}
            assertEquals(0,probe(root.resolve("backend.lock")));try(var acquired=competitor.backendRoot(root)){assertNotNull(acquired);}
        }
    }
    @Test void schemaMetadataReplacementAndColdReadUseSameDebtWithoutChargingExportSstPayloads()throws Exception {
        Path schema=Files.createDirectories(root.resolve("schema")),request=schema.resolve("request");
        Path artifacts=Files.createDirectories(root.resolve("exports/job"));String name="0".repeat(64)+"-00000000-0000-0000-0000-000000000001-00000000-0000-0000-0000-000000000002-0.sst";
        for(var suffix:List.of("",".download",".building"))Files.write(artifacts.resolve(name+suffix),new byte[1024]);SessionJournalFiles.write(artifacts.resolve(name+".manifest"),new byte[64],1024);
        var fields=java.util.Map.of("phase",new byte[]{1},"input",new byte[1024]);
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var lease=budget.backendRoot(root)){
            RatisBackendJournalInventory.inventory(root,budget);assertEquals(96,budget.chargedDiskBytes());RatisSchemaClientJournal.writeOwned(request,fields,budget);assertEquals(96+Files.size(request),budget.chargedDiskBytes());
            fields=java.util.Map.of("phase",new byte[]{2},"input",new byte[2048]);RatisSchemaClientJournal.writeOwned(request,fields,budget);assertEquals(96+Files.size(request),budget.chargedDiskBytes());
        }
        try(var cold=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var lease=cold.backendRoot(root)){
            RatisBackendJournalInventory.inventory(root,cold);var recovered=RatisSchemaClientJournal.readOwned(request,cold);assertArrayEquals(fields.get("input"),recovered.get("input"));assertArrayEquals(new byte[]{2},recovered.get("phase"));assertEquals(96+Files.size(request),cold.chargedDiskBytes());
        }
    }

    @Test void ownedExportMetadataPreflightsBeforeChunkAndColdAckRefundsOnlyForcedSidecarRemoval()throws Exception {
        Path artifacts=Files.createDirectories(root.resolve("exports/job")),path=artifacts.resolve("child");var helper=new RatisExportJournalTest();helper.root=artifacts;var directory=helper.directory(1);var definition=helper.definition(directory);var captures=helper.tokens(definition.fence());
        var limits=new RemoteMutationOwnerLimits(1,2,1,1,0,4,128L<<20,16L<<20,128,16L<<20,8L<<20,1);Path artifact;
        try(var budget=new RatisRequestBudget(limits);var lease=budget.backendRoot(root);var journal=RatisExportJournal.createOwned(path,definition,budget)){
            journal.bindSource(helper.capturedSource(directory,definition.fence(),captures));Path competing=root.resolve("retained-history");Files.write(competing,new byte[0]);long promise=(16L<<20)-budget.chargedDiskBytes()-(4L<<20);budget.reserve(new RatisJournalAccess.Reservation(competing,16<<20,promise));long disk=budget.chargedDiskBytes(),bytes=Files.size(path);
            assertThrows(RuntimeException.class,()->journal.freezeChunk(directory.ranges().getFirst().groupId()));assertEquals(disk,budget.chargedDiskBytes());assertEquals(bytes,Files.size(path));assertNull(journal.snapshot().pending());
            budget.fulfilled(new RatisJournalAccess.Reservation(competing,16<<20,promise));var intent=journal.freezeChunk(directory.ranges().getFirst().groupId());artifact=journal.artifactPath(intent);assertFalse(Files.exists(artifact));journal.publish(intent,helper.artifact(journal,intent,null,false,false));
            for(var group:journal.snapshot().sourceCleanup())journal.sourceReleased(group);journal.markReady(helper.releasedSource(directory,definition.fence(),captures));assertTrue(budget.chargedDiskBytes()>Files.size(path));assertTrue(Files.exists(artifact));
        }
        try(var cold=new RatisRequestBudget(limits);var lease=cold.backendRoot(root)){
            RatisBackendJournalInventory.inventory(root,cold);try(var journal=RatisExportJournal.reopenOwned(path,cold)){assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase());journal.acknowledge(journal.snapshot().chunks().getFirst());journal.release();journal.reclaimArtifacts();assertFalse(Files.exists(artifact));assertTrue(journal.snapshot().artifactsReclaimed());assertEquals(Files.size(path),cold.chargedDiskBytes());}
        }
    }

    private static int probe(Path marker)throws Exception {
        String classpath=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",classpath,RootLockProbe.class.getName(),marker.toString()).redirectErrorStream(true).start();
        if(!child.waitFor(10,java.util.concurrent.TimeUnit.SECONDS)){child.destroyForcibly();child.waitFor(5,java.util.concurrent.TimeUnit.SECONDS);fail("Root lock child probe did not exit");}
        int code=child.exitValue();if(code!=20 && code!=21)fail("Root lock child probe failed: "+new String(child.getInputStream().readNBytes(4096),java.nio.charset.StandardCharsets.UTF_8));return code==20?0:1;
    }
    public static final class RootLockProbe {
        public static void main(String[] arguments)throws Exception {
            int code;try(var channel=java.nio.channels.FileChannel.open(Path.of(arguments[0]),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);var lock=channel.tryLock()){code=lock==null?21:20;}System.exit(code);
        }
    }

}
