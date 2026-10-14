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
}
