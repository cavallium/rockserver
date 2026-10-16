package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRequestBudgetTest {
    @TempDir Path root;
    @Test void kindsShareRequestAndRamCapsAndDynamicTransferDoesNotCreateCredit()throws Exception {
        var limits=new RemoteMutationOwnerLimits(1,1,1,1,0,1,64L<<20,16L<<20,8,16L<<20,8L<<20,1);
        try(var budget=new RatisRequestBudget(limits);var point=budget.owner(RemoteMutationToken.Kind.POINT,0,0);var bulk=budget.owner(RemoteMutationToken.Kind.BULK,0,0);var range=budget.owner(RemoteMutationToken.Kind.RANGE,0,0)){
            assertThrows(RuntimeException.class,()->budget.owner(RemoteMutationToken.Kind.BULK,0,0));assertThrows(IllegalStateException.class,()->range.opened(1,false));range.opened(0,false);point.opened(0,false);bulk.opened(0,false);
            try(var request=budget.request(RemoteMutationToken.Kind.BULK,1024);var work=budget.work(2048)){
                assertThrows(RuntimeException.class,()->budget.request(RemoteMutationToken.Kind.POINT,1));long charged=budget.chargedBytes();work.transferTo(request,512);assertEquals(charged,budget.chargedBytes());assertEquals(1536,request.amount);assertEquals(1536,work.amount);request.growTo(4096);assertEquals(charged+2560,budget.chargedBytes());request.shrinkTo(1024);assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.BULK));assertThrows(RuntimeException.class,()->work.growTo(limits.maximumBytes()));assertThrows(java.io.IOException.class,budget::close);
            }
        }
    }
    @Test void openingCleanupTransfersCreditUnderCompetingPressureWithoutAnotherReservation()throws Exception {
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var owner=budget.owner(RemoteMutationToken.Kind.BULK,0,1L<<20)){
            owner.requireOpening(1L<<20);long before=budget.chargedBytes();
            try(var competing=budget.work(budget.limits().maximumBytes()-before-(2L<<20))){
                assertThrows(RuntimeException.class,()->budget.work(7L<<20));long charged=budget.chargedBytes();
                try(var cleanup=owner.openedWorkspace(7L<<20)){assertEquals(charged+(1L<<20),budget.chargedBytes());assertEquals(0,budget.retainedRequests());assertEquals(7L<<20,cleanup.amount);}
            }
        }
    }
    @Test void multibytePathRegistryPrepaysBothNativePathGraphsBeforeRetention()throws Exception {
        var path=root;for(int n=0;n<10;n++)path=path.resolve("漢字".repeat(24));path=path.resolve("request");
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            long before=budget.chargedBytes();try(var disk=budget.disk(path,1024)){
                String live=path.toAbsolutePath().normalize().toString(),pending=live+".pending";
                long retained=budget.chargedBytes()-before;long utf8=live.getBytes(java.nio.charset.StandardCharsets.UTF_8).length+pending.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                long stringAndOffsets=2L*(live.length()+pending.length())+8L*path.getNameCount();assertTrue(retained>=utf8+stringAndOffsets+512);assertTrue(retained>256L+2L*live.length());
            }
        }
    }
    @Test void exactJournalDebtSurvivesCloseAndUnknownReplacementUntilDurableReconciliation()throws Exception {
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var file=root.resolve("owned");Files.write(file,new byte[1024]);try(var disk=budget.disk(file,16384)){disk.prepareReplacement(2048);Files.write(file.resolveSibling("owned.pending"),new byte[2048]);assertEquals(3072,budget.chargedDiskBytes());}
            assertEquals(3072,budget.chargedDiskBytes());try(var disk=budget.disk(file,16384)){assertEquals(3072,disk.debt());Files.delete(file.resolveSibling("owned.pending"));SessionJournalFiles.force(root);disk.forcedReconcile();assertEquals(1024,budget.chargedDiskBytes());}
            assertEquals(1024,budget.chargedDiskBytes());
        }
    }
    @Test void diskAndPathBoundsRejectBeforeReplacementOrRegistryGrowth()throws Exception {
        var limits=new RemoteMutationOwnerLimits(1,1,1,1,0,2,64L<<20,16L<<20,1,16L<<20,8L<<20,1);
        try(var budget=new RatisRequestBudget(limits)){
            var file=root.resolve("owned");Files.write(file,new byte[1024]);try(var disk=budget.disk(file,16<<20)){assertThrows(RuntimeException.class,()->disk.prepareReplacement(16L<<20));assertEquals(1024,budget.chargedDiskBytes());assertEquals(1024,Files.size(file));assertFalse(Files.exists(root.resolve("owned.pending")));}
            long memory=budget.chargedBytes();assertThrows(RuntimeException.class,()->budget.disk(root.resolve("another"),1024));assertEquals(memory,budget.chargedBytes());
        }
    }
}
