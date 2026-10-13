package it.cavallium.rockserver.distributed.ratis;

import java.util.UUID;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisImportPublicationLaneTest {
    @Test void expandedMetadataLaneIsIndependentAndActualOwnershipDrains()throws Exception{
        var lane=RatisProofQueryService.importPublicationLane(RatisProofFixtures.admission());var ordinary=new RatisProofQueryService(RatisProofFixtures.admission());var entered=new CountDownLatch(1);var resume=new CountDownLatch(1);long weight=RatisProofQueryService.IMPORT_PUBLICATION_BYTES;var group=UUID.randomUUID();
        try{
            assertThrows(ExecutionException.class,()->ordinary.submit(group,new byte[]{1},weight,false,input->Message.valueOf("Unexpected")).get(5,TimeUnit.SECONDS));
            var first=lane.submit(group,new byte[]{1},weight,false,input->{entered.countDown();if(!resume.await(5,TimeUnit.SECONDS))throw new AssertionError("Lookup timed out");return Message.valueOf("Ready");});assertTrue(entered.await(5,TimeUnit.SECONDS));
            var queued=lane.submit(group,new byte[]{2},weight,false,input->{throw new AssertionError("Stopped queued lookup ran");});assertEquals(2,lane.stats().jobs());assertEquals(2*weight,lane.stats().bytes());assertThrows(ExecutionException.class,()->lane.submit(group,new byte[]{3},weight,false,input->Message.valueOf("Unexpected")).get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->lane.submit(group,new byte[]{1},weight,false,input->Message.valueOf("Unexpected")).get(5,TimeUnit.SECONDS));
            assertTrue(first.cancel(false));var stopped=lane.stop();assertFalse(stopped.isDone());assertThrows(ExecutionException.class,()->queued.get(5,TimeUnit.SECONDS));assertEquals(weight,lane.stats().bytes());resume.countDown();stopped.get(5,TimeUnit.SECONDS);assertEquals(0,lane.stats().jobs());assertEquals(0,lane.stats().bytes());
        }finally{resume.countDown();lane.stop().get(5,TimeUnit.SECONDS);ordinary.stop().get(5,TimeUnit.SECONDS);}
    }
}
