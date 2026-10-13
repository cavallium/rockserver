package it.cavallium.rockserver.distributed.ratis;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaReplicaAdmissionTest {
    @Test void actualTaskAndObserverOwnershipSharesNodeHeapAcrossPools()throws Exception {
        UUID a=new UUID(1,1),b=new UUID(1,2);var lanes=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L<<20,32),new RatisNodeResourceBudget.Lane(128L<<20,32));var graph=new RatisNodeResourceBudget(lanes,Map.of(a,lanes,b,lanes));var limits=new RatisSchemaReplicaService.Limits(1,16L<<20,64L<<20,1,4,0);var admission=new RatisSchemaReplicaAdmission(limits,graph);
        var parsing=admission.heap(a,4L<<20,1);assertThrows(java.io.IOException.class,()->admission.heap(b,4L<<20,1));parsing.resize(48L<<20,1);
        var observer=admission.heap(b,8L<<20,0);assertEquals(56L<<20,graph.stats().protectedLane().bytes());assertThrows(java.io.IOException.class,()->admission.heap(b,16L<<20,0));observer.close();assertEquals(48L<<20,graph.stats().protectedLane().bytes());parsing.close();assertEquals(0,graph.stats().protectedLane().bytes());
        try(var next=admission.heap(b,48L<<20,1)){assertEquals(48L<<20,graph.stats(b).protectedLane().bytes());}
    }
    @Test void durableUploadClaimsSurviveOtherPoolAdmissionUntilActualRelease()throws Exception {
        UUID a=new UUID(2,1),b=new UUID(2,2);var lanes=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L<<20,32),new RatisNodeResourceBudget.Lane(128L<<20,32));var admission=new RatisSchemaReplicaAdmission(new RatisSchemaReplicaService.Limits(1,16L<<20,64L<<20,1,4,0),new RatisNodeResourceBudget(lanes,Map.of(a,lanes,b,lanes)));
        admission.disk(a,"forced-original",8L<<20);admission.disk(a,"forced-original",8L<<20);assertThrows(java.io.IOException.class,()->admission.disk(b,"another",1));admission.released(a,"forced-original");admission.disk(b,"another",16L<<20);assertThrows(java.io.IOException.class,()->admission.disk(b,"another",(16L<<20)+1));admission.released(b,"another");admission.disk(a,"recovered",16L<<20);
    }
}
