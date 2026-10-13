package it.cavallium.rockserver.distributed.ratis;

import java.util.Map;
import java.util.UUID;

/** Finite resource owner for isolated proof fixtures; runtime fixtures use the node's actual graph. */
final class RatisProofFixtures {
    static RatisProofQueryService.ResourceAdmission admission() {
        var pool=UUID.randomUUID();
        var limits=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L<<20,128),new RatisNodeResourceBudget.Lane(1L<<30,128));
        var graph=new RatisNodeResourceBudget(limits,Map.of(pool,limits));
        return (group,bytes)->graph.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,bytes,1)::close;
    }
}
