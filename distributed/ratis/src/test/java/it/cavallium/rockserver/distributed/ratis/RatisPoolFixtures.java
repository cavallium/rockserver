package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/** Explicit one-pool declaration for tests whose subject does not require physical isolation. */
final class RatisPoolFixtures {
    static final UUID ID=new UUID(0,900_001);
    static List<RatisNativePoolSpec> pools(){return List.of(spec(ID,false));}
    static RatisNativePoolSpec spec(UUID id,boolean spinning){return new RatisNativePoolSpec(id,("database.global.spinning="+spinning).getBytes(StandardCharsets.UTF_8),RatisNodePlan.MAX_LOCAL_GROUPS,new NativeRangeImage.Limits(8<<20,256L<<20,256),RatisRangeStateMachine.RetentionLimits.defaults(),new PoolSnapshotAdmission.Limits(64,256L<<20,Duration.ofHours(24),0),RatisSchemaReplicaService.Limits.defaults(),new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L<<20,512),new RatisNodeResourceBudget.Lane(512L<<20,128)),256L<<30);}
    static RatisSchemaReplicaAdmission schemaAdmission(RatisSchemaReplicaService.Limits limits){var budget=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(256L<<20,1024),new RatisNodeResourceBudget.Lane(512L<<20,512));return new RatisSchemaReplicaAdmission(limits,new RatisNodeResourceBudget(budget,Map.of(ID,budget)));}
    static RatisSnapshotCohortService cohortService(RatisNodeRuntime node)throws Exception{var field=RatisNodeRuntime.class.getDeclaredField("pools");field.setAccessible(true);var owner=((Map<?,?>)field.get(node)).get(ID);var service=owner.getClass().getDeclaredField("cohorts");service.setAccessible(true);return (RatisSnapshotCohortService)service.get(owner);}
    static RatisSchemaReplicaService schemaService(RatisNodeRuntime node)throws Exception{var field=RatisNodeRuntime.class.getDeclaredField("pools");field.setAccessible(true);var owner=((Map<?,?>)field.get(node)).get(ID);var service=owner.getClass().getDeclaredField("schemas");service.setAccessible(true);return (RatisSchemaReplicaService)service.get(owner);}
    static Map<UUID,UUID> domains(Collection<RatisNodePlan.Domain> domains){var values=new HashMap<UUID,UUID>();domains.forEach(domain->values.put(domain.domainId(),ID));return values;}
    static Map<UUID,UUID> groups(RatisNodePlan.Catalogue catalogue,List<RatisNodePlan.Home> homes){var values=new HashMap<UUID,UUID>();values.put(catalogue.groupId(),ID);homes.forEach(home->home.directory().ranges().forEach(range->values.put(range.groupId(),ID)));return values;}
}
