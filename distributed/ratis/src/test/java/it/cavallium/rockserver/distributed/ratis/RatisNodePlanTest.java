package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RatisNodePlanTest {
    private static final UUID NODE=new UUID(0,1), INCARNATION=new UUID(0,2), OTHER=new UUID(0,3), OTHER_INC=new UUID(0,4);
    private static final UUID CLUSTER=new UUID(0,5), DATABASE=new UUID(0,6), DOMAIN=new UUID(0,7), COLUMN=new UUID(0,8);
    private static final UUID DIRECTORY_COLUMN=new UUID(0,9), SESSIONS_COLUMN=new UUID(0,10), CATALOGUE=new UUID(0,30);
    private static final UUID HIGH_GROUP=new UUID(0,20), LOW_GROUP=new UUID(0,11);
    private static ReplicaDescriptor local(String peer,String address,ReplicaDescriptor.Role role) {
        return new ReplicaDescriptor(NODE,INCARNATION,new UUID(1,1),peer,address,role);
    }
    private static ReplicaDescriptor remote() {
        return new ReplicaDescriptor(OTHER,OTHER_INC,new UUID(1,2),"remote","127.0.0.1:9002",ReplicaDescriptor.Role.VOTER);
    }
    private static RatisNodePlan.Catalogue catalogue(List<ReplicaDescriptor> replicas) {
        return new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,replicas);
    }
    private static RangeDescriptor range(UUID group,byte[] lower,byte[] upper,List<ReplicaDescriptor> replicas) {
        return new RangeDescriptor(group,group,1,0,lower,upper,RangeDescriptor.TopologyState.ACTIVE,replicas);
    }
    private static RangeDirectory directory(long generation,List<RangeDescriptor> ranges) {
        return new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],generation,Set.of(COLUMN),0,ranges);
    }
    private static RatisNodePlan plan(RangeDirectory directory,RatisNodePlan.Catalogue catalogue,int budget) {
        return new RatisNodePlan(NODE,INCARNATION,directory,catalogue,Map.of(COLUMN,"data"),Set.of(),budget);
    }

    @Test void dataOnlyListenerUsesOneGlobalPeerAndExactEndpointWithSortedLocalGroups() {
        var listener=local("local","[::1]:9001",ReplicaDescriptor.Role.LISTENER);
        var directory=directory(0,List.of(range(HIGH_GROUP,null,new byte[] {127},List.of(listener,remote())),
                range(LOW_GROUP,new byte[] {127},null,List.of(listener,remote()))));
        var plan=plan(directory,catalogue(List.of(remote())),2);
        assertEquals(NODE,plan.nodeId()); assertEquals(INCARNATION,plan.nodeIncarnation());
        assertEquals("local",plan.peerId()); assertEquals("[::1]:9001",plan.address());
        assertEquals(List.of(LOW_GROUP,HIGH_GROUP),plan.localGroups().stream().map(group -> group.getGroupId().getUuid()).toList());
        for (var group:plan.localGroups()) {
            assertEquals(RaftPeerRole.LISTENER,group.getPeer(RaftPeerId.valueOf("local")).getStartupRole());
            assertEquals("[::1]:9001",group.getPeer(RaftPeerId.valueOf("local")).getAddress());
            assertEquals(RaftPeerRole.FOLLOWER,group.getPeer(RaftPeerId.valueOf("remote")).getStartupRole());
        }
        assertTrue(plan.catalogueGroup().getPeers().stream().noneMatch(peer -> peer.getId().equals(RaftPeerId.valueOf("local"))));
        assertEquals(CATALOGUE,plan.catalogue().raftGroup().getGroupId().getUuid());
        assertThrows(UnsupportedOperationException.class,() -> plan.localGroups().clear());
    }

    @Test void globalNodePeerEndpointAndIncarnationConflictsAreRejectedAcrossGroups() {
        var base=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var catalogue=catalogue(List.of(base,remote()));
        for (var conflicting:List.of(
                local("different","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER),
                local("local","127.0.0.1:9011",ReplicaDescriptor.Role.VOTER),
                new ReplicaDescriptor(NODE,new UUID(9,9),new UUID(1,3),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER),
                new ReplicaDescriptor(new UUID(9,1),new UUID(9,2),new UUID(1,3),"local","127.0.0.1:9011",ReplicaDescriptor.Role.VOTER),
                new ReplicaDescriptor(new UUID(9,1),new UUID(9,2),new UUID(1,3),"different","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER))) {
            assertThrows(IllegalArgumentException.class,() -> plan(directory(0,List.of(range(HIGH_GROUP,null,null,List.of(conflicting)))),catalogue,2));
        }
    }

    @Test void staticGroupsMetadataColumnsPhysicalBindingsAndBudgetsAreValidated() {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var directory=directory(0,List.of(range(HIGH_GROUP,null,null,List.of(local,remote()))));
        var catalogue=catalogue(List.of(local,remote()));
        assertEquals(2,plan(directory,catalogue,2).localGroups().size());
        for (int budget:new int[] {0,1,RangeDirectoryCodec.MAX_RANGES+2}) {
            assertThrows(IllegalArgumentException.class,() -> plan(directory,catalogue,budget));
        }
        assertDoesNotThrow(() -> plan(directory,catalogue,RangeDirectoryCodec.MAX_RANGES+1));
        assertThrows(IllegalArgumentException.class,() -> plan(directory(0,List.of(range(HIGH_GROUP,null,null,List.of(remote())))),catalogue(List.of(remote())),2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,new UUID(9,9),directory,catalogue,Map.of(COLUMN,"data"),Set.of(),2));
        var inactive=new RangeDescriptor(HIGH_GROUP,HIGH_GROUP,1,0,null,null,RangeDescriptor.TopologyState.FENCED,List.of(local));
        assertThrows(IllegalArgumentException.class,() -> plan(directory(0,List.of(inactive)),catalogue,2));
        assertThrows(IllegalArgumentException.class,() -> plan(directory(0,List.of(range(CATALOGUE,null,null,List.of(local)))),catalogue,2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],COLUMN,COLUMN,List.of(local)));
        var overlapping=new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],COLUMN,SESSIONS_COLUMN,List.of(local));
        assertThrows(IllegalArgumentException.class,() -> plan(directory,overlapping,2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,-1,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(local)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[31],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(local)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(local,local)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,directory,catalogue,Map.of(COLUMN,"default"),Set.of(),2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,directory,catalogue,Map.of(COLUMN,"data"),Set.of(DIRECTORY_COLUMN),2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,directory,catalogue,Map.of(),Set.of(),2));
    }

    @Test void defensiveCopiesAndDigestBindImmutableLocalPlacement() {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        byte[] schema=new byte[32]; var replicas=new ArrayList<>(List.of(remote()));
        var catalogue=new RatisNodePlan.Catalogue(CATALOGUE,1,schema,DIRECTORY_COLUMN,SESSIONS_COLUMN,replicas);
        schema[0]=1; replicas.clear(); catalogue.schemaDigest()[0]=2;
        assertEquals(0,catalogue.schemaDigest()[0]); assertEquals(1,catalogue.replicas().size());
        var directory=directory(0,List.of(range(HIGH_GROUP,null,null,List.of(local))));
        var columns=new HashMap<>(Map.of(COLUMN,"data")); var merges=new HashSet<>(Set.of(COLUMN));
        var original=new RatisNodePlan(NODE,INCARNATION,directory,catalogue,columns,merges,1);
        columns.clear(); merges.clear(); byte[] digest=original.identityDigest(); original.identityDigest()[0]^=1;
        assertArrayEquals(digest,original.identityDigest()); assertEquals(Map.of(COLUMN,"data"),original.columns()); assertEquals(Set.of(COLUMN),original.merges());
        assertThrows(UnsupportedOperationException.class,() -> original.columns().clear());
        assertThrows(UnsupportedOperationException.class,() -> original.merges().clear());
        assertThrows(UnsupportedOperationException.class,() -> catalogue.replicas().clear());
        var expanded=directory(5,List.of(range(HIGH_GROUP,null,new byte[] {127},List.of(local)),range(LOW_GROUP,new byte[] {127},null,List.of(local))));
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,expanded,catalogue,Map.of(COLUMN,"data"),Set.of(COLUMN),2).identityDigest()));
        assertArrayEquals(digest,new RatisNodePlan(NODE,INCARNATION,directory(5,directory.ranges()),catalogue,Map.of(COLUMN,"data"),Set.of(COLUMN),1).identityDigest());
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,directory,catalogue,Map.of(COLUMN,"other"),Set.of(COLUMN),1).identityDigest()));
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,directory,catalogue,Map.of(COLUMN,"data"),Set.of(),1).identityDigest()));
        var changedCatalogue=new RatisNodePlan.Catalogue(CATALOGUE,2,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(remote()));
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,directory,changedCatalogue,Map.of(COLUMN,"data"),Set.of(COLUMN),1).identityDigest()));
    }
}
