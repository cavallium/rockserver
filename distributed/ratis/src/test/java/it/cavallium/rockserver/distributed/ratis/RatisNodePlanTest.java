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
        return new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of())),catalogue,List.of(),List.of(),Map.of(),budget,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of()))),RatisPoolFixtures.groups(catalogue,List.of()));
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
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,new UUID(9,9),List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of())),catalogue,List.of(),List.of(),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of()))),RatisPoolFixtures.groups(catalogue,List.of())));
        var inactive=new RangeDescriptor(HIGH_GROUP,HIGH_GROUP,1,0,null,null,RangeDescriptor.TopologyState.FENCED,List.of(local));
        assertThrows(IllegalArgumentException.class,() -> plan(directory(0,List.of(inactive)),catalogue,2));
        assertThrows(IllegalArgumentException.class,() -> plan(directory(0,List.of(range(CATALOGUE,null,null,List.of(local)))),catalogue,2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],COLUMN,COLUMN,List.of(local)));
        var overlapping=new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],COLUMN,SESSIONS_COLUMN,List.of(local));
        assertThrows(IllegalArgumentException.class,() -> plan(directory,overlapping,2));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,-1,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(local)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[31],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(local)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Catalogue(CATALOGUE,1,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(local,local)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"default"),Set.of())),catalogue,List.of(),List.of(),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"default"),Set.of()))),RatisPoolFixtures.groups(catalogue,List.of())));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of(DIRECTORY_COLUMN))),catalogue,List.of(),List.of(),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of(DIRECTORY_COLUMN)))),RatisPoolFixtures.groups(catalogue,List.of())));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(),Set.of())),catalogue,List.of(),List.of(),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(),Set.of()))),RatisPoolFixtures.groups(catalogue,List.of())));
    }

    @Test void defensiveCopiesAndDigestBindImmutableLocalPlacement() {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        byte[] schema=new byte[32]; var replicas=new ArrayList<>(List.of(remote()));
        var catalogue=new RatisNodePlan.Catalogue(CATALOGUE,1,schema,DIRECTORY_COLUMN,SESSIONS_COLUMN,replicas);
        schema[0]=1; replicas.clear(); catalogue.schemaDigest()[0]=2;
        assertEquals(0,catalogue.schemaDigest()[0]); assertEquals(1,catalogue.replicas().size());
        var directory=directory(0,List.of(range(HIGH_GROUP,null,null,List.of(local))));
        var columns=new HashMap<>(Map.of(COLUMN,"data")); var merges=new HashSet<>(Set.of(COLUMN));
        var original=new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,columns,merges)),catalogue,List.of(),List.of(),Map.of(),1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,columns,merges))),RatisPoolFixtures.groups(catalogue,List.of()));
        columns.clear(); merges.clear(); byte[] digest=original.identityDigest(); original.identityDigest()[0]^=1;
        assertArrayEquals(digest,original.identityDigest()); assertEquals(Map.of(COLUMN,"data"),original.columns()); assertEquals(Set.of(COLUMN),original.merges());
        assertThrows(UnsupportedOperationException.class,() -> original.columns().clear());
        assertThrows(UnsupportedOperationException.class,() -> original.merges().clear());
        assertThrows(UnsupportedOperationException.class,() -> catalogue.replicas().clear());
        var expanded=directory(5,List.of(range(HIGH_GROUP,null,new byte[] {127},List.of(local)),range(LOW_GROUP,new byte[] {127},null,List.of(local))));
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(expanded,Map.of(COLUMN,"data"),Set.of(COLUMN))),catalogue,List.of(),List.of(),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(expanded,Map.of(COLUMN,"data"),Set.of(COLUMN)))),RatisPoolFixtures.groups(catalogue,List.of())).identityDigest()));
        assertArrayEquals(digest,new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory(5,directory.ranges()),Map.of(COLUMN,"data"),Set.of(COLUMN))),catalogue,List.of(),List.of(),Map.of(),1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory(5,directory.ranges()),Map.of(COLUMN,"data"),Set.of(COLUMN)))),RatisPoolFixtures.groups(catalogue,List.of())).identityDigest());
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"other"),Set.of(COLUMN))),catalogue,List.of(),List.of(),Map.of(),1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"other"),Set.of(COLUMN)))),RatisPoolFixtures.groups(catalogue,List.of())).identityDigest()));
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of())),catalogue,List.of(),List.of(),Map.of(),1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of()))),RatisPoolFixtures.groups(catalogue,List.of())).identityDigest()));
        var changedCatalogue=new RatisNodePlan.Catalogue(CATALOGUE,2,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(remote()));
        assertFalse(Arrays.equals(digest,new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of(COLUMN))),changedCatalogue,List.of(),List.of(),Map.of(),1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory,Map.of(COLUMN,"data"),Set.of(COLUMN)))),RatisPoolFixtures.groups(changedCatalogue,List.of())).identityDigest()));
    }
    private static RatisNodePlan.Home home(RatisNodePlan.Role role,UUID group,UUID column,UUID domain,String name,List<ReplicaDescriptor> replicas) {
        byte[] schema=new byte[32]; schema[0]=9;
        var directory=new RangeDirectory(CLUSTER,DATABASE,domain,7,schema,11,Set.of(column),0,List.of(range(group,null,null,replicas)));
        return new RatisNodePlan.Home(role,directory,column,name);
    }
    private static RatisNodePlan withHomes(List<RatisNodePlan.Home> homes) {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        return new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory(0,List.of(range(HIGH_GROUP,null,null,List.of(local)))),Map.of(COLUMN,"data"),Set.of())),catalogue(List.of(local,remote())),homes,List.of(),Map.of(),100,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory(0,List.of(range(HIGH_GROUP,null,null,List.of(local)))),Map.of(COLUMN,"data"),Set.of()))),RatisPoolFixtures.groups(catalogue(List.of(local,remote())),homes));
    }
    @Test void dedicatedHomeRolesAreQualifiedCanonicalAndSeparateFromDataAndCatalogue() {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var tx=home(RatisNodePlan.Role.TRANSACTION_HOME,new UUID(0,100),new UUID(0,200),new UUID(0,300),"transaction-homes",List.of(local));
        var snapshot=home(RatisNodePlan.Role.SNAPSHOT_HOME,new UUID(0,101),new UUID(0,201),new UUID(0,301),"snapshot-homes",List.of(local));
        var plan=withHomes(List.of(snapshot,tx)); assertEquals(List.of(tx,snapshot),plan.homes()); assertEquals(4,plan.localGroups().size());
        assertEquals(RatisNodePlan.Role.DATA,plan.role(HIGH_GROUP)); assertEquals(RatisNodePlan.Role.CATALOGUE,plan.role(CATALOGUE)); assertEquals(tx.role(),plan.role(new UUID(0,100)));
        assertSame(tx,plan.home(tx.role())); assertSame(tx,plan.home(new UUID(0,100))); assertNull(plan.home(HIGH_GROUP)); assertNull(plan.home(RatisNodePlan.Role.DATA));
        assertSame(tx.directory(),plan.groupDirectory(new UUID(0,100))); assertEquals(tx.directory().ranges().getFirst(),plan.groupDescriptor(new UUID(0,100)));
        assertThrows(IllegalArgumentException.class,() -> plan.groupDirectory(CATALOGUE)); assertThrows(IllegalArgumentException.class,() -> plan.role(UUID.randomUUID()));
        assertThrows(UnsupportedOperationException.class,() -> plan.homes().clear()); assertArrayEquals(plan.identityDigest(),withHomes(List.of(tx,snapshot)).identityDigest());
        assertFalse(Arrays.equals(plan.identityDigest(),withHomes(List.of(tx)).identityDigest()));
        var changed=new RatisNodePlan.Home(tx.role(),new RangeDirectory(CLUSTER,DATABASE,tx.directory().domainId(),8,tx.directory().schemaDigest(),12,Set.of(tx.column()),0,tx.directory().ranges()),tx.column(),tx.physicalName());
        assertFalse(Arrays.equals(withHomes(List.of(tx)).identityDigest(),withHomes(List.of(changed)).identityDigest()));
        assertFalse(Arrays.equals(withHomes(List.of(tx)).identityDigest(),withHomes(List.of(new RatisNodePlan.Home(RatisNodePlan.Role.SNAPSHOT_HOME,tx.directory(),tx.column(),tx.physicalName()))).identityDigest()));
    }
    @Test void homeRolesCannotAliasGroupsColumnsPhysicalNamesDomainsOrGlobalReplicaIdentity() {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var tx=home(RatisNodePlan.Role.TRANSACTION_HOME,new UUID(0,100),new UUID(0,200),new UUID(0,300),"transaction-homes",List.of(local));
        for (var column:List.of(COLUMN,DIRECTORY_COLUMN,SESSIONS_COLUMN)) assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(home(tx.role(),new UUID(0,100),column,new UUID(0,300),tx.physicalName(),List.of(local)))));
        for (var group:List.of(HIGH_GROUP,CATALOGUE)) assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(home(tx.role(),group,tx.column(),new UUID(0,300),tx.physicalName(),List.of(local)))));
        for (var domain:List.of(DOMAIN,CATALOGUE)) assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(home(tx.role(),new UUID(0,100),tx.column(),domain,tx.physicalName(),List.of(local)))));
        assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(home(tx.role(),new UUID(0,100),tx.column(),new UUID(0,300),"data",List.of(local)))));
        assertThrows(IllegalArgumentException.class,() -> home(tx.role(),new UUID(0,100),tx.column(),new UUID(0,300),"catalogue-directory",List.of(local)));
        var snapshot=home(RatisNodePlan.Role.SNAPSHOT_HOME,new UUID(0,101),new UUID(0,201),new UUID(0,300),"snapshot-homes",List.of(local));
        assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(tx,snapshot)));
        assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(tx,new RatisNodePlan.Home(tx.role(),snapshot.directory(),snapshot.column(),snapshot.physicalName()))));
        var bad=home(tx.role(),new UUID(0,100),tx.column(),new UUID(0,300),tx.physicalName(),List.of(local("wrong","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER)));
        assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(bad)));
        var wrongDatabase=new RangeDirectory(CLUSTER,new UUID(0,999),tx.directory().domainId(),7,tx.directory().schemaDigest(),11,Set.of(tx.column()),0,tx.directory().ranges());
        assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(new RatisNodePlan.Home(tx.role(),wrongDatabase,tx.column(),tx.physicalName()))));
    }
    @Test void sharedHomeColumnCanSpanShardsButRoleScopeAndTotalHomeGroupBudgetStayBounded() {
        var local=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER); UUID column=new UUID(0,200);
        var ranges=new ArrayList<RangeDescriptor>();
        for (int i=0;i<40;i++) ranges.add(range(new UUID(0,2000+i),i==0 ? null : new byte[]{(byte)i},i==39 ? null : new byte[]{(byte)(i+1)},List.of(local)));
        var tx=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,new RangeDirectory(CLUSTER,DATABASE,new UUID(0,300),1,new byte[32],0,Set.of(column),0,ranges),column,"transaction-homes");
        assertEquals(42,withHomes(List.of(tx)).localGroups().size());
        var more=new ArrayList<RangeDescriptor>(); for (int i=0;i<25;i++) more.add(range(new UUID(0,3000+i),i==0 ? null : new byte[]{(byte)i},i==24 ? null : new byte[]{(byte)(i+1)},List.of(local)));
        var snapshot=new RatisNodePlan.Home(RatisNodePlan.Role.SNAPSHOT_HOME,new RangeDirectory(CLUSTER,DATABASE,new UUID(0,301),1,new byte[32],0,Set.of(new UUID(0,201)),0,more),new UUID(0,201),"snapshot-homes");
        assertThrows(IllegalArgumentException.class,() -> withHomes(List.of(tx,snapshot)));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Home(RatisNodePlan.Role.DATA,tx.directory(),column,tx.physicalName()));
        var projected=new RangeDirectory(CLUSTER,DATABASE,new UUID(0,300),1,new byte[32],0,Set.of(column),1,List.of(range(new UUID(0,100),null,null,List.of(local))));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Home(tx.role(),projected,column,tx.physicalName()));
        var inactive=new RangeDescriptor(new UUID(0,100),new UUID(0,100),1,0,null,null,RangeDescriptor.TopologyState.FENCED,List.of(local));
        var inactiveDirectory=new RangeDirectory(CLUSTER,DATABASE,new UUID(0,300),1,new byte[32],0,Set.of(column),0,List.of(inactive));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan.Home(tx.role(),inactiveDirectory,column,tx.physicalName()));
    }
    private static RatisNodePlan.Domain dataDomain(int n,int columns) {
        var names=new LinkedHashMap<UUID,String>(); for(int i=0;i<columns;i++) names.put(new UUID(n,100+i),"domain-"+n+"-"+i); var range=range(new UUID(n,200),null,null,List.of(local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER)));
        var directory=new RangeDirectory(CLUSTER,DATABASE,new UUID(n,300),1,new byte[32],0,names.keySet(),0,List.of(range)); return new RatisNodePlan.Domain(directory,names,Set.of());
    }
    @Test void twoDataDomainsShareOneNamespaceAndCanonicalAggregateBindingsWithCorrectGroupLookup() {
        var first=dataDomain(1,1); var second=dataDomain(2,2); var cat=catalogue(List.of(remote())); var plan=new RatisNodePlan(NODE,INCARNATION,List.of(second,first),cat,List.of(),List.of(),Map.of(),20,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(second,first)),RatisPoolFixtures.groups(cat,List.of()));
        assertEquals(List.of(first,second),plan.domains()); assertSame(second,plan.domain(second.domainId())); assertSame(second.directory(),plan.groupDirectory(second.directory().ranges().getFirst().groupId())); assertEquals(3,plan.columns().size()); assertEquals(20,plan.maxLocalGroups()); assertEquals(2,plan.localGroups().size());
        var ordered=new RatisNodePlan(NODE,INCARNATION,List.of(first,second),cat,List.of(),List.of(),Map.of(),20,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,second)),RatisPoolFixtures.groups(cat,List.of())); assertArrayEquals(plan.identityDigest(),ordered.identityDigest()); assertFalse(Arrays.equals(plan.identityDigest(),new RatisNodePlan(NODE,INCARNATION,List.of(first,second),cat,List.of(),List.of(),Map.of(),21,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,second)),RatisPoolFixtures.groups(cat,List.of())).identityDigest()));
        assertThrows(UnsupportedOperationException.class,()->plan.domains().clear()); assertThrows(IllegalArgumentException.class,()->plan.domain(UUID.randomUUID()));
    }
    @Test void globalDataCollisionsAndAggregateBudgetsCannotHideBehindDistinctDomains() {
        var first=dataDomain(1,1); var second=dataDomain(2,1); var cat=catalogue(List.of(remote()));
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(first,first),cat,List.of(),List.of(),Map.of(),20,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,first)),RatisPoolFixtures.groups(cat,List.of())));
        var range=second.directory().ranges().getFirst(); var sameRange=new RangeDescriptor(first.directory().ranges().getFirst().rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),null,null,range.topologyState(),range.replicas()); var sameGroup=new RangeDescriptor(range.rangeId(),first.directory().ranges().getFirst().groupId(),range.rangeEpoch(),range.configurationVersion(),null,null,range.topologyState(),range.replicas());
        for(var changed:List.of(sameRange,sameGroup)) { var directory=new RangeDirectory(CLUSTER,DATABASE,second.domainId(),1,new byte[32],0,second.columns().keySet(),0,List.of(changed)); assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(first,new RatisNodePlan.Domain(directory,second.columns(),Set.of())),cat,List.of(),List.of(),Map.of(),20,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,new RatisNodePlan.Domain(directory,second.columns(),Set.of()))),RatisPoolFixtures.groups(cat,List.of()))); }
        var alias=new RatisNodePlan.Domain(second.directory(),Map.of(second.columns().keySet().iterator().next(),first.columns().values().iterator().next()),Set.of()); assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(first,alias),cat,List.of(),List.of(),Map.of(),20,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,alias)),RatisPoolFixtures.groups(cat,List.of())));
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(dataDomain(1,129),dataDomain(2,128)),cat,List.of(),List.of(),Map.of(),20,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(dataDomain(1,129),dataDomain(2,128))),RatisPoolFixtures.groups(cat,List.of())));
        var many=new ArrayList<RatisNodePlan.Domain>(); for(int n=1;n<=65;n++) many.add(dataDomain(n,1)); assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,many,cat,List.of(),List.of(),Map.of(),100,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(many),RatisPoolFixtures.groups(cat,List.of())));
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(first,second),cat,List.of(),List.of(),Map.of(),1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,second)),RatisPoolFixtures.groups(cat,List.of())));
    }
    private static RatisNodePlan.Domain manyRanges(int domain,int count) {
        var ranges=new ArrayList<RangeDescriptor>(); var replica=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        for(int i=0;i<count;i++) { byte[] lower=i==0 ? null : new byte[]{(byte)(i>>>8),(byte)i}; int next=i+1; byte[] upper=i==count-1 ? null : new byte[]{(byte)(next>>>8),(byte)next}; ranges.add(range(new UUID(domain,1000+i),lower,upper,List.of(replica))); }
        UUID column=new UUID(domain,999); var directory=new RangeDirectory(CLUSTER,DATABASE,new UUID(domain,900),1,new byte[32],0,Set.of(column),2,ranges); return new RatisNodePlan.Domain(directory,Map.of(column,"many-"+domain),Set.of());
    }
    @Test void dataGroupCapacityIsDatabaseWideRatherThanMultipliedByDomainCount() {
        var cat=catalogue(List.of(remote())); var first=manyRanges(1,512); var second=manyRanges(2,513); assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(first,second),cat,List.of(),List.of(),Map.of(),RatisNodePlan.MAX_DATA_GROUPS+1,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(first,second)),RatisPoolFixtures.groups(cat,List.of())));
    }

    @Test void sequentialImportsQualifyEveryInitiallyFencedGroupWithoutRebasing() {
        var replica=local("local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var a=new RangeDescriptor(LOW_GROUP,LOW_GROUP,1,0,null,new byte[]{64},RangeDescriptor.TopologyState.FENCED,List.of(replica));
        var b=new RangeDescriptor(HIGH_GROUP,HIGH_GROUP,1,0,new byte[]{64},null,RangeDescriptor.TopologyState.FENCED,List.of(replica));
        var initial=directory(0,List.of(a,b));var domain=new RatisNodePlan.Domain(initial,Map.of(COLUMN,"data"),Set.of());
        var firstId=new UUID(4,2);var secondId=new UUID(4,1);
        var firstManifest=new NativeSstImportManifest(firstId,a.groupId(),1,DOMAIN,initial.schemaDigest(),a.lowerInclusive(),a.upperExclusive(),List.of(new NativeSstImportManifest.Column(COLUMN,"data",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS);
        var secondManifest=new NativeSstImportManifest(secondId,b.groupId(),1,DOMAIN,initial.schemaDigest(),b.lowerInclusive(),b.upperExclusive(),List.of(new NativeSstImportManifest.Column(COLUMN,"data",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS);
        var first=new RangeImportDefinition(firstId,new OperationId(new UUID(4,3),1,0),initial,List.of(firstManifest));
        var second=new RangeImportDefinition(secondId,new OperationId(new UUID(4,3),1,1),first.targetDirectory(),List.of(secondManifest));
        var plan=new RatisNodePlan(NODE,INCARNATION,List.of(domain),catalogue(List.of(remote())),List.of(),List.of(second,first),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue(List.of(remote())),List.of()));
        assertEquals(List.of(first,second),plan.imports());assertSame(first,plan.importDefinition(a.groupId()));assertSame(second,plan.importDefinition(b.groupId()));assertArrayEquals(first.targetDirectory().payloadDigest(),plan.importDefinition(b.groupId()).sourceDirectory().payloadDigest());
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(domain),catalogue(List.of(remote())),List.of(),List.of(second),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue(List.of(remote())),List.of())));
        var fork=new RangeImportDefinition(secondId,second.operation(),initial,List.of(secondManifest));
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(NODE,INCARNATION,List.of(domain),catalogue(List.of(remote())),List.of(),List.of(first,fork),Map.of(),2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue(List.of(remote())),List.of())));
    }

}
