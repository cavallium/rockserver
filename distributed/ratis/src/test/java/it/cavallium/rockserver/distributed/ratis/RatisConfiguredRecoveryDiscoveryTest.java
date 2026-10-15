package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisConfiguredRecoveryDiscoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static final UUID CLUSTER=id(1),DATABASE=id(2),DOMAIN=id(3),COLUMN=id(4);
    private static UUID id(long n){return new UUID(987,n);}
    private static final ReplicaDescriptor LOCAL=new ReplicaDescriptor(id(5),id(6),id(7),"local","127.0.0.1:32001",ReplicaDescriptor.Role.VOTER);
    private static final ReplicaDescriptor REMOTE=new ReplicaDescriptor(id(8),id(9),id(10),"remote","127.0.0.1:32002",ReplicaDescriptor.Role.VOTER);
    private static RangeDescriptor range(long n,byte[] lower,byte[] upper,List<ReplicaDescriptor> replicas,long cfg){return new RangeDescriptor(id(n),id(n+100),1,cfg,lower,upper,RangeDescriptor.TopologyState.ACTIVE,replicas);}
    private static RangeDirectory directory(long generation,List<RangeDescriptor> ranges){return new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],generation,Set.of(COLUMN),1,ranges);}
    private static RatisNodePlan plan(RangeDirectory bootstrap,int limit){
        var catalogue=new RatisNodePlan.Catalogue(id(11),1,new byte[32],id(12),id(13),List.of(LOCAL,REMOTE));var domain=new RatisNodePlan.Domain(bootstrap,Map.of(COLUMN,"data"),Set.of());
        return new RatisNodePlan(LOCAL.nodeId(),LOCAL.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),limit,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue,List.of()));
    }
    private static final class WireSource implements RatisConfiguredRecoveryDiscovery.Source {
        RangeDirectory current;final Map<UUID,TopologyChange.Definition> births=new HashMap<>();long index=20;int views,birthReads;boolean changeLast,omitAdmission;
        WireSource(RangeDirectory current,TopologyChange.Definition... definitions){this.current=current;for(var d:definitions)for(var r:d.targets())births.put(r.groupId(),d);}
        public RatisCatalogueViewRpc.View view(RatisCatalogueViewRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException {
            views++;var directory=changeLast && views>1 ? new RangeDirectory(current.clusterId(),current.databaseId(),current.domainId(),current.schemaEpoch(),current.schemaDigest(),current.generation()+1,current.columns(),current.routingPrefixBytes(),current.ranges()) : current;
            byte[] wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,new ApplyPosition(1,index++),directory,null));if(!omitAdmission)admission.beforeReply(wire.length);return RatisCatalogueViewRpc.reply(wire,request);
        }
        public RatisRecoveryLineageRpc.Reply birth(RatisRecoveryLineageRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException {
            birthReads++;byte[] wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,new ApplyPosition(1,index++),births.get(request.group())));if(!omitAdmission)admission.beforeReply(wire.length);return RatisRecoveryLineageRpc.reply(wire,request);
        }
    }
    @Test void traversesBothMergeParentsFromNonlocalLeafAndUsesFinalConsumingCohort()throws Exception {
        var genesis=directory(1,List.of(range(20,null,null,List.of(REMOTE),0)));
        var split=new TopologyChange.Definition(id(30),genesis,directory(2,List.of(range(21,null,new byte[]{64},List.of(REMOTE),0),range(22,new byte[]{64},null,List.of(REMOTE),0))));
        // Both split children legitimately acquire this node before they are consumed by a merge.
        var finalParents=directory(4,List.of(range(21,null,new byte[]{64},List.of(LOCAL,REMOTE),1),range(22,new byte[]{64},null,List.of(LOCAL,REMOTE),1)));
        var merge=new TopologyChange.Definition(id(31),finalParents,directory(5,List.of(range(23,null,null,List.of(LOCAL,REMOTE),1))));
        var current=directory(6,List.of(range(23,null,null,List.of(REMOTE),2)));var source=new WireSource(current,split,merge);var charges=new ArrayList<Integer>();
        var result=RatisConfiguredRecoveryDiscovery.discover(plan(genesis,4),source,(retained,incoming)->charges.add(incoming));
        assertEquals(1,result.roots().size());assertEquals(2,result.definitions().size());assertEquals(4,result.edges().size());assertEquals(4,source.birthReads);assertEquals(6,charges.size());assertTrue(charges.stream().allMatch(n->n>0));
        assertEquals(Set.of(id(121),id(122)),new HashSet<>(result.localObligations().stream().map(o->o.range().groupId()).toList()));
        for(var obligation:result.localObligations()){assertEquals(RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,obligation.purpose());assertArrayEquals(merge.header().encode(),obligation.header().encode());assertArrayEquals(finalParents.payloadDigest(),obligation.sourceDirectory().payloadDigest());assertEquals(LOCAL,obligation.localReplica());}
        assertThrows(UnsupportedOperationException.class,()->result.roots().clear());
        assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(plan(genesis,2),new WireSource(current,split,merge),(retained,incoming)->{}));
    }
    @Test void ordersAncestorsBeforeLocalChildrenAndAcceptsClockOnlyCutProgress()throws Exception {
        var genesis=directory(1,List.of(range(40,null,null,List.of(LOCAL,REMOTE),0)));
        var split=new TopologyChange.Definition(id(50),genesis,directory(2,List.of(range(41,null,new byte[]{64},List.of(LOCAL,REMOTE),0),range(42,new byte[]{64},null,List.of(LOCAL,REMOTE),0))));
        var source=new WireSource(split.targetDirectory(),split);var result=RatisConfiguredRecoveryDiscovery.discover(plan(genesis,4),source,(retained,incoming)->{});
        assertEquals(3,result.localObligations().size());assertEquals(RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,result.localObligations().getFirst().purpose());
        assertEquals(id(140),result.localObligations().getFirst().range().groupId());assertTrue(result.localObligations().subList(1,3).stream().allMatch(o->o.purpose()==RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF));assertTrue(result.catalogueCut().index()>20);
    }
    @Test void duplicateBirthReplyIsTemporaryWhileUniqueGraphIsChargedOnce()throws Exception {
        var genesis=directory(1,List.of(range(80,null,null,List.of(REMOTE),0)));
        var split=new TopologyChange.Definition(id(85),genesis,directory(2,List.of(range(81,null,new byte[]{64},List.of(REMOTE),0),range(82,new byte[]{64},null,List.of(REMOTE),0))));
        var node=plan(genesis,4);var samples=new ArrayList<long[]>();
        var source=new WireSource(split.targetDirectory(),split);
        var result=RatisConfiguredRecoveryDiscovery.discover(node,source,(retained,incoming)->samples.add(new long[]{retained,incoming}));
        assertEquals(3,source.birthReads);assertEquals(1,result.definitions().size());assertEquals(5,samples.size());assertEquals(0,samples.getFirst()[0]);
        // Two leaves return the same complete split definition. Only one copy belongs to the graph.
        long retainedGraph=samples.getFirst()[1]+split.encode().length;
        long mapStorage=256L*(2+3)+128L*(2+1+1); // roots, nodes, edges, unique definitions, domains
        assertEquals(retainedGraph+mapStorage,samples.getLast()[0]);
        long budget=samples.stream().mapToLong(sample->sample[0]+sample[1]).max().orElseThrow();
        long cumulative=samples.stream().mapToLong(sample->sample[1]).sum();
        assertTrue(cumulative+mapStorage>budget,"Cumulative incoming traffic would falsely exceed this graph workspace");
        RatisConfiguredRecoveryDiscovery.discover(node,new WireSource(split.targetDirectory(),split),(retained,incoming)->{if(retained+incoming>budget)throw new IOException("Graph workspace exhausted");});
    }

    @Test void rejectsAbsentBirthWithoutExactBootstrapChangedViewBoundsAndMissingAdmission()throws Exception {
        var genesis=directory(1,List.of(range(60,null,null,List.of(LOCAL,REMOTE),0)));var node=plan(genesis,4);
        var absent=directory(2,List.of(range(61,null,null,List.of(LOCAL,REMOTE),0)));
        assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(node,new WireSource(absent),(retained,incoming)->{}));
        var changed=new WireSource(genesis);changed.changeLast=true;assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(node,changed,(retained,incoming)->{}));
        var omitted=new WireSource(genesis);omitted.omitAdmission=true;assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(node,omitted,(retained,incoming)->{}));
        var split=new TopologyChange.Definition(id(65),genesis,directory(2,List.of(range(62,null,new byte[]{64},List.of(LOCAL,REMOTE),0),range(63,new byte[]{64},null,List.of(LOCAL,REMOTE),0))));
        var wrong=directory(3,List.of(range(62,null,new byte[]{65},List.of(LOCAL,REMOTE),0),range(63,new byte[]{65},null,List.of(LOCAL,REMOTE),0)));
        assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(node,new WireSource(wrong,split),(retained,incoming)->{}));
        var rejected=new WireSource(genesis);assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(node,rejected,(retained,incoming)->{throw new IOException("No reply capacity");}));assertEquals(0,rejected.birthReads);
    }
    @Test void rejectsConflictingConsumersCyclesAndDefinitionOverflow()throws Exception {
        var genesis=directory(1,List.of(range(70,null,null,List.of(REMOTE),0)));
        var first=new TopologyChange.Definition(id(75),genesis,directory(2,List.of(range(71,null,new byte[]{64},List.of(REMOTE),0),range(72,new byte[]{64},null,List.of(REMOTE),0))));
        var conflicting=new TopologyChange.Definition(id(76),genesis,directory(2,List.of(range(73,null,new byte[]{64},List.of(REMOTE),0),range(74,new byte[]{64},null,List.of(REMOTE),0))));
        var impossible=directory(3,List.of(first.targets().getFirst(),conflicting.targets().getLast()));
        assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(plan(genesis,4),new WireSource(impossible,first,conflicting),(retained,incoming)->{}));
        var cycle=new TopologyChange.Definition(id(77),first.targetDirectory(),directory(3,genesis.ranges()));
        assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(plan(genesis,4),new WireSource(cycle.targetDirectory(),first,cycle),(retained,incoming)->{}));
        var history=new ArrayList<TopologyChange.Definition>();var current=genesis;
        for(int i=0;i<=RatisConfiguredRecoveryDiscovery.MAX_DEFINITIONS;i++) {
            long next=1000L+10L*i;var ranges=current.ranges().size()==1 ? List.of(range(next,null,new byte[]{64},List.of(REMOTE),0),range(next+1,new byte[]{64},null,List.of(REMOTE),0)) : List.of(range(next,null,null,List.of(REMOTE),0));
            var change=new TopologyChange.Definition(id(10000+i),current,directory(current.generation()+1,ranges));history.add(change);current=change.targetDirectory();
        }
        var overflow=new WireSource(current,history.toArray(TopologyChange.Definition[]::new));assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(plan(genesis,4),overflow,(retained,incoming)->{}));
    }

    @Test void twoDomainsClaimFirstFinalViewCutWhenEarlierCheckedDomainChangesLater()throws Exception {
        var a=directory(1,List.of(range(90,null,null,List.of(REMOTE),0)));UUID otherDomain=id(200),otherColumn=id(201);
        var b=new RangeDirectory(CLUSTER,DATABASE,otherDomain,1,new byte[32],1,Set.of(otherColumn),1,List.of(range(91,null,null,List.of(REMOTE),0)));
        var catalogue=new RatisNodePlan.Catalogue(id(11),1,new byte[32],id(12),id(13),List.of(LOCAL,REMOTE));
        var domains=List.of(new RatisNodePlan.Domain(a,Map.of(COLUMN,"data-a"),Set.of()),new RatisNodePlan.Domain(b,Map.of(otherColumn,"data-b"),Set.of()));
        var node=new RatisNodePlan(LOCAL.nodeId(),LOCAL.nodeIncarnation(),domains,catalogue,List.of(),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(domains),RatisPoolFixtures.groups(catalogue,List.of()));
        var live=new HashMap<UUID,RangeDirectory>(Map.of(DOMAIN,a,otherDomain,b));var viewCounts=new HashMap<UUID,Integer>();long[] index={20},firstFinal={-1},lastFinal={-1};
        var source=new RatisConfiguredRecoveryDiscovery.Source() {
            public RatisCatalogueViewRpc.View view(RatisCatalogueViewRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException {
                var directory=live.get(request.domain());long position=index[0]++;int count=viewCounts.merge(request.domain(),1,Integer::sum);
                byte[] wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,new ApplyPosition(1,position),directory,null));admission.beforeReply(wire.length);
                if(count==2) {
                    if(request.domain().equals(DOMAIN)) {
                        firstFinal[0]=position;
                        // A changes immediately after its checked view, before B's later final view.
                        live.put(DOMAIN,new RangeDirectory(a.clusterId(),a.databaseId(),a.domainId(),a.schemaEpoch(),a.schemaDigest(),a.generation()+1,a.columns(),a.routingPrefixBytes(),a.ranges()));index[0]++;
                    } else lastFinal[0]=position;
                }
                return RatisCatalogueViewRpc.reply(wire,request);
            }
            public RatisRecoveryLineageRpc.Reply birth(RatisRecoveryLineageRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException {
                byte[] wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,new ApplyPosition(1,index[0]++),null));admission.beforeReply(wire.length);return RatisRecoveryLineageRpc.reply(wire,request);
            }
        };
        var result=RatisConfiguredRecoveryDiscovery.discover(node,source,(retained,incoming)->{});
        assertEquals(2,result.roots().size());assertEquals(new ApplyPosition(1,firstFinal[0]),result.catalogueCut());assertTrue(lastFinal[0]>result.catalogueCut().index());assertEquals(a.generation()+1,live.get(DOMAIN).generation());assertEquals(a.generation(),result.roots().stream().filter(r->r.directory().domainId().equals(DOMAIN)).findFirst().orElseThrow().directory().generation());assertEquals(Map.of(DOMAIN,2,otherDomain,2),viewCounts);
    }

    @Test void fencedImportInImmutablePlanIsNotOrdinaryBootstrapWitness()throws Exception {
        var active=range(95,null,null,List.of(REMOTE),0);
        var fenced=new RangeDescriptor(active.rangeId(),active.groupId(),active.rangeEpoch(),active.configurationVersion(),null,null,RangeDescriptor.TopologyState.FENCED,active.replicas());
        var bootstrap=directory(1,List.of(fenced));var domain=new RatisNodePlan.Domain(bootstrap,Map.of(COLUMN,"data"),Set.of());
        var catalogue=new RatisNodePlan.Catalogue(id(11),1,new byte[32],id(12),id(13),List.of(LOCAL,REMOTE));UUID importId=id(96);
        var manifest=new NativeSstImportManifest(importId,fenced.groupId(),fenced.rangeEpoch(),DOMAIN,bootstrap.schemaDigest(),null,null,List.of(new NativeSstImportManifest.Column(COLUMN,"data",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS);
        var imported=new RangeImportDefinition(importId,new OperationId(id(97),1,0),bootstrap,List.of(manifest));
        var node=new RatisNodePlan(LOCAL.nodeId(),LOCAL.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(imported),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue,List.of()));
        var source=new WireSource(imported.targetDirectory());
        assertThrows(IOException.class,()->RatisConfiguredRecoveryDiscovery.discover(node,source,(retained,incoming)->{}));assertEquals(1,source.birthReads);
    }

    @Test void actualCommittedCatalogueSplitAndMergeProduceCompleteLocalObligations()throws Exception {
        for(boolean merge:List.of(false,true)) {
            var helper=new RatisCdcTopologyHandoffTest();helper.root=root.resolve("actual-"+merge);
            try(var fixture=helper.new Fixture(merge)) {
                fixture.activate();var topology=fixture.topology;var node=topology.plan;var store=topology.env.catalogue.store;store.enableCatalogueScopeCompaction();var binding=node.catalogue();
                var resolver=new RatisRecoveryLineage(node.clusterId(),node.databaseId(),binding.groupId(),store,binding.directoryColumn());
                var calls=new java.util.concurrent.atomic.AtomicInteger();
                var io=(org.apache.ratis.client.api.BlockingApi)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args)->{
                    assertEquals("sendReadOnly",method.getName());calls.incrementAndGet();byte[] query=((org.apache.ratis.protocol.Message)args[0]).getContent().toByteArray();byte[] wire;
                    int kind=ScopedTransactionRpc.kind(query,RatisRecoveryLineageRpc.REQUEST_BYTES);
                    if(kind==RatisCatalogueViewRpc.KIND) {
                        var request=RatisCatalogueViewRpc.request(query);byte[] key=java.nio.ByteBuffer.allocate(16).putLong(request.domain().getMostSignificantBits()).putLong(request.domain().getLeastSignificantBits()).array();
                        var read=store.readSchemaAuthorityPoints(List.of(new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),key),new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),RatisDatabaseDomainRoster.key(request.database()))),new int[]{RatisCatalogue.MAX_DIRECTORY_BYTES,DatabaseDomainRoster.MAX_BYTES});var directory=RangeDirectoryCodec.decode(read.values().getFirst());var roster=read.values().get(1);wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,read.position(),directory,roster==null?null:DatabaseDomainRoster.decode(roster)));
                    } else {
                        assertEquals(RatisRecoveryLineageRpc.KIND,kind);var request=RatisRecoveryLineageRpc.request(query);var lookup=resolver.lookup(new RatisRecoveryLineage.Request(request.domain(),request.range(),request.group(),request.epoch()),request.minimumCatalogueCut());wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,lookup.catalogueCut(),lookup.birth()==null?null:lookup.birth().definition()));
                    }
                    return topology.reply(binding.groupId(),wire);
                });
                var client=(org.apache.ratis.client.RaftClient)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args)->switch(method.getName()) {
                    case "getGroupId"->org.apache.ratis.protocol.RaftGroupId.valueOf(binding.groupId());case "io"->io;default->throw new AssertionError(method);
                });
                var source=new RatisCatalogue(client,node.clusterId(),node.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).recoveryDiscoverySource();
                var result=RatisConfiguredRecoveryDiscovery.discover(node,source,(retained,incoming)->{});assertEquals(topology.definition.targets().size(),result.roots().size());assertEquals(1,result.definitions().size());assertArrayEquals(topology.definition.encode(),result.definitions().getFirst().encode());assertEquals(topology.definition.parents().size()+topology.definition.targets().size(),result.localObligations().size());assertEquals(store.applied(),result.catalogueCut());assertEquals(2+topology.definition.parents().size()+topology.definition.targets().size(),calls.get());assertTrue(result.localObligations().subList(0,topology.definition.parents().size()).stream().allMatch(o->o.purpose()==RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA));
            }
        }
    }
}
