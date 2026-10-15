package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Fresh CAT graph discovery only. The caller durably freezes request identities before recovery IO. */
public final class RatisConfiguredRecoveryDiscovery {
    public static final int MAX_DEFINITIONS=128;
    public static final long MAX_DEFINITION_BYTES=64L<<20;
    private static final int MAX_GROUPS=RatisNodePlan.MAX_DATA_GROUPS+2*MAX_DEFINITIONS;
    /** Current retained graph plus one temporary incoming reply, never cumulative reply traffic. */
    @FunctionalInterface public interface Admission { void beforeReply(long retainedEncodedBytes,int incomingEncodedBytes)throws IOException; }
    @FunctionalInterface public interface ReplyAdmission { void beforeReply(int actualEncodedBytes)throws IOException; }
    /** Implementations must invoke admission exactly once, before copying or decoding the reply. */
    public interface Source {
        RatisCatalogueViewRpc.View view(RatisCatalogueViewRpc.Request request,ReplyAdmission admission)throws IOException;
        RatisRecoveryLineageRpc.Reply birth(RatisRecoveryLineageRpc.Request request,ReplyAdmission admission)throws IOException;
    }
    public record Identity(UUID domain,UUID range,UUID group,long epoch) {}
    public record Root(RangeDirectory directory,RangeDescriptor range) {}
    public record Edge(Identity parent,Identity child,TopologyChange.Header consuming) {}
    public record Obligation(RatisTopologySourceSnapshotRpc.Purpose purpose,RangeDirectory sourceDirectory,
                             RangeDescriptor range,ReplicaDescriptor localReplica,TopologyChange.Header header) {}
    public record Plan(ApplyPosition catalogueCut,List<Root> roots,List<TopologyChange.Definition> definitions,
                       List<Edge> edges,List<Obligation> localObligations) {
        public Plan {roots=List.copyOf(roots);definitions=List.copyOf(definitions);edges=List.copyOf(edges);localObligations=List.copyOf(localObligations);}
    }
    private RatisConfiguredRecoveryDiscovery() {}
    public static Plan discover(RatisNodePlan plan,Source source,Admission admission)throws IOException {
        return new Scan(Objects.requireNonNull(plan),Objects.requireNonNull(source),Objects.requireNonNull(admission)).run();
    }
    private static Identity id(RangeDirectory d,RangeDescriptor r){return new Identity(d.domainId(),r.rangeId(),r.groupId(),r.rangeEpoch());}
    private static boolean geometry(RangeDescriptor a,RangeDescriptor b){return a.groupId().equals(b.groupId()) && a.rangeId().equals(b.rangeId()) && a.rangeEpoch()==b.rangeEpoch() && Arrays.equals(a.lowerInclusive(),b.lowerInclusive()) && Arrays.equals(a.upperExclusive(),b.upperExclusive());}
    private static final class Charge implements ReplyAdmission {
        final Scan scan;final int maximum;boolean called;int received;
        Charge(Scan scan,int maximum){this.scan=scan;this.maximum=maximum;}
        public void beforeReply(int bytes)throws IOException {if(called || bytes<1 || bytes>maximum)throw new IOException("Discovery reply exceeds its single bounded admission");called=true;scan.admission.beforeReply(scan.retainedBytes(),bytes);received=bytes;}
        void require()throws IOException{if(!called)throw new IOException("Discovery source omitted reply admission");}
    }
    private static final class Scan {
        final RatisNodePlan plan;final Source source;final Admission admission;
        final List<Root> roots=new ArrayList<>();final Map<UUID,RangeDirectory> directories=new LinkedHashMap<>();
        final Map<Identity,RangeDescriptor> ranges=new LinkedHashMap<>();final Map<UUID,Identity> groups=new HashMap<>(),rangeIds=new HashMap<>();
        final Map<UUID,TopologyChange.Definition> definitions=new LinkedHashMap<>(),orderedDefinitions=new LinkedHashMap<>();final Map<Identity,TopologyChange.Definition> consumers=new HashMap<>();final Map<UUID,TopologyChange.Header> headers=new HashMap<>();
        final Set<Identity> visiting=new HashSet<>(),visited=new HashSet<>();final List<Identity> order=new ArrayList<>();final List<Edge> edges=new ArrayList<>();
        ApplyPosition cut=new ApplyPosition(1,0);long encodedBytes;
        Scan(RatisNodePlan plan,Source source,Admission admission){this.plan=plan;this.source=source;this.admission=admission;}
        Plan run()throws IOException {
            for(var domain:plan.domains()) {var directory=view(domain.domainId());if(roots.size()+directory.ranges().size()>MAX_GROUPS)throw new IOException("Recovery roots exceed group bound");directories.put(domain.domainId(),directory);for(var range:directory.ranges()){if(range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)throw new IOException("Current recovery root is not active");roots.add(new Root(directory,range));}}
            for(var root:roots)visit(root.directory(),root.range());
            var obligations=new ArrayList<Obligation>();
            for(var identity:order){var consuming=consumers.get(identity);if(consuming==null)continue;var range=consuming.parents().stream().filter(r->id(consuming.sourceDirectory(),r).equals(identity)).findFirst().orElseThrow();var local=local(range);if(local!=null)obligations.add(new Obligation(RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,consuming.sourceDirectory(),range,local,headers.get(consuming.intentId())));}
            for(var root:roots){if(consumers.containsKey(id(root.directory(),root.range())))throw new IOException("Current root is also a consumed ancestor");var local=local(root.range());if(local!=null)obligations.add(new Obligation(RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,root.directory(),root.range(),local,null));}
            long other=plan.localGroups().stream().filter(g->plan.role(g.getGroupId().getUuid())!=RatisNodePlan.Role.DATA).count();
            if(obligations.size()+other>plan.maxLocalGroups())throw new IOException("Discovered local recovery groups exceed node capacity");
            ApplyPosition commonCut=null;
            for(var domain:plan.domains()) {
                var latest=view(domain.domainId());if(commonCut==null)commonCut=cut;
                if(!Arrays.equals(directories.get(domain.domainId()).payloadDigest(),latest.payloadDigest()))throw new IOException("Catalogue placement/schema changed during discovery; rebuild unforced plan");
            }
            // Every birth precedes this first final view. Later final views confirm unchanged
            // generations across it; their later cuts cannot extend the already checked domains.
            return new Plan(Objects.requireNonNull(commonCut),roots,new ArrayList<>(orderedDefinitions.values()),edges,obligations);
        }
        RatisCatalogueViewRpc.Request viewRequest(UUID domain){return new RatisCatalogueViewRpc.Request(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId(),plan.catalogue().directoryColumn(),domain);}
        RangeDirectory view(UUID domain)throws IOException {
            var request=viewRequest(domain);var charge=new Charge(this,RatisCatalogueViewRpc.MAX_REPLY_BYTES);var reply=source.view(request,charge);charge.require();
            if(reply==null || !request.equals(reply.request()) || reply.directory()==null)throw new IOException("Fresh catalogue directory is absent or changed request");advance(reply.cut());if(!directories.containsKey(domain))retain(charge.received);return reply.directory();
        }
        // Encoded graphs are charged once. Conservative bounded map/list/identity/header storage is
        // charged separately; it does not grow with duplicate birth replies or final view checks.
        long retainedBytes(){return encodedBytes+256L*(roots.size()+ranges.size())+128L*(edges.size()+definitions.size()+directories.size());}
        void retain(int bytes)throws IOException {if(bytes>MAX_DEFINITION_BYTES-encodedBytes)throw new IOException("Recovery root/definition bytes exceeded");encodedBytes+=bytes;}
        void advance(ApplyPosition next)throws IOException {if(!RatisRelocationOwnerRpc.covers(next,cut))throw new IOException("Catalogue discovery cut regressed");cut=next;}
        void visit(RangeDirectory context,RangeDescriptor range)throws IOException {
            var identity=id(context,range);var oldGroup=groups.putIfAbsent(range.groupId(),identity);var oldRange=ranges.putIfAbsent(identity,range);var oldId=rangeIds.putIfAbsent(range.rangeId(),identity);
            if(oldGroup!=null && !oldGroup.equals(identity) || oldId!=null && !oldId.equals(identity) || oldRange!=null && !geometry(oldRange,range))throw new IOException("Recovery graph reused lineage with conflicting geometry");
            if(ranges.size()>MAX_GROUPS)throw new IOException("Recovery graph exceeds group bound");
            if(visiting.contains(identity))throw new IOException("Recovery lineage cycle");if(visited.contains(identity))return;visiting.add(identity);
            var request=new RatisRecoveryLineageRpc.Request(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId(),plan.catalogue().directoryColumn(),context.domainId(),range.rangeId(),range.groupId(),range.rangeEpoch(),cut);
            var charge=new Charge(this,RatisRecoveryLineageRpc.MAX_REPLY_BYTES);var reply=source.birth(request,charge);charge.require();
            if(reply==null || !request.equals(reply.request()))throw new IOException("Recovery birth changed request");advance(reply.catalogueCut());
            var birth=reply.birth();
            if(birth==null){var witness=plan.directory(context.domainId()).ranges().stream().filter(r->r.topologyState()==RangeDescriptor.TopologyState.ACTIVE && geometry(r,range)).findFirst();if(witness.isEmpty())throw new IOException("Absent birth lacks immutable plan bootstrap witness");}
            else {
                if(!geometry(reply.target(),range) || context.generation()<birth.targetDirectory().generation() || context.schemaEpoch()<birth.targetDirectory().schemaEpoch() || context.schemaEpoch()==birth.targetDirectory().schemaEpoch() && !Arrays.equals(context.schemaDigest(),birth.targetDirectory().schemaDigest()))throw new IOException("Recovery birth geometry/schema/generation exceeds descendant");
                var previous=definitions.get(birth.intentId());
                if(previous!=null && !Arrays.equals(previous.digest(),birth.digest()))throw new IOException("Conflicting immutable recovery definition");
                if(previous==null){if(definitions.size()>=MAX_DEFINITIONS)throw new IOException("Recovery definition count exceeded");retain(birth.encode().length);definitions.put(birth.intentId(),birth);headers.put(birth.intentId(),birth.header());}
                for(var parent:birth.parents()) {
                    var parentId=id(birth.sourceDirectory(),parent);var consumed=consumers.putIfAbsent(parentId,birth);
                    if(consumed!=null && !Arrays.equals(consumed.digest(),birth.digest()))throw new IOException("Ancestor has conflicting consuming definitions");
                    var edge=new Edge(parentId,identity,headers.get(birth.intentId()));if(!edges.contains(edge))edges.add(edge);
                    visit(birth.sourceDirectory(),parent);
                }
            }
            if(birth!=null)orderedDefinitions.putIfAbsent(birth.intentId(),birth);
            visiting.remove(identity);visited.add(identity);order.add(identity);
        }
        ReplicaDescriptor local(RangeDescriptor range)throws IOException {
            ReplicaDescriptor result=null;
            for(var replica:range.replicas())if(replica.nodeId().equals(plan.nodeId())) {
                if(!replica.nodeIncarnation().equals(plan.nodeIncarnation()))continue;
                if(!replica.peerId().equals(plan.peerId()) || !replica.address().equals(plan.address()) || result!=null)throw new IOException("Recovery cohort changed local peer identity");result=replica;
            }
            return result;
        }
    }
}
