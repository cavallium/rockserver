package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** One current durable node identity. Genesis scopes are original plan witnesses, not CAT authority. */
final class RatisNodeIdentity {
    enum Purpose { GENESIS, INITIAL_PLACEMENT, RESTORE_TARGET }
    private static final int HEADER_BYTES=1+32+RatisNativeExtentPolicy.BYTES+1+4,SCOPE_BYTES=16+32+1;
    /** Whole SessionJournalFiles file, including its checksum. Admission must precede its bounded read. */
    static final int MAX_BYTES=HEADER_BYTES+SCOPE_BYTES*RatisNodePlan.MAX_DOMAINS+32;
    private record Scope(byte[] digest,boolean closed) {
        Scope {if(digest.length!=32)throw new IllegalArgumentException("Invalid genesis directory digest");digest=digest.clone();}
    }
    private record Local(UUID node,UUID incarnation,String peer,String address) {static Local of(RatisNodePlan plan){return new Local(plan.nodeId(),plan.nodeIncarnation(),plan.peerId(),plan.address());}}
    private final Local local;
    private final int phase;
    private final byte[] planDigest;
    private final RatisNativeExtentPolicy nativePolicy;
    private final Purpose purpose;
    private final NavigableMap<UUID,Scope> scopes;

    private RatisNodeIdentity(int phase,byte[] planDigest,RatisNativeExtentPolicy nativePolicy,Purpose purpose,Map<UUID,Scope> scopes,Local local) {
        if(phase<0 || phase>2 || planDigest.length!=32 || scopes.size()>RatisNodePlan.MAX_DOMAINS || (purpose==Purpose.GENESIS)!=(!scopes.isEmpty()))throw new IllegalArgumentException("Invalid node identity phase/purpose/scopes");
        this.local=Objects.requireNonNull(local);this.phase=phase;this.planDigest=planDigest.clone();this.nativePolicy=Objects.requireNonNull(nativePolicy);this.purpose=Objects.requireNonNull(purpose);
        var sorted=new TreeMap<UUID,Scope>(RangeDirectoryCodec.UUID_ORDER);sorted.putAll(scopes);this.scopes=Collections.unmodifiableNavigableMap(sorted);
    }
    static RatisNodeIdentity create(RatisNodePlan plan,RatisNativeExtentPolicy nativePolicy,Purpose purpose) {
        Objects.requireNonNull(plan);Objects.requireNonNull(purpose);var scopes=new TreeMap<UUID,Scope>(RangeDirectoryCodec.UUID_ORDER);
        if(purpose==Purpose.GENESIS){requireGenesisPlan(plan);for(var domain:plan.domains())scopes.put(domain.domainId(),new Scope(domain.directory().payloadDigest(),false));}
        return new RatisNodeIdentity(0,plan.identityDigest(),nativePolicy,purpose,scopes,Local.of(plan));
    }
    int phase(){return phase;}
    Purpose purpose(){return purpose;}
    int encodedSize(){return HEADER_BYTES+SCOPE_BYTES*scopes.size();}
    Set<UUID> genesisDomains(){return scopes.navigableKeySet();}
    boolean genesisOpen(UUID domain){var scope=scopes.get(Objects.requireNonNull(domain));return scope!=null && !scope.closed();}
    RatisNodeIdentity withPhase(int next) {
        if(next<phase || next>2)throw new IllegalArgumentException("Node exposure phase cannot regress");
        return next==phase?this:new RatisNodeIdentity(next,planDigest,nativePolicy,purpose,scopes,local);
    }
    /** Caller forces this replacement before relying on observed CAT issuance/publication. */
    RatisNodeIdentity closeGenesis(Collection<UUID> domains) {
        Objects.requireNonNull(domains);if(domains.size()>RatisNodePlan.MAX_DOMAINS)throw new IllegalArgumentException("Genesis closure exceeds original scope bound");TreeMap<UUID,Scope> next=null;
        for(var domain:domains){var scope=scopes.get(Objects.requireNonNull(domain));if(scope==null)throw new IllegalArgumentException("No original genesis scope for domain");if(!scope.closed()){if(next==null)next=new TreeMap<>(scopes);next.put(domain,new Scope(scope.digest(),true));}}
        return next==null?this:new RatisNodeIdentity(phase,planDigest,nativePolicy,purpose,next,local);
    }
    /** Also requires the caller's fresh add-only roster and absent-directory proof before creation. */
    void requireGenesisScope(RangeDirectory directory,RangeDescriptor range,ReplicaDescriptor replica) {
        var scope=scopes.get(directory.domainId());
        if(scope==null || scope.closed() || directory.generation()!=0 || !Arrays.equals(scope.digest(),directory.payloadDigest()) || range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || !directory.ranges().contains(range) || !range.replicas().contains(replica) || !local.node().equals(replica.nodeId()) || !local.incarnation().equals(replica.nodeIncarnation()) || !local.peer().equals(replica.peerId()) || !local.address().equals(replica.address()))throw new IllegalArgumentException("Genesis request differs from its exact open original scope");
    }
    byte[] encode() {
        var out=ByteBuffer.allocate(encodedSize()).put((byte)phase).put(planDigest).put(nativePolicy.encode()).put((byte)purpose.ordinal()).putInt(scopes.size());
        scopes.forEach((domain,scope)->out.putLong(domain.getMostSignificantBits()).putLong(domain.getLeastSignificantBits()).put(scope.digest()).put((byte)(scope.closed()?1:0)));return out.array();
    }
    static RatisNodeIdentity decode(byte[] bytes,RatisNodePlan plan,RatisNativeExtentPolicy nativePolicy) {
        if(bytes==null || bytes.length<HEADER_BYTES || bytes.length>MAX_BYTES-32)throw new IllegalArgumentException("Invalid bounded node identity size");
        Objects.requireNonNull(plan);Objects.requireNonNull(nativePolicy);var in=ByteBuffer.wrap(bytes);int phase=in.get()&255;byte[] digest=new byte[32];in.get(digest);var policy=RatisNativeExtentPolicy.read(in);int purpose=in.get()&255,count=in.getInt();
        if(phase>2 || purpose>=Purpose.values().length || count<0 || count>RatisNodePlan.MAX_DOMAINS || in.remaining()!=SCOPE_BYTES*count || !Arrays.equals(digest,plan.identityDigest()) || !Arrays.equals(policy.encode(),nativePolicy.encode()))throw new IllegalArgumentException("Node identity/schema/placement/native policy differs");
        var scopes=new TreeMap<UUID,Scope>(RangeDirectoryCodec.UUID_ORDER);UUID previous=null;
        for(int i=0;i<count;i++) {
            var domain=new UUID(in.getLong(),in.getLong());byte[] payload=new byte[32];in.get(payload);int closed=in.get()&255;
            if(closed>1 || previous!=null && RangeDirectoryCodec.UUID_ORDER.compare(previous,domain)>=0)throw new IllegalArgumentException("Noncanonical genesis scope identities/state");previous=domain;scopes.put(domain,new Scope(payload,closed==1));
        }
        var result=new RatisNodeIdentity(phase,digest,policy,Purpose.values()[purpose],scopes,Local.of(plan));
        if(result.purpose==Purpose.GENESIS){requireGenesisPlan(plan);if(count!=plan.domains().size())throw new IllegalArgumentException("Original genesis domain inventory changed");for(var domain:plan.domains()){var scope=scopes.get(domain.domainId());if(scope==null || !Arrays.equals(scope.digest(),domain.directory().payloadDigest()))throw new IllegalArgumentException("Original generation-zero directory changed");}}
        if(!Arrays.equals(bytes,result.encode()))throw new IllegalArgumentException("Noncanonical node identity");return result;
    }
    private static void requireGenesisPlan(RatisNodePlan plan) {
        if(!plan.imports().isEmpty() || plan.domains().stream().anyMatch(domain->domain.directory().generation()!=0 || domain.directory().ranges().stream().anyMatch(range->range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || range.configurationVersion()!=0)) || plan.homes().stream().anyMatch(home->home.directory().generation()!=0))throw new IllegalArgumentException("Genesis requires original active generation-zero placement without imports");
    }
}
