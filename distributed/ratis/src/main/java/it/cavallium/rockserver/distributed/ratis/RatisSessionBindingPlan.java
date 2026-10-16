package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Exact frozen native targets of one immutable grant owner; namespaces share one database. */
final class RatisSessionBindingPlan {
    // Keep the existing aggregate encoding ceiling as namespace count grows.
    static final int MAX_BYTES=12+3*(4+RangeDirectoryCodec.MAX_ENCODED_BYTES);
    static final int MAX_NAMESPACES=DatabaseDomainRoster.MAX_DOMAINS+2;
    static final int MAX_TARGETS=RatisNodePlan.MAX_DATA_GROUPS+TransactionStatus.MAX_PARTICIPANTS;
    private static final Comparator<UUID> UUID_ORDER=(a,b)->{
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0?first:Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    record Selection(RangeDirectory directory,Set<UUID> groups) {
        Selection {
            Objects.requireNonNull(directory);Objects.requireNonNull(groups);
            if(groups.isEmpty() || groups.size()>MAX_TARGETS)throw new IllegalArgumentException("Invalid selected binding target count");
            groups=Set.copyOf(groups);var active=new HashSet<UUID>();
            for(var range:directory.ranges())if(range.topologyState()==RangeDescriptor.TopologyState.ACTIVE)active.add(range.groupId());
            if(!active.containsAll(groups))throw new IllegalArgumentException("Selected target absent from frozen active directory");
        }
        static Selection all(RangeDirectory directory) {
            var groups=new HashSet<UUID>();
            for(var range:directory.ranges())if(range.topologyState()==RangeDescriptor.TopologyState.ACTIVE)groups.add(range.groupId());
            return new Selection(directory,groups);
        }
    }
    private final List<Selection> selections;
    private final List<RatisBackendSessions.Target> targets;
    private final byte[] encoded;
    private RatisSessionBindingPlan(List<Selection> selections,List<RatisBackendSessions.Target> targets) {
        this.selections=selections;this.targets=List.copyOf(targets);
        var out=ByteBuffer.allocate(encodedSize(selections)).putInt(selections.size());
        for(var selection:selections) {
            byte[] placement=RangeDirectoryCodec.encode(selection.directory());
            out.putInt(placement.length).put(placement).putInt(selection.groups().size());
            selection.groups().stream().sorted(UUID_ORDER).forEach(group->out.putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()));
        }
        encoded=out.array();
    }
    record Input(List<Selection> selections,List<RatisRoutedClient> routes) {
        Input {selections=canonical(selections);qualify(selections,routes);routes=List.copyOf(routes);}
        long reservation(){return metadataBytes(selections,encodedSize(selections));}
        RatisSessionBindingPlan freeze(){return create(selections,routes);}
    }
    static Input borrow(List<Selection> selections,List<RatisRoutedClient> routes){return new Input(selections,routes);}
    /** Long-lived POINT/BULK owners intentionally bind all active groups of their configured namespaces. */
    static Input all(RangeDirectory data,List<RatisRoutedClient> routes) {
        var selections=new ArrayList<Selection>();selections.add(Selection.all(data));
        for(var route:routes)if(!route.bootstrapDirectory().domainId().equals(data.domainId()))selections.add(Selection.all(route.directory()));
        return borrow(selections,routes);
    }
    private static List<Selection> canonical(List<Selection> selections) {
        if(selections.isEmpty() || selections.size()>MAX_NAMESPACES)throw new IllegalArgumentException("Owner binding namespace count exceeds bound");
        var ordered=selections.stream().sorted(Comparator.comparing(s->s.directory().domainId(),UUID_ORDER)).toList();
        var namespaces=new HashSet<UUID>();var groups=new HashSet<UUID>();var first=ordered.getFirst().directory();
        for(var selection:ordered) {
            var placement=selection.directory();
            if(!namespaces.add(placement.domainId()) || !placement.clusterId().equals(first.clusterId()) || !placement.databaseId().equals(first.databaseId()))throw new IllegalArgumentException("Foreign or duplicate owner binding namespace");
            for(var group:selection.groups())if(!groups.add(group))throw new IllegalArgumentException("Duplicate native binding group across namespaces");
            if(groups.size()>MAX_TARGETS)throw new IllegalArgumentException("Aggregate owner target count exceeds bound");
        }
        encodedSize(ordered);return ordered;
    }
    private static Map<UUID,RatisRoutedClient> qualify(List<Selection> selections,List<RatisRoutedClient> routes) {
        if(routes.isEmpty() || routes.size()>MAX_NAMESPACES)throw new IllegalArgumentException("Owner binding route count exceeds bound");
        var byDomain=new HashMap<UUID,RatisRoutedClient>();var first=selections.getFirst().directory();
        for(var route:routes) {
            var identity=route.bootstrapDirectory();
            if(!identity.clusterId().equals(first.clusterId()) || !identity.databaseId().equals(first.databaseId()) || byDomain.put(identity.domainId(),route)!=null)throw new IllegalArgumentException("Foreign or duplicate owner binding route");
        }
        for(var selection:selections)if(!byDomain.containsKey(selection.directory().domainId()))throw new IllegalArgumentException("Original owner binding route unavailable");
        return byDomain;
    }
    private static RatisSessionBindingPlan create(List<Selection> selections,List<RatisRoutedClient> routes) {
        var byDomain=qualify(selections,routes);var targets=new ArrayList<RatisBackendSessions.Target>();
        for(var selection:selections) {
            var routed=byDomain.get(selection.directory().domainId());
            selection.groups().stream().sorted(UUID_ORDER).forEach(group->targets.add(new RatisBackendSessions.Target(routed,group,selection.directory())));
        }
        return new RatisSessionBindingPlan(selections,targets);
    }
    private static int encodedSize(List<Selection> selections) {
        long bytes=4;
        for(var selection:selections) {
            bytes+=8L+RangeDirectoryCodec.encodedSize(selection.directory())+16L*selection.groups().size();
            if(bytes>MAX_BYTES)throw new IllegalArgumentException("Owner binding plan exceeds aggregate byte bound");
        }
        return (int)bytes;
    }
    private static long metadataBytes(List<Selection> selections,int encodedBytes) {
        long bytes=8L*encodedBytes+4L*(2L*SessionEpochLedger.MAX_BYTES+16384);
        for(var selection:selections)bytes+=selection.groups().size()*4L*(RangeDirectoryCodec.encodedSize(selection.directory())+2L*SessionEpochLedger.MAX_BYTES+16384);
        return bytes;
    }
    static RatisSessionBindingPlan restore(byte[] bytes,List<RatisRoutedClient> routes)throws IOException {
        try {
            if(bytes.length<4 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Owner binding plan length invalid");
            var in=ByteBuffer.wrap(bytes);int count=in.getInt(),targets=0;
            if(count<1 || count>MAX_NAMESPACES)throw new IllegalArgumentException("Owner namespace count exceeds bound");
            var selections=new ArrayList<Selection>();
            for(int n=0;n<count;n++) {
                int length=in.getInt();
                if(length<1 || length>RangeDirectoryCodec.MAX_ENCODED_BYTES || length>in.remaining()-4)throw new IllegalArgumentException("Owner directory length invalid");
                byte[] value=new byte[length];in.get(value);var directory=RangeDirectoryCodec.decode(value);
                int groupCount=in.getInt();
                if(groupCount<1 || groupCount>MAX_TARGETS-targets || 16L*groupCount>in.remaining())throw new IllegalArgumentException("Owner selected target count invalid");
                targets+=groupCount;var groups=new HashSet<UUID>();
                for(int g=0;g<groupCount;g++)if(!groups.add(new UUID(in.getLong(),in.getLong())))throw new IllegalArgumentException("Duplicate owner binding target");
                selections.add(new Selection(directory,groups));
            }
            if(in.hasRemaining())throw new IllegalArgumentException("Trailing owner binding bytes");
            var result=borrow(selections,routes).freeze();
            if(!Arrays.equals(bytes,result.encoded))throw new IllegalArgumentException("Owner plan encoding is not canonical");
            return result;
        } catch(RuntimeException invalid) {throw new IOException("Invalid retained native owner binding plan",invalid);}
    }
    RangeDirectory directory(UUID domain){return selections.stream().filter(selection->selection.directory().domainId().equals(domain)).findFirst().orElseThrow(()->new IllegalArgumentException("Domain absent from frozen binding plan")).directory();}
    List<RatisBackendSessions.Target> targets(){return targets;}
    byte[] encode(){return encoded.clone();}
    byte[] digest(){return RemoteMutationToken.digest(encoded);}
    long metadataBytes(){return metadataBytes(selections,encoded.length);}
}
