package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Frozen DATA/HOME bindings of one immutable native owner root. */
final class RatisSessionBindingPlan {
    static final int MAX_BYTES=12+3*(4+RangeDirectoryCodec.MAX_ENCODED_BYTES);
    private final RangeDirectory directory;private final List<RatisBackendSessions.Target> targets;private final byte[] encoded;
    private RatisSessionBindingPlan(RangeDirectory directory,List<RatisBackendSessions.Target> targets,List<RangeDirectory> placements){
        this.directory=directory;this.targets=List.copyOf(targets);int bytes=12;for(var placement:placements)bytes+=4+RangeDirectoryCodec.encode(placement).length;
        var out=ByteBuffer.allocate(bytes).putInt(placements.size()).putLong(directory.generation());for(var placement:placements){byte[] value=RangeDirectoryCodec.encode(placement);out.putInt(value.length).put(value);}encoded=out.array();
    }
    record Input(RangeDirectory directory,List<RangeDirectory> placements,List<RatisRoutedClient> routes) {
        long reservation(){long encoded=12,metadata=4L*(2L*SessionEpochLedger.MAX_BYTES+16384);for(var placement:placements){long size=RangeDirectoryCodec.encodedSize(placement);encoded+=4+size;metadata+=placement.ranges().size()*4L*(size+2L*SessionEpochLedger.MAX_BYTES+16384);}return metadata+8*encoded;}
        RatisSessionBindingPlan freeze(){return create(directory,placements,routes);}
    }
    static Input borrow(RangeDirectory data,List<RatisRoutedClient> routes){
        if(routes.isEmpty() || routes.size()>3)throw new IllegalArgumentException("Owner binding namespaces exceed bound");var placements=new ArrayList<RangeDirectory>();placements.add(data);for(var route:routes)if(!route.bootstrapDirectory().domainId().equals(data.domainId()))placements.add(route.directory());return new Input(data,List.copyOf(placements),routes);
    }
    private static RatisSessionBindingPlan create(RangeDirectory data,List<RangeDirectory> placements,List<RatisRoutedClient> routes){
        var targets=new ArrayList<RatisBackendSessions.Target>();var domains=new HashSet<UUID>();for(var placement:placements){if(!domains.add(placement.domainId()) || !placement.clusterId().equals(data.clusterId()) || !placement.databaseId().equals(data.databaseId()))throw new IllegalArgumentException("Foreign or duplicate owner binding namespace");var routed=routes.stream().filter(route->route.bootstrapDirectory().domainId().equals(placement.domainId())).findFirst().orElseThrow(()->new IllegalArgumentException("Original owner binding route unavailable"));for(var range:placement.ranges())targets.add(new RatisBackendSessions.Target(routed,range.groupId(),placement));}
        if(targets.size()>RatisNodePlan.MAX_DATA_GROUPS+TransactionStatus.MAX_PARTICIPANTS)throw new IllegalArgumentException("Owner binding group count exceeds native plan bound");return new RatisSessionBindingPlan(data,targets,placements);
    }
    static RatisSessionBindingPlan restore(byte[] bytes,List<RatisRoutedClient> routes)throws IOException {
        try{if(bytes.length>MAX_BYTES)throw new IllegalArgumentException("Owner binding plan too large");var in=ByteBuffer.wrap(bytes);int count=in.getInt();long generation=in.getLong();if(count<1 || count>3)throw new IllegalArgumentException("Owner namespace count changed");var placements=new ArrayList<RangeDirectory>();for(int n=0;n<count;n++){int length=in.getInt();if(length<1 || length>RangeDirectoryCodec.MAX_ENCODED_BYTES || length>in.remaining())throw new IllegalArgumentException("Owner directory length invalid");byte[] value=new byte[length];in.get(value);placements.add(RangeDirectoryCodec.decode(value));}if(in.hasRemaining() || placements.getFirst().generation()!=generation)throw new IllegalArgumentException("Owner directory plan is not canonical");var result=create(placements.getFirst(),placements,routes);if(!Arrays.equals(bytes,result.encoded))throw new IllegalArgumentException("Owner plan encoding changed");return result;}catch(RuntimeException invalid){throw new IOException("Invalid retained native owner binding plan",invalid);}
    }
    RangeDirectory directory(){return directory;}
    List<RatisBackendSessions.Target> targets(){return targets;}
    byte[] encode(){return encoded.clone();}
    byte[] digest(){return RemoteMutationToken.digest(encoded);}
    long metadataBytes(){long bytes=8L*encoded.length+4L*(2L*SessionEpochLedger.MAX_BYTES+16384);for(var target:targets)bytes+=4L*(RangeDirectoryCodec.encodedSize(target.directory())+2L*SessionEpochLedger.MAX_BYTES+16384);return bytes;}
}
