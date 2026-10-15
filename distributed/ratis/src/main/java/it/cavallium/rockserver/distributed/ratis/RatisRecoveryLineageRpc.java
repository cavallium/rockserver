package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Original committed birth provenance. An absent birth never grants ordinary-bootstrap permission. */
public final class RatisRecoveryLineageRpc {
    public static final int KIND=119,REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+136;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+188+TopologyChange.MAX_DEFINITION_BYTES;
    private RatisRecoveryLineageRpc() {}
    public record Request(UUID cluster,UUID database,UUID catalogue,UUID column,UUID domain,UUID range,UUID group,long epoch,ApplyPosition minimumCatalogueCut) {
        public Request {
            Objects.requireNonNull(cluster);Objects.requireNonNull(database);Objects.requireNonNull(catalogue);Objects.requireNonNull(column);Objects.requireNonNull(domain);Objects.requireNonNull(range);Objects.requireNonNull(group);Objects.requireNonNull(minimumCatalogueCut);
            if(epoch<0 || minimumCatalogueCut.term()<1)throw new IllegalArgumentException("Invalid recovery birth lineage/minimum catalogue cut");
        }
    }
    public record Reply(Request request,ApplyPosition catalogueCut,TopologyChange.Definition birth) {
        public Reply {
            Objects.requireNonNull(request);Objects.requireNonNull(catalogueCut);
            if(catalogueCut.term()<1 || !RatisRelocationOwnerRpc.covers(catalogueCut,request.minimumCatalogueCut()))throw new IllegalArgumentException("Recovery birth reply lacks required completed catalogue cut");
            if(birth!=null){var header=birth.header();if(birth.kind()==TopologyChange.Kind.RELOCATE || !header.clusterId().equals(request.cluster()) || !header.databaseId().equals(request.database()) || !header.domainId().equals(request.domain()))throw new IllegalArgumentException("Recovery birth namespace/kind differs");requireTarget(request,birth);}
        }
        public RangeDescriptor target(){return birth==null?null:requireTarget(request,birth);}
    }
    public static byte[] request(Request request){return ScopedTransactionRpc.frame(KIND,requestBody(request));}
    public static Request request(byte[] encoded){var packet=ScopedTransactionRpc.parse(encoded,REQUEST_BYTES);if(packet.kind()!=KIND || packet.body().length!=136)throw new IllegalArgumentException("Invalid recovery birth request");return readRequest(ByteBuffer.wrap(packet.body()));}
    public static byte[] reply(Reply reply){
        byte[] definition=reply.birth()==null?new byte[0]:reply.birth().encode();if(definition.length>TopologyChange.MAX_DEFINITION_BYTES)throw new IllegalArgumentException("Recovery birth definition exceeds bound");
        var body=ByteBuffer.allocate(156+(definition.length==0?0:32+definition.length)).put(requestBody(reply.request())).putLong(reply.catalogueCut().term()).putLong(reply.catalogueCut().index()).putInt(definition.length);
        if(definition.length!=0)body.put(reply.birth().digest()).put(definition);return ScopedTransactionRpc.frame(KIND,body.array());
    }
    public static Reply reply(byte[] encoded,Request expected){
        var packet=ScopedTransactionRpc.parse(encoded,MAX_REPLY_BYTES);var body=ByteBuffer.wrap(packet.body());if(packet.kind()!=KIND || body.remaining()<156)throw new IllegalArgumentException("Invalid recovery birth reply");
        var request=readRequest(body);if(!request.equals(expected))throw new IllegalArgumentException("Recovery birth reply changed exact request scope");var cut=new ApplyPosition(body.getLong(),body.getLong());int length=body.getInt();
        if(length<0 || length>TopologyChange.MAX_DEFINITION_BYTES || body.remaining()!=(length==0?0:32+length))throw new IllegalArgumentException("Recovery birth definition framing exceeds bound");
        TopologyChange.Definition definition=null;
        if(length!=0){byte[] digest=new byte[32],bytes=new byte[length];body.get(digest);body.get(bytes);definition=TopologyChange.Definition.decode(bytes);if(!Arrays.equals(digest,definition.digest()))throw new IllegalArgumentException("Recovery birth immutable definition digest changed");}
        var reply=new Reply(request,cut,definition);if(!Arrays.equals(encoded,reply(reply)))throw new IllegalArgumentException("Noncanonical recovery birth reply");return reply;
    }
    private static byte[] requestBody(Request request){var body=ByteBuffer.allocate(136);for(var id:List.of(request.cluster(),request.database(),request.catalogue(),request.column(),request.domain(),request.range(),request.group()))body.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());return body.putLong(request.epoch()).putLong(request.minimumCatalogueCut().term()).putLong(request.minimumCatalogueCut().index()).array();}
    private static Request readRequest(ByteBuffer body){return new Request(uuid(body),uuid(body),uuid(body),uuid(body),uuid(body),uuid(body),uuid(body),body.getLong(),new ApplyPosition(body.getLong(),body.getLong()));}
    private static UUID uuid(ByteBuffer body){return new UUID(body.getLong(),body.getLong());}
    private static RangeDescriptor requireTarget(Request request,TopologyChange.Definition birth){return birth.targets().stream().filter(range->range.groupId().equals(request.group()) && range.rangeId().equals(request.range()) && range.rangeEpoch()==request.epoch()).findFirst().orElseThrow(()->new IllegalArgumentException("Recovery birth target differs from requested lineage"));}
}
