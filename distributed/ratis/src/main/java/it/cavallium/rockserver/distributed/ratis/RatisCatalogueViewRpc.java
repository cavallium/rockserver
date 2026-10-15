package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** One atomic catalogue directory value and its genuinely completed native cut. */
public final class RatisCatalogueViewRpc {
    public static final int KIND=118,REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+80;
    public static final int MAX_REPLY_BYTES=REQUEST_BYTES+20+RatisCatalogue.MAX_DIRECTORY_BYTES;
    private RatisCatalogueViewRpc() {}
    public record Request(UUID cluster,UUID database,UUID catalogue,UUID column,UUID domain) {
        public Request {Objects.requireNonNull(cluster);Objects.requireNonNull(database);Objects.requireNonNull(catalogue);Objects.requireNonNull(column);Objects.requireNonNull(domain);}
    }
    public record View(Request request,ApplyPosition cut,RangeDirectory directory) {
        public View {Objects.requireNonNull(request);Objects.requireNonNull(cut);if(cut.term()<1)throw new IllegalArgumentException("Catalogue view lacks completed cut");if(directory!=null && (!directory.clusterId().equals(request.cluster()) || !directory.databaseId().equals(request.database()) || !directory.domainId().equals(request.domain())))throw new IllegalArgumentException("Catalogue view crossed namespace");}
    }
    public static byte[] request(Request r){var b=ByteBuffer.allocate(80);for(var id:List.of(r.cluster(),r.database(),r.catalogue(),r.column(),r.domain()))b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());return ScopedTransactionRpc.frame(KIND,b.array());}
    public static Request request(byte[] bytes){var p=ScopedTransactionRpc.parse(bytes,REQUEST_BYTES);if(p.kind()!=KIND || p.body().length!=80)throw new IllegalArgumentException("Invalid catalogue view request");var b=ByteBuffer.wrap(p.body());return new Request(uuid(b),uuid(b),uuid(b),uuid(b),uuid(b));}
    public static byte[] reply(View v){byte[] request=ScopedTransactionRpc.parse(request(v.request()),REQUEST_BYTES).body(),directory=v.directory()==null?new byte[0]:RangeDirectoryCodec.encode(v.directory());if(directory.length>RatisCatalogue.MAX_DIRECTORY_BYTES)throw new IllegalArgumentException("Directory view exceeds bound");return ScopedTransactionRpc.frame(KIND,ByteBuffer.allocate(100+directory.length).put(request).putLong(v.cut().term()).putLong(v.cut().index()).putInt(directory.length).put(directory).array());}
    public static View reply(byte[] bytes,Request expected){var p=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);var b=ByteBuffer.wrap(p.body());if(p.kind()!=KIND || b.remaining()<100)throw new IllegalArgumentException("Invalid catalogue view reply");var actual=new Request(uuid(b),uuid(b),uuid(b),uuid(b),uuid(b));if(!actual.equals(expected))throw new IllegalArgumentException("Catalogue view request changed");var cut=new ApplyPosition(b.getLong(),b.getLong());int n=b.getInt();if(n<0 || n>RatisCatalogue.MAX_DIRECTORY_BYTES || n!=b.remaining())throw new IllegalArgumentException("Catalogue directory framing changed");byte[] d=new byte[n];b.get(d);return new View(actual,cut,n==0?null:RangeDirectoryCodec.decode(d));}
    private static UUID uuid(ByteBuffer b){return new UUID(b.getLong(),b.getLong());}
}
