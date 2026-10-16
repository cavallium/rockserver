package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** A committed HOME admission fence, not a source publication or a claim that old work completed. */
public record NativeHomeSourceClosing(Request request, ApplyPosition position) {
    private static final int MAGIC=0x48034c53;
    public static final int MAX_BYTES=24+Request.MAX_BYTES;
    public record Request(UUID home,long homeEpoch,byte[] schema,long configurationTerm,long configurationIndex,
                          byte[] configuration,NativeSourceClosing source) {
        public static final int MAX_BYTES=80+65536+NativeSourceClosing.MAX_BYTES;
        public Request {
            Objects.requireNonNull(home);Objects.requireNonNull(schema);Objects.requireNonNull(configuration);Objects.requireNonNull(source);
            if(homeEpoch<0 || schema.length!=32 || configurationTerm<0 || configurationIndex< -1 || (configurationIndex<0)!=(configurationTerm==0) || configuration.length==0 || configuration.length>65536)throw new IllegalArgumentException("Invalid HOME closing owner/configuration");
            schema=schema.clone();configuration=configuration.clone();
        }
        @Override public byte[] schema(){return schema.clone();}
        @Override public byte[] configuration(){return configuration.clone();}
        public boolean references(UUID domain,UUID range,UUID group,long epoch){var h=source.request().header();var p=h.participant(TopologyChange.Stage.FREEZE,source.request().group());return h.domainId().equals(domain) && p.rangeId().equals(range) && p.groupId().equals(group) && p.rangeEpoch()==epoch;}
        public boolean references(OperationRerouteOrigin origin){var h=source.request().header();return h.clusterId().equals(origin.clusterId()) && h.databaseId().equals(origin.databaseId()) && references(origin.domainId(),origin.source().rangeId(),origin.source().groupId(),origin.source().rangeEpoch());}
        public byte[] encode(){byte[] body=source.encode();return ByteBuffer.allocate(80+configuration.length+body.length).putLong(home.getMostSignificantBits()).putLong(home.getLeastSignificantBits()).putLong(homeEpoch).put(schema).putLong(configurationTerm).putLong(configurationIndex).putInt(configuration.length).put(configuration).putInt(body.length).put(body).array();}
        public byte[] digest(){return hash(encode());}
        public static Request decode(byte[] bytes){try{if(bytes.length<80 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid HOME closing request length");var b=ByteBuffer.wrap(bytes);var home=new UUID(b.getLong(),b.getLong());long epoch=b.getLong();byte[] schema=new byte[32];b.get(schema);long term=b.getLong(),index=b.getLong();int n=b.getInt();if(n<1 || n>65536 || n>b.remaining()-4)throw new IllegalArgumentException("Invalid HOME closing configuration length");byte[] cfg=new byte[n];b.get(cfg);int m=b.getInt();if(m!=b.remaining() || m>NativeSourceClosing.MAX_BYTES)throw new IllegalArgumentException("Invalid source closing length");byte[] source=new byte[m];b.get(source);return new Request(home,epoch,schema,term,index,cfg,NativeSourceClosing.decode(source));}catch(java.nio.BufferUnderflowException malformed){throw new IllegalArgumentException("Truncated HOME closing",malformed);}}
        @Override public boolean equals(Object other){return other instanceof Request r && Arrays.equals(encode(),r.encode());}
        @Override public int hashCode(){return Arrays.hashCode(digest());}
    }
    public NativeHomeSourceClosing{Objects.requireNonNull(request);Objects.requireNonNull(position);if(position.term()<1 || position.index()<1 || position.index()<=request.configurationIndex() || position.term()<request.configurationTerm())throw new IllegalArgumentException("HOME closing does not follow actual configuration");}
    public void requireCut(ApplyPosition cut){if(cut==null || cut.index()<position.index() || cut.term()<position.term() || cut.index()==position.index() && cut.term()!=position.term())throw new IllegalArgumentException("HOME closing exceeds actual HOME prefix");}
    public byte[] encode(){byte[] body=request.encode();return ByteBuffer.allocate(24+body.length).putInt(MAGIC).putInt(body.length).put(body).putLong(position.term()).putLong(position.index()).array();}
    public static NativeHomeSourceClosing decode(byte[] bytes){try{if(bytes.length<24 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid HOME closing length");var b=ByteBuffer.wrap(bytes);if(b.getInt()!=MAGIC || b.getInt()!=bytes.length-24)throw new IllegalArgumentException("Invalid HOME closing record");byte[] body=new byte[bytes.length-24];b.get(body);return new NativeHomeSourceClosing(Request.decode(body),new ApplyPosition(b.getLong(),b.getLong()));}catch(java.nio.BufferUnderflowException malformed){throw new IllegalArgumentException("Truncated HOME closing",malformed);}}
    private static byte[] hash(byte[] bytes){try{return MessageDigest.getInstance("SHA-256").digest(bytes);}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
}
