package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Committed one-source admission boundary. This is neither a terminal tombstone nor deletion authority. */
public record NativeSourceClosing(Request request,ApplyPosition position) {
    public static final int MAX_BYTES=24+Request.MAX_BYTES;
    public NativeSourceClosing {Objects.requireNonNull(request);Objects.requireNonNull(position);if(position.term()<1 || position.index()<1 || request.configurationIndex()>position.index() || request.configurationTerm()>position.term() || request.configurationIndex()==position.index() && request.configurationTerm()!=position.term())throw new IllegalArgumentException("Source closing configuration exceeds its own cut");}
    public record Request(TopologyChange.Header header,UUID group,UUID catalogue,ApplyPosition catalogueCut,long configurationTerm,long configurationIndex,byte[] configuration,byte[] recoveryObligationsDigest) {
        public static final int MAX_BYTES=108+TopologyChange.MAX_HEADER_BYTES+65536;
        public Request {Objects.requireNonNull(header);Objects.requireNonNull(group);Objects.requireNonNull(catalogue);Objects.requireNonNull(catalogueCut);header.participant(TopologyChange.Stage.FREEZE,group);
            if(header.kind()==TopologyChange.Kind.RELOCATE || catalogueCut.term()<1 || configurationTerm<0 || configurationIndex< -1 || (configurationIndex<0)!=(configurationTerm==0) || configuration==null || configuration.length<1 || configuration.length>65536 || recoveryObligationsDigest==null || recoveryObligationsDigest.length!=32)throw new IllegalArgumentException("Invalid source closing authority");configuration=configuration.clone();recoveryObligationsDigest=recoveryObligationsDigest.clone();}
        @Override public byte[] configuration(){return configuration.clone();}
        @Override public byte[] recoveryObligationsDigest(){return recoveryObligationsDigest.clone();}
        public byte[] encode(){byte[] h=header.encode();var out=ByteBuffer.allocate(108+h.length+configuration.length).putInt(0x53434c52).putInt(h.length).put(h);uuid(out,group);uuid(out,catalogue);out.putLong(catalogueCut.term()).putLong(catalogueCut.index()).putLong(configurationTerm).putLong(configurationIndex).putInt(configuration.length).put(configuration).put(recoveryObligationsDigest);return out.array();}
        public static Request decode(byte[] bytes){if(bytes==null || bytes.length<109 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid source closing request length");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x53434c52)throw new IllegalArgumentException("Invalid source closing tag");int n=in.getInt();if(n<1 || n>TopologyChange.MAX_HEADER_BYTES || n>in.remaining()-100)throw new IllegalArgumentException("Invalid source closing header");byte[] h=new byte[n];in.get(h);var group=uuid(in);var catalogue=uuid(in);var cut=new ApplyPosition(in.getLong(),in.getLong());long term=in.getLong(),index=in.getLong();int length=in.getInt();if(length<1 || length>65536 || length!=in.remaining()-32)throw new IllegalArgumentException("Invalid source closing configuration");byte[] cfg=new byte[length],digest=new byte[32];in.get(cfg).get(digest);var value=new Request(TopologyChange.Header.decode(h),group,catalogue,cut,term,index,cfg,digest);if(!Arrays.equals(bytes,value.encode()))throw new IllegalArgumentException("Noncanonical source closing request");return value;}
        @Override public boolean equals(Object other){return other instanceof Request r && Arrays.equals(encode(),r.encode());}
        @Override public int hashCode(){return Arrays.hashCode(encode());}
    }
    public void requireFreeze(NativeTopologyFreeze freeze){if(!request.group().equals(freeze.groupId()) || !Arrays.equals(request.header().encode(),freeze.header().encode()) || position.index()<=freeze.position().index() || position.term()<freeze.position().term())throw new IllegalArgumentException("Source closing lost original FREEZE/cut");}
    public void requireCut(ApplyPosition cut){if(cut==null || cut.term()<position.term() || cut.index()<position.index() || cut.index()==position.index() && cut.term()!=position.term())throw new IllegalArgumentException("Source closing exceeds completed prefix");}
    public byte[] encode(){byte[] body=request.encode();return ByteBuffer.allocate(24+body.length).putInt(0x53434c53).putLong(position.term()).putLong(position.index()).putInt(body.length).put(body).array();}
    public static NativeSourceClosing decode(byte[] bytes){if(bytes==null || bytes.length<24 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid native source closing length");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x53434c53)throw new IllegalArgumentException("Invalid native source closing tag");var position=new ApplyPosition(in.getLong(),in.getLong());int size=in.getInt();if(size!=in.remaining())throw new IllegalArgumentException("Invalid native source closing body");byte[] body=new byte[size];in.get(body);return new NativeSourceClosing(Request.decode(body),position);}
    private static void uuid(ByteBuffer b,UUID id){b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());}
    private static UUID uuid(ByteBuffer b){return new UUID(b.getLong(),b.getLong());}
    @Override public boolean equals(Object other){return other instanceof NativeSourceClosing c && request.equals(c.request) && position.equals(c.position);}
    @Override public int hashCode(){return Objects.hash(request,position);}
}
