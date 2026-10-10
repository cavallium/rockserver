package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Original file result lookup at a fresh completed native cut. Empty means no committed original. */
public final class RatisRangeImportFileResultRpc {
    public static final int KIND=80,MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+149;
    private RatisRangeImportFileResultRpc() {}
    public static byte[] request(RatisReferencedFile.Request request){return ScopedTransactionRpc.frame(KIND,request.encode());}
    public static RatisReferencedFile.Request decodeRequest(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.HEADER_BYTES+RatisReferencedFile.Request.BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong file result request");return RatisReferencedFile.Request.decode(packet.body());}
    public static byte[] reply(RatisReferencedFile.Request request,ApplyPosition cut,RatisReferencedFile reference){
        require(request,cut,reference);var out=ByteBuffer.allocate(49+(reference==null?0:RatisReferencedFile.BYTES)).put(RangeImportAdmission.sha256(request.encode())).putLong(cut.term()).putLong(cut.index()).put((byte)(reference==null?0:1));if(reference!=null)out.put(reference.encode());return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static RatisReferencedFile decodeReply(byte[] bytes,RatisReferencedFile.Request request){
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong file result reply");
        try{var in=ByteBuffer.wrap(packet.body());byte[] digest=new byte[32];in.get(digest);if(!Arrays.equals(digest,RangeImportAdmission.sha256(request.encode())))throw new IllegalArgumentException("File result crossed original request");var cut=new ApplyPosition(in.getLong(),in.getLong());int present=in.get()&255;if(present>1 || in.remaining()!=(present==0?0:100))throw new IllegalArgumentException("Invalid file result body");RatisReferencedFile reference=null;if(present!=0){byte[] value=new byte[100];in.get(value);reference=RatisReferencedFile.decode(value);}require(request,cut,reference);return reference;
        }catch(java.nio.BufferUnderflowException invalid){throw new IllegalArgumentException("Truncated file result reply",invalid);}
    }
    private static void require(RatisReferencedFile.Request request,ApplyPosition cut,RatisReferencedFile reference){if(cut==null || cut.term()<1)throw new IllegalArgumentException("File result lacks completed current-term cut");if(reference!=null && (!reference.request().equals(request) || reference.index()>cut.index() || reference.term()>cut.term() || reference.index()==cut.index() && reference.term()!=cut.term()))throw new IllegalArgumentException("File result differs from request/cut");}
}
