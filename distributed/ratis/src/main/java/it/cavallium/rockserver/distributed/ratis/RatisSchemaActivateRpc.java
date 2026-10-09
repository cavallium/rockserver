package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Activation preparation returns the exact append record qualified at a catalogue publication cut. */
public final class RatisSchemaActivateRpc {
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+8+2*RatisSchemaActivateRecord.MAX_BYTES+NativeSchemaActivationProof.BYTES+1;
    private RatisSchemaActivateRpc() {}
    public record Request(SchemaChange.Header header,UUID groupId,ApplyPosition expectedInstallPosition) {
        public Request {
            Objects.requireNonNull(header); Objects.requireNonNull(groupId); Objects.requireNonNull(expectedInstallPosition);
            if (expectedInstallPosition.term()==0 || header.participants().stream().noneMatch(p -> p.groupId().equals(groupId))) throw new IllegalArgumentException("Invalid activation request");
            if (36L+header.encode().length>RatisSchemaActivateRecord.MAX_BYTES) throw new IllegalArgumentException("Activation request exceeds bound");
        }
        public byte[] encode() {
            byte[] bytes=header.encode(); var out=ByteBuffer.allocate(36+bytes.length);
            ScopedTransactionRpc.uuid(out,groupId); out.putLong(expectedInstallPosition.term()).putLong(expectedInstallPosition.index()); ScopedTransactionRpc.putBlob(out,bytes); return out.array();
        }
        public static Request decode(byte[] bytes) {
            if (bytes==null || bytes.length<36 || bytes.length>RatisSchemaActivateRecord.MAX_BYTES) throw new IllegalArgumentException("Invalid activation request size");
            var in=ByteBuffer.wrap(bytes); UUID group=ScopedTransactionRpc.uuid(in); var install=new ApplyPosition(in.getLong(),in.getLong());
            var header=SchemaChange.Header.decode(ScopedTransactionRpc.blob(in,RatisSchemaActivateRecord.MAX_BYTES-36,false));
            if (in.hasRemaining()) throw new IllegalArgumentException("Trailing activation request"); return new Request(header,group,install);
        }
        public void requireRecord(RatisSchemaActivateRecord record) {
            if (!groupId.equals(record.groupId()) || !expectedInstallPosition.equals(record.expectedInstallPosition()) || !Arrays.equals(header.encode(),record.header().encode())) throw new IllegalArgumentException("Activation record differs from original request");
        }
        @Override public boolean equals(Object other) { return other instanceof Request r && Arrays.equals(encode(),r.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
    }
    public record State(Request request,NativeSchemaActivationProof activation,boolean ready) {
        public State {
            Objects.requireNonNull(request);
            if (ready && activation==null) throw new IllegalArgumentException("Ready activation lacks native proof");
            if (activation!=null) {
                activation.requireHeader(request.header());
                if (!activation.installation().fence().participant().groupId().equals(request.groupId())
                        || !activation.installation().position().equals(request.expectedInstallPosition())) throw new IllegalArgumentException("Activation proof differs from original installation");
            }
        }
    }
    public static byte[] prepareRequest(Request request) { return query(ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request); }
    public static byte[] cancelRequest(Request request) { return query(ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL,request); }
    public static byte[] stateRequest(Request request) { return query(ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE,request); }
    private static byte[] query(int kind,Request request) { return ScopedTransactionRpc.frame(kind,request.encode()); }
    public static Request request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (!ScopedTransactionRpc.schemaActivate(packet.kind())) throw new IllegalArgumentException("Expected activation query"); return Request.decode(packet.body());
    }
    public static byte[] preparedReply(int kind,Request request,RatisSchemaActivateRecord record) {
        requirePreparedKind(kind);
        if (kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE && record==null || kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL && record!=null) throw new IllegalArgumentException("Invalid activation preparation record");
        if (record!=null) request.requireRecord(record);
        byte[] input=request.encode(),frozen=record==null ? null : record.encode(); var out=ByteBuffer.allocate(8+input.length+(frozen==null ? 0 : frozen.length));
        ScopedTransactionRpc.putBlob(out,input); ScopedTransactionRpc.putBlob(out,frozen); return ScopedTransactionRpc.frame(kind,out.array());
    }
    public static RatisSchemaActivateRecord decodePreparedReply(byte[] bytes,int expectedKind,Request expected) {
        requirePreparedKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed activation preparation reply");
        var in=ByteBuffer.wrap(packet.body()); var request=Request.decode(ScopedTransactionRpc.blob(in,RatisSchemaActivateRecord.MAX_BYTES,false));
        if (!request.equals(expected)) throw new IllegalArgumentException("Activation preparation identity changed");
        byte[] frozen=ScopedTransactionRpc.blob(in,RatisSchemaActivateRecord.MAX_BYTES,true);
        if (in.hasRemaining() || expectedKind==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE && frozen==null || expectedKind==ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL && frozen!=null) throw new IllegalArgumentException("Invalid activation preparation reply");
        var record=frozen==null ? null : RatisSchemaActivateRecord.decode(frozen); if (record!=null) expected.requireRecord(record); return record;
    }
    public static byte[] reply(int kind,State state) {
        requireStateKind(kind); byte[] input=state.request().encode(),proof=state.activation()==null ? null : state.activation().encode();
        var out=ByteBuffer.allocate(9+input.length+(proof==null ? 0 : proof.length)); ScopedTransactionRpc.putBlob(out,input); ScopedTransactionRpc.putBlob(out,proof); out.put((byte)(state.ready() ? 1 : 0));
        return ScopedTransactionRpc.frame(kind,out.array());
    }
    public static State stateReply(byte[] bytes,int expectedKind,Request expected) {
        requireStateKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed activation state reply");
        var in=ByteBuffer.wrap(packet.body()); var request=Request.decode(ScopedTransactionRpc.blob(in,RatisSchemaActivateRecord.MAX_BYTES,false));
        if (!request.equals(expected)) throw new IllegalArgumentException("Activation state identity changed");
        byte[] proof=ScopedTransactionRpc.blob(in,NativeSchemaActivationProof.BYTES,true);
        if (in.remaining()!=1) throw new IllegalArgumentException("Invalid activation state tail"); int ready=in.get() & 255;
        if (ready>1) throw new IllegalArgumentException("Invalid activation readiness flag");
        return new State(request,proof==null ? null : NativeSchemaActivationProof.decode(proof),ready==1);
    }
    private static void requirePreparedKind(int kind) { if (kind!=ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE && kind!=ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL) throw new IllegalArgumentException("Expected activation preparation query"); }
    private static void requireStateKind(int kind) { if (kind!=ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE) throw new IllegalArgumentException("Expected activation state query"); }
}
