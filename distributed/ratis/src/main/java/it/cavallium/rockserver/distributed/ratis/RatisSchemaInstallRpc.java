package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeSchemaInstallProof;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Bounded installation queries; preparation is not a native installation proof. */
public final class RatisSchemaInstallRpc {
    public static final int MAX_BYTES=2*1024*1024;
    private RatisSchemaInstallRpc() {}
    public record State(RatisSchemaInstallRecord record,NativeSchemaInstallProof installation,boolean ready) {
        public State {
            Objects.requireNonNull(record);
            if (ready && installation==null) throw new IllegalArgumentException("Ready installation lacks native proof");
            if (installation!=null) {
                installation.requireHeader(record.header());
                if (!installation.fence().participant().groupId().equals(record.groupId())
                        || !installation.fence().position().equals(record.expectedFencePosition()))
                    throw new IllegalArgumentException("Installation proof differs from original fence");
            }
        }
    }
    public static byte[] prepareRequest(RatisSchemaInstallRecord record) { return request(ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE,record); }
    public static byte[] cancelRequest(RatisSchemaInstallRecord record) { return request(ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL,record); }
    public static byte[] stateRequest(RatisSchemaInstallRecord record) { return request(ScopedTransactionRpc.SCHEMA_INSTALL_STATE,record); }
    private static byte[] request(int kind,RatisSchemaInstallRecord record) { return boundedFrame(kind,record.encode()); }
    public static RatisSchemaInstallRecord request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (!ScopedTransactionRpc.schemaInstall(packet.kind())) throw new IllegalArgumentException("Expected installation query");
        return RatisSchemaInstallRecord.decode(packet.body());
    }
    public static byte[] preparedReply(int kind,RatisSchemaInstallRecord record) {
        requirePreparedKind(kind); return boundedFrame(kind,record.encode());
    }
    public static RatisSchemaInstallRecord decodePreparedReply(byte[] bytes,int expectedKind,RatisSchemaInstallRecord expected) {
        requirePreparedKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed installation preparation reply");
        var record=RatisSchemaInstallRecord.decode(packet.body());
        if (!record.equals(expected)) throw new IllegalArgumentException("Installation preparation identity changed"); return record;
    }
    public static byte[] reply(int kind,State state) {
        requireStateKind(kind); byte[] record=state.record().encode(),proof=state.installation()==null ? null : state.installation().encode();
        var out=ByteBuffer.allocate(9+record.length+(proof==null ? 0 : proof.length));
        ScopedTransactionRpc.putBlob(out,record); ScopedTransactionRpc.putBlob(out,proof); out.put((byte)(state.ready() ? 1 : 0));
        return boundedFrame(kind,out.array());
    }
    public static State stateReply(byte[] bytes,int expectedKind,RatisSchemaInstallRecord expected) {
        requireStateKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed installation state reply");
        var in=ByteBuffer.wrap(packet.body()); var record=RatisSchemaInstallRecord.decode(ScopedTransactionRpc.blob(in,RatisSchemaInstallRecord.MAX_BYTES,false));
        if (!record.equals(expected)) throw new IllegalArgumentException("Installation state identity changed");
        byte[] proof=ScopedTransactionRpc.blob(in,NativeSchemaInstallProof.BYTES,true);
        if (in.remaining()!=1) throw new IllegalArgumentException("Invalid installation state tail"); int ready=in.get() & 255;
        if (ready>1) throw new IllegalArgumentException("Invalid installation readiness flag");
        return new State(record,proof==null ? null : NativeSchemaInstallProof.decode(proof),ready==1);
    }
    private static byte[] boundedFrame(int kind,byte[] body) {
        if ((long)ScopedTransactionRpc.HEADER_BYTES+body.length>MAX_BYTES) throw new IllegalArgumentException("Installation query exceeds bound");
        return ScopedTransactionRpc.frame(kind,body);
    }
    private static void requirePreparedKind(int kind) { if (kind!=ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE && kind!=ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL) throw new IllegalArgumentException("Expected installation preparation query"); }
    private static void requireStateKind(int kind) { if (kind!=ScopedTransactionRpc.SCHEMA_INSTALL_STATE) throw new IllegalArgumentException("Expected installation state query"); }
}
