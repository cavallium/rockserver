package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeTopologyActivation;
import java.nio.ByteBuffer;
import java.util.*;

/** Exact common activation request, local durable receipt, and volatile prepared readiness. */
public final class RatisTopologyActivationRpc {
    public static final int PREPARE=89,CANCEL=90,STATE=91;
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+RatisTopologyActivationRecord.MAX_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+9+RatisTopologyActivationRecord.MAX_BYTES+NativeTopologyActivation.MAX_BYTES;
    private RatisTopologyActivationRpc() {}
    public record State(RatisTopologyActivationRecord record,NativeTopologyActivation activation,boolean ready) {
        public State { Objects.requireNonNull(record); if(activation!=null) requireActivation(record,activation); }
    }
    public static void requireActivation(RatisTopologyActivationRecord record,NativeTopologyActivation activation) {
        activation.requireRequest(record.definition(),record.freezeDigest(),record.catalogueGroup(),record.catalogueCut(),record.publicationCommitment());
        if(!activation.birth().groupId().equals(record.groupId())) throw new IllegalArgumentException("Topology activation reply crossed child group");
    }
    public static byte[] prepareRequest(RatisTopologyActivationRecord record) { return request(PREPARE,record); }
    public static byte[] cancelRequest(RatisTopologyActivationRecord record) { return request(CANCEL,record); }
    public static byte[] stateRequest(RatisTopologyActivationRecord record) { return request(STATE,record); }
    private static byte[] request(int kind,RatisTopologyActivationRecord record) { return ScopedTransactionRpc.frame(kind,record.encode()); }
    public static RatisTopologyActivationRecord request(byte[] bytes) { var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES); if(!ScopedTransactionRpc.topologyActivate(packet.kind()))throw new IllegalArgumentException("Expected topology activation control"); return RatisTopologyActivationRecord.decode(packet.body()); }
    public static byte[] cancelledReply(RatisTopologyActivationRecord record) { return cancelRequest(record); }
    public static void decodeCancelledReply(byte[] bytes,RatisTopologyActivationRecord expected) { var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES); if(packet.kind()!=CANCEL || !request(bytes).equals(expected))throw new IllegalArgumentException("Crossed topology activation cancellation"); }
    public static byte[] reply(int kind,State state) { requireStateKind(kind); byte[] record=state.record().encode(),receipt=state.activation()==null ? null : state.activation().encode(); var out=ByteBuffer.allocate(9+record.length+(receipt==null ? 0 : receipt.length)); ScopedTransactionRpc.putBlob(out,record); ScopedTransactionRpc.putBlob(out,receipt); out.put((byte)(state.ready()?1:0)); return ScopedTransactionRpc.frame(kind,out.array()); }
    public static State stateReply(byte[] bytes,int kind,RatisTopologyActivationRecord expected) { requireStateKind(kind); var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES); if(packet.kind()!=kind)throw new IllegalArgumentException("Crossed topology activation state"); var in=ByteBuffer.wrap(packet.body()); var record=RatisTopologyActivationRecord.decode(ScopedTransactionRpc.blob(in,RatisTopologyActivationRecord.MAX_BYTES,false)); if(!record.equals(expected))throw new IllegalArgumentException("Topology activation record changed"); byte[] receipt=ScopedTransactionRpc.blob(in,NativeTopologyActivation.MAX_BYTES,true); if(in.remaining()!=1)throw new IllegalArgumentException("Invalid topology activation tail"); int ready=Byte.toUnsignedInt(in.get()); if(ready>1)throw new IllegalArgumentException("Invalid topology activation readiness"); return new State(record,receipt==null ? null : NativeTopologyActivation.decode(receipt),ready==1); }
    private static void requireStateKind(int kind) { if(kind!=PREPARE && kind!=STATE)throw new IllegalArgumentException("Expected topology activation state kind"); }
}
