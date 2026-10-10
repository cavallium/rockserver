package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeTopologyFreeze;
import java.nio.ByteBuffer;
import java.util.*;

/** Bounded exact-record control replies. Gate readiness is distinct from a committed native artifact. */
public final class RatisTopologyFreezeRpc {
    public static final int PREPARE=ScopedTransactionRpc.TOPOLOGY_FREEZE_PREPARE,CANCEL=ScopedTransactionRpc.TOPOLOGY_FREEZE_CANCEL,STATE=ScopedTransactionRpc.TOPOLOGY_FREEZE_STATE;
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+RatisTopologyFreezeRecord.MAX_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+8+RatisTopologyFreezeRecord.MAX_BYTES+NativeTopologyFreeze.MAX_BYTES+25;
    private RatisTopologyFreezeRpc() {}
    public record State(RatisTopologyFreezeRecord record,NativeTopologyFreeze freeze,long snapshotPins,long preparedParticipants,long activeSnapshotGates,boolean ready) {
        public State {
            Objects.requireNonNull(record); if(snapshotPins<0 || preparedParticipants<0 || activeSnapshotGates<0 || ready && (snapshotPins!=0 || preparedParticipants!=0 || activeSnapshotGates!=0)) throw new IllegalArgumentException("Topology gate readiness contradicts drain counts");
            if(freeze!=null) record.requireFreeze(freeze);
        }
    }
    public static byte[] prepareRequest(RatisTopologyFreezeRecord record) { return request(PREPARE,record); }
    public static byte[] cancelRequest(RatisTopologyFreezeRecord record) { return request(CANCEL,record); }
    public static byte[] stateRequest(RatisTopologyFreezeRecord record) { return request(STATE,record); }
    private static byte[] request(int kind,RatisTopologyFreezeRecord record) { return frame(kind,record.encode(),MAX_REQUEST_BYTES); }
    public static RatisTopologyFreezeRecord request(byte[] bytes) { var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES); requireKind(packet.kind()); return RatisTopologyFreezeRecord.decode(packet.body()); }
    public static byte[] cancelledReply(RatisTopologyFreezeRecord record) { return request(CANCEL,record); }
    public static RatisTopologyFreezeRecord decodeCancelledReply(byte[] bytes,RatisTopologyFreezeRecord expected) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES); if(packet.kind()!=CANCEL) throw new IllegalArgumentException("Crossed topology cancel reply"); var record=RatisTopologyFreezeRecord.decode(packet.body()); requireExact(record,expected); return record;
    }
    public static byte[] reply(int kind,State state) {
        requireStateKind(kind); byte[] record=state.record().encode(),freeze=state.freeze()==null ? null : state.freeze().encode(); var out=ByteBuffer.allocate(33+record.length+(freeze==null ? 0 : freeze.length));
        ScopedTransactionRpc.putBlob(out,record); ScopedTransactionRpc.putBlob(out,freeze); out.putLong(state.snapshotPins()).putLong(state.preparedParticipants()).putLong(state.activeSnapshotGates()).put((byte)(state.ready() ? 1 : 0)); return frame(kind,out.array(),MAX_REPLY_BYTES);
    }
    public static State stateReply(byte[] bytes,int expectedKind,RatisTopologyFreezeRecord expected) {
        requireStateKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES); if(packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed topology control reply");
        var in=ByteBuffer.wrap(packet.body()); var record=RatisTopologyFreezeRecord.decode(ScopedTransactionRpc.blob(in,RatisTopologyFreezeRecord.MAX_BYTES,false)); requireExact(record,expected);
        byte[] freeze=ScopedTransactionRpc.blob(in,NativeTopologyFreeze.MAX_BYTES,true); if(in.remaining()!=25) throw new IllegalArgumentException("Invalid topology state tail");
        long pins=in.getLong(),participants=in.getLong(),gates=in.getLong(); int ready=in.get()&255; if(ready>1) throw new IllegalArgumentException("Invalid topology readiness flag");
        return new State(record,freeze==null ? null : NativeTopologyFreeze.decode(freeze),pins,participants,gates,ready==1);
    }
    private static void requireExact(RatisTopologyFreezeRecord record,RatisTopologyFreezeRecord expected) { if(!record.equals(expected)) throw new IllegalArgumentException("Topology reply changed exact original record"); }
    private static void requireKind(int kind) { if(!ScopedTransactionRpc.topologyFreeze(kind)) throw new IllegalArgumentException("Expected topology FREEZE control"); }
    private static void requireStateKind(int kind) { if(kind!=PREPARE && kind!=STATE) throw new IllegalArgumentException("Expected topology prepare/state reply"); }
    private static byte[] frame(int kind,byte[] body,int maximum) { if((long)ScopedTransactionRpc.HEADER_BYTES+body.length>maximum) throw new IllegalArgumentException("Topology FREEZE frame exceeds bound"); return ScopedTransactionRpc.frame(kind,body); }
}
