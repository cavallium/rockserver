package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Replicated catalogue history accounting. Apply only after successful predicates, in the effects/result batch. */
public final class CatalogueHistory {
    public static final long MAX_BYTES=256L*1024*1024;
    public static final long MAX_ROWS=65536;
    public static final int STATE_BYTES=32,CREDIT_BYTES=48;
    private static final long CREDIT_RECORD_BYTES=33+CREDIT_BYTES;
    private CatalogueHistory() {}
    public static byte[] stateKey() { return new byte[]{0x78}; }
    public static byte[] creditKey(byte[] definition) { requireDigest(definition); return ByteBuffer.allocate(33).put((byte)0x79).put(definition).array(); }
    public record State(long actualBytes,long actualRows,long promisedBytes,long promisedRows) {
        public State { if(actualBytes<0 || actualRows<0 || promisedBytes<0 || promisedRows<0 || actualBytes>MAX_BYTES-promisedBytes || actualRows>MAX_ROWS-promisedRows) throw new IllegalArgumentException("Catalogue history capacity exhausted or corrupt"); }
        public static State empty() { return new State(1+STATE_BYTES,1,0,0); }
        public byte[] encode() { return ByteBuffer.allocate(STATE_BYTES).putLong(actualBytes).putLong(actualRows).putLong(promisedBytes).putLong(promisedRows).array(); }
        public static State decode(byte[] bytes) { if(bytes==null || bytes.length!=STATE_BYTES) throw new IllegalArgumentException("Invalid catalogue history state"); var in=ByteBuffer.wrap(bytes); return new State(in.getLong(),in.getLong(),in.getLong(),in.getLong()); }
    }
    public record Credit(byte[] definition,long bytes,long rows) {
        public Credit { requireDigest(definition); if(bytes<0 || rows<0 || bytes>MAX_BYTES || rows>MAX_ROWS) throw new IllegalArgumentException("Invalid catalogue history credit"); definition=definition.clone(); }
        @Override public byte[] definition() { return definition.clone(); }
        public byte[] encode() { return ByteBuffer.allocate(CREDIT_BYTES).put(definition).putLong(bytes).putLong(rows).array(); }
        public static Credit decode(byte[] bytes) { if(bytes==null || bytes.length!=CREDIT_BYTES) throw new IllegalArgumentException("Invalid catalogue history credit"); var in=ByteBuffer.wrap(bytes); byte[] id=new byte[32]; in.get(id); return new Credit(id,in.getLong(),in.getLong()); }
    }
    /** Exact signed key+value occupancy delta, computed from current values under native apply serialization. */
    public record Delta(long bytes,long rows) {
        public static Delta replacement(byte[] key,byte[] previous,byte[] next) { Objects.requireNonNull(key); return new Delta((next==null ? 0 : key.length+(long)next.length)-(previous==null ? 0 : key.length+(long)previous.length),(next==null ? 0 : 1)-(previous==null ? 0 : 1)); }
        public Delta plus(Delta other) { return new Delta(Math.addExact(bytes,other.bytes),Math.addExact(rows,other.rows)); }
    }
    public enum Action { UNRESERVED,BEGIN,SPEND,TERMINAL }
    /** Trusted validated-command policy, never a request-supplied reservation or admission boolean. */
    public record Directive(Action action,byte[] definition,long reserveBytes,long reserveRows) {
        public Directive { Objects.requireNonNull(action); if(action==Action.UNRESERVED ? definition!=null : definition==null) throw new IllegalArgumentException("Invalid catalogue credit identity"); if(definition!=null) { requireDigest(definition); definition=definition.clone(); } if(reserveBytes<0 || reserveRows<0 || action!=Action.BEGIN && (reserveBytes!=0 || reserveRows!=0)) throw new IllegalArgumentException("Invalid catalogue reservation"); }
        @Override public byte[] definition() { return definition==null ? null : definition.clone(); }
        public static Directive unreserved() { return new Directive(Action.UNRESERVED,null,0,0); }
        public static Directive begin(byte[] definition,long bytes,long rows) { return new Directive(Action.BEGIN,definition,bytes,rows); }
        public static Directive spend(byte[] definition) { return new Directive(Action.SPEND,definition,0,0); }
        public static Directive terminal(byte[] definition) { return new Directive(Action.TERMINAL,definition,0,0); }
    }
    public record Update(State state,Credit credit) {}
    /** Duplicate results/failed predicates must skip this method altogether: they create no occupancy or credit. */
    public static Update apply(State state,Credit original,Directive directive,Delta delta) {
        Objects.requireNonNull(state); Objects.requireNonNull(directive); Objects.requireNonNull(delta);
        long bytes=Math.addExact(state.actualBytes(),delta.bytes()),rows=Math.addExact(state.actualRows(),delta.rows());
        long promisedBytes=state.promisedBytes(),promisedRows=state.promisedRows(); Credit next=original;
        if(directive.action()==Action.BEGIN) {
            if(original!=null) throw new IllegalArgumentException("Catalogue BEGIN already owns credit");
            next=new Credit(directive.definition(),directive.reserveBytes(),directive.reserveRows());
            bytes=Math.addExact(bytes,CREDIT_RECORD_BYTES); rows=Math.addExact(rows,1);
            promisedBytes=Math.addExact(promisedBytes,next.bytes()); promisedRows=Math.addExact(promisedRows,next.rows());
        } else if(directive.action()!=Action.UNRESERVED) {
            if(original==null || !Arrays.equals(original.definition(),directive.definition())) throw new IllegalArgumentException("Catalogue stage lacks original BEGIN credit");
            long spendBytes=Math.max(0,delta.bytes()),spendRows=Math.max(0,delta.rows());
            if(spendBytes>original.bytes() || spendRows>original.rows()) throw new IllegalArgumentException("Catalogue stage exceeds original completion reservation");
            long remainingBytes=original.bytes()-spendBytes,remainingRows=original.rows()-spendRows;
            promisedBytes-=spendBytes; promisedRows-=spendRows;
            if(directive.action()==Action.TERMINAL) { promisedBytes-=remainingBytes; promisedRows-=remainingRows; bytes-=CREDIT_RECORD_BYTES; rows--; next=null; }
            else next=new Credit(original.definition(),remainingBytes,remainingRows);
        } else if(original!=null) throw new IllegalArgumentException("Unreserved history must not consume active credit");
        return new Update(new State(bytes,rows,promisedBytes,promisedRows),next);
    }
    private static void requireDigest(byte[] digest) { if(digest==null || digest.length!=32) throw new IllegalArgumentException("Catalogue credit requires exact immutable definition digest"); }
}
