package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/**
 * Cached-schema logical point operations for one domain. Borrows the routed client and owns one
 * newly issued session sequence window. Prepared identities survive uncertain replies in this
 * instance; use RatisDurableLogicalSession for recovery across restart.
 * Bucket operations, operators, scans and transactions require their own distributed protocols.
 */
public final class RatisLogicalClient {
    /** Ordered PUT/DELETE input. Logical keys and values are copied at construction. */
    public static final class Change {
        private final long column;
        private final Keys keys;
        private final byte[] value;
        private final Mutation.Kind kind;
        private final Mutation.ReturnMode returns;
        private Change(long column,Keys keys,Buf value,Mutation.Kind kind,Mutation.ReturnMode returns) {
            if (value!=null && value.size()>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Logical value exceeds command budget");
            this.column=column; this.keys=copy(keys); this.value=value==null ? null : value.toByteArray();
            this.kind=kind; this.returns=Objects.requireNonNull(returns);
        }
        public static Change put(long column,Keys keys,Buf value,Mutation.ReturnMode returns) {
            return new Change(column,keys,value,Mutation.Kind.PUT,returns);
        }
        public static Change delete(long column,Keys keys,Mutation.ReturnMode returns) {
            return new Change(column,keys,null,Mutation.Kind.DELETE,returns);
        }
    }
    /** Equality condition on the original image; null means absence, empty means present empty. */
    public static final class Condition {
        private final long column;
        private final Keys keys;
        private final byte[] expected;
        public Condition(long column,Keys keys,Buf expected) {
            if (expected!=null && expected.size()>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Logical predicate exceeds command budget");
            this.column=column; this.keys=copy(keys); this.expected=expected==null ? null : expected.toByteArray();
        }
    }
    public static final class Pending {
        private final RatisLogicalClient owner;
        private final RatisSessionClient.Pending pending;
        private Pending(RatisLogicalClient owner,RatisSessionClient.Pending pending) { this.owner=owner; this.pending=pending; }
        public OperationId operationId() { return pending.command().operationId(); }
        public DistributedCommand command() { return pending.command(); }
        public RangeDescriptor descriptor() { return pending.descriptor(); }
    }
    private final DistributedSchema schema;
    private final RatisRoutedClient routed;
    private final RatisSessionClient session;

    public RatisLogicalClient(DistributedSchema schema,RatisRoutedClient routed,SessionEpochLedger issuance,
                              SessionEpochLedger.Grant grant,RatisSessionClient.Limits limits) {
        this.schema=Objects.requireNonNull(schema); this.routed=Objects.requireNonNull(routed);
        schema.requireMatches(routed.directory());
        session=new RatisSessionClient(routed,issuance,grant,limits);
    }
    public DistributedSchema schema() { return schema; }
    public long getColumnId(String name) { return schema.column(name).apiId(); }
    public ColumnSchema getColumnSchema(long column) { return schema.column(column).schema(); }
    public Map<String,ColumnSchema> getAllColumnDefinitions() {
        var definitions=new LinkedHashMap<String,ColumnSchema>();
        for (var column:schema.columns()) definitions.put(column.name(),column.schema());
        return Collections.unmodifiableMap(definitions);
    }
    /** Linearizable point read with exactly the standalone fixed-key encoding. */
    public Buf get(long column,Keys keys) throws IOException {
        var binding=supported(schema,column); byte[] physical=schema.encodeFixedKey(binding.columnId(),keys);
        byte[] value=routed.read(binding.columnId(),physical);
        if (value!=null && !binding.schema().hasValue() && value.length!=0) throw new IOException("Nonempty value in key-only column");
        return value==null ? null : Buf.wrap(value);
    }
    public Pending preparePut(long column,Keys keys,Buf value,Mutation.ReturnMode returns) {
        return prepare(List.of(Change.put(column,keys,value,returns)),List.of());
    }
    public Pending prepareDelete(long column,Keys keys,Mutation.ReturnMode returns) {
        return prepare(List.of(Change.delete(column,keys,returns)),List.of());
    }
    /** One atomic same-range plan. Cross-range inputs are rejected before allocation or any RPC. */
    public Pending prepare(List<Change> changes,List<Condition> conditions) {
        var plan=plan(schema,changes,conditions); return new Pending(this,session.prepare(plan.mutations(),plan.predicates()));
    }
    record Plan(List<Mutation> mutations,List<KeyPredicate> predicates) {}
    static Plan plan(DistributedSchema schema,List<Change> changes,List<Condition> conditions) {
        changes=List.copyOf(changes); conditions=List.copyOf(conditions);
        if (changes.isEmpty() || (long)changes.size()+conditions.size()>DistributedCommandCodec.MAX_ITEMS) {
            throw new IllegalArgumentException("Invalid logical atomic command item count");
        }
        var mutations=new ArrayList<Mutation>(changes.size()); var predicates=new ArrayList<KeyPredicate>(conditions.size());
        long bytes=0;
        for (var change:changes) {
            var column=supported(schema,change.column); var key=schema.encodeFixedKey(column.columnId(),change.keys);
            byte[] value=change.kind==Mutation.Kind.DELETE ? null : storedValue(column,change.value);
            bytes+=26L+key.length+(value==null ? 0 : value.length); requireBudget(bytes);
            mutations.add(new Mutation(change.kind,column.columnId(),key,value,change.returns));
        }
        for (var condition:conditions) {
            var column=supported(schema,condition.column); var key=schema.encodeFixedKey(column.columnId(),condition.keys);
            // Null is an absence predicate even for value-bearing columns.
            byte[] expected=condition.expected==null ? null : storedValue(column,condition.expected);
            bytes+=25L+key.length+(expected==null ? 0 : expected.length); requireBudget(bytes);
            predicates.add(new KeyPredicate(column.columnId(),key,expected));
        }
        return new Plan(List.copyOf(mutations),List.copyOf(predicates));
    }
    /** Frozen retry; IOException retains the same identity, route, payload and sequence window. */
    public CommandResult execute(Pending pending) throws IOException { return session.execute(owned(pending)); }
    /** Application calls this only after durably consuming the saved terminal outcome. */
    public void acknowledge(Pending pending) { session.acknowledge(owned(pending)); }
    public int outstanding() { return session.outstanding(); }
    public long acknowledgedThrough() { return session.acknowledgedThrough(); }
    public Pending prepareAcknowledgement(long column,Keys routingKey) {
        var binding=supported(schema,column);
        return new Pending(this,session.prepareAcknowledgement(binding.columnId(),schema.encodeFixedKey(binding.columnId(),routingKey)));
    }
    /** Separate maintenance owner must retain/retry the returned binding identity before data submission. */
    public RatisRoutedClient.PreparedCommand prepareBinding(RatisSessionEpochs epochs,OperationId issuer,Pending pending,long acknowledgedThrough) throws IOException {
        return session.prepareBinding(epochs,issuer,owned(pending),acknowledgedThrough);
    }
    private RatisSessionClient.Pending owned(Pending pending) {
        if (pending==null || pending.owner!=this) throw new IllegalArgumentException("Operation belongs to another logical client");
        return pending.pending;
    }
    static DistributedSchema.Column supported(DistributedSchema schema,long column) {
        var binding=schema.column(column);
        if (binding.schema().variableLengthKeysCount()!=0) throw new UnsupportedOperationException("Logical bucket operations require the distributed bucket protocol");
        if (binding.operator()!=null) throw new UnsupportedOperationException("Logical operator operations require verified distributed operator activation");
        return binding;
    }
    private static byte[] storedValue(DistributedSchema.Column column,byte[] value) {
        if (column.schema().hasValue()) {
            if (value==null) throw new IllegalArgumentException("Value-bearing column requires a value");
            return value;
        }
        if (value!=null && value.length!=0) throw new IllegalArgumentException("Key-only column requires an empty value");
        return new byte[0];
    }
    private static void requireBudget(long bytes) {
        if (bytes>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Logical command exceeds byte budget");
    }
    private static Keys copy(Keys keys) {
        Objects.requireNonNull(keys); var parts=keys.keys();
        if (parts.length>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Too many logical key components");
        long bytes=0; for (var part:parts) { bytes+=Objects.requireNonNull(part).size(); requireBudget(bytes); }
        var copy=new Buf[parts.length];
        for (int i=0;i<parts.length;i++) copy[i]=Buf.wrap(Objects.requireNonNull(parts[i]).toByteArray());
        return new Keys(copy);
    }
}
