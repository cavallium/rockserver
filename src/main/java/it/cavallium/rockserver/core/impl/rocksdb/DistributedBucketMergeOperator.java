package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.BucketMergeOperand;
import it.cavallium.rockserver.core.common.distributed.DistributedBucket;
import it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator;
import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import java.util.List;
import java.util.Objects;

/** Native ordered member operands over the standalone physical collision-bucket value format. */
public final class DistributedBucketMergeOperator extends FFMByteArrayMergeOperator {
    public static final int VALUE_FORMAT_ID=0x424b5654;
    private final ColumnSchema shape;
    private final FFMAbstractMergeOperator delegate;
    public DistributedBucketMergeOperator(ColumnSchema shape,FFMAbstractMergeOperator ownedDelegate) {
        super(identity(shape,ownedDelegate)); this.shape=new ColumnSchema(new it.unimi.dsi.fastutil.ints.IntArrayList(shape.keys()),new it.unimi.dsi.fastutil.objects.ObjectArrayList<>(shape.variableTailKeys()),shape.hasValue(),null,null,null); this.delegate=ownedDelegate;
        try { retainOwnedDependency(ownedDelegate); } catch(RuntimeException|Error failure) { close(); throw failure; }
    }
    private static String identity(ColumnSchema shape,FFMAbstractMergeOperator delegate) {
        Objects.requireNonNull(shape); Objects.requireNonNull(delegate);
        if(shape.variableLengthKeysCount()==0 || !shape.hasValue() || !delegate.isOwningHandle()) throw new IllegalArgumentException("Bucket callback requires a value-bearing bucket and owning delegate");
        DistributedBucket.empty(shape);
        return "DistributedBucket-"+Integer.toHexString(VALUE_FORMAT_ID)+"-"+Integer.toHexString(BucketMergeOperand.FORMAT_ID);
    }
    @Override public byte[] merge(byte[] physicalKey,byte[] existing,List<byte[]> operands) {
        if(!delegate.isOwningHandle()) throw new IllegalStateException("Bucket callback delegate has released native ownership");
        long expectedKey=0; for(int length:shape.keys()) expectedKey+=length; if(physicalKey.length!=expectedKey) throw new IllegalArgumentException("Bucket callback physical key differs from schema");
        var bucket=DistributedBucket.decode(shape,existing);
        for(var encoded:operands) {
            var operand=BucketMergeOperand.decode(encoded); var members=operand.memberKeys();
            switch(operand.type()) {
                case PUT -> bucket=bucket.put(members,operand.value());
                case DELETE -> bucket=bucket.remove(members);
                case MERGE -> {
                    byte[] prior=bucket.get(members),value=operand.value();
                    if(prior!=null) { var merged=delegate.merge(Buf.wrap(physicalKey),Buf.wrap(prior),List.of(Buf.wrap(value))); value=merged==null ? prior : merged.asArray(); }
                    bucket=bucket.put(members,value);
                }
            }
        }
        return bucket.encode();
    }
    /** Operand envelopes retain ordering; refusing partial merge leaves native full merge to resolve it. */
    @Override public byte[] partialMergeMulti(byte[] physicalKey,List<byte[]> operands) { return null; }
}
