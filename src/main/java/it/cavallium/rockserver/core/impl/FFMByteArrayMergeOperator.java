package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import java.util.ArrayList;
import java.util.List;

/**
 * An adapter class that bridges the high-performance {@link FFMAbstractMergeOperator}
 * to the legacy RocksDB Merge Operator API using byte arrays.
 * <p>
 * Use this class if you have existing merge logic using {@code byte[]} and want to
 * run it on the FFM engine without rewriting the logic to use {@link Buf}.
 * <p>
 * <b>Note:</b> This introduces memory copy overhead (Native -> Heap) required to create
 * the byte arrays. For maximum performance, extend {@link FFMAbstractMergeOperator} directly.
 */
public abstract class FFMByteArrayMergeOperator extends FFMAbstractMergeOperator {

    public FFMByteArrayMergeOperator(String name) {
        super(name);
    }

    /**
     * Legacy merge method using standard Java byte arrays.
     *
     * @param key          The key (copied to heap).
     * @param existingValue The existing value (copied to heap), or null.
     * @param operands     The list of operands (copied to heap).
     * @return The merged value as a byte array, or null to indicate failure/no-op.
     */
    public abstract byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands);

    /**
     * Preferred partial merge hook operating on a list of operands (RocksDB's partialMergeMulti).
     * Override to enable associative merging optimizations during flush/compaction.
     */
    public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
        return null;
    }

    /**
     * Legacy pair-wise partial merge hook. Deprecated in RocksDB in favor of {@link #partialMergeMulti(byte[], List)}.
     */
    @Deprecated
    public byte[] partialMerge(byte[] key, byte[] leftOperand, byte[] rightOperand) {
        return null;
    }

    // --- Bridge Implementation ---

    @Override
    public final Buf merge(Buf key, Buf existingValue, List<Buf> operands) {
        // Copy native memory to Java Heap for legacy compatibility
        byte[] keyArr = key.asArray();
        byte[] existingArr = (existingValue != null) ? existingValue.asArray() : null;

        List<byte[]> operandsArr = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            operandsArr.add(operands.get(i).asArray());
        }

        // Invoke legacy user logic
        byte[] result = merge(keyArr, existingArr, operandsArr);

        // Wrap result back to Buf. 
        // Note: FFMAbstractMergeOperator will handle the copy from this Buf back to Native memory.
        return (result != null) ? Buf.wrap(result) : null;
    }

    @Override
    public final Buf partialMergeMulti(Buf key, List<Buf> operands) {
        byte[] keyArr = key.asArray();

        List<byte[]> operandsArr = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            operandsArr.add(operands.get(i).asArray());
        }

        byte[] result = partialMergeMulti(keyArr, operandsArr);
        if (result == null && operandsArr.size() == 2) {
            result = partialMerge(keyArr, operandsArr.getFirst(), operandsArr.getLast());
        }

        return (result != null) ? Buf.wrap(result) : null;
    }

    @Override
    public final Buf partialMerge(Buf key, Buf leftOperand, Buf rightOperand) {
        // Copy inputs to heap
        byte[] keyArr = key.asArray();
        byte[] leftArr = leftOperand.asArray();
        byte[] rightArr = rightOperand.asArray();

        // Invoke legacy user logic
        byte[] result = partialMerge(keyArr, leftArr, rightArr);

        return (result != null) ? Buf.wrap(result) : null;
    }

    @Override
    protected void onMergeError(Throwable t, boolean partial, Buf key) {
        // Preserve binary compatibility with older overrides
        onMergeError(t, partial);
    }

    /**
     * Backward-compatible hook for subclasses that previously overrode onMergeError(Throwable, boolean).
     */
    @SuppressWarnings("deprecation")
    protected void onMergeError(Throwable t, boolean partial) {
        super.onMergeError(t, partial, null);
    }
}