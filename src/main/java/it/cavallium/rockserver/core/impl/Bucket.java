package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static java.lang.Math.toIntExact;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.buffer.Buf;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.jetbrains.annotations.Nullable;

public class Bucket {

    private final ColumnInstance col;
    private final ArrayList<Entry<Buf[], Buf>> elements;

    public java.util.List<Entry<Buf[], Buf>> getElements() {
        return java.util.Collections.unmodifiableList(elements);
    }

    public Bucket(ColumnInstance col) {
        this(col, emptyBuf());
    }

    public static int readElementCount(Buf rawBucketSegment) {
        if (rawBucketSegment.size() < Integer.BYTES) {
            return 0;
        }
        return rawBucketSegment.getInt(0);
    }

    public Bucket(ColumnInstance col, Buf rawBucketSegment) {
        if (col == null) {
            throw new IllegalArgumentException("col must not be null");
        }
        if (rawBucketSegment == null) {
            throw new IllegalArgumentException("rawBucketSegment must not be null");
        }
        this.col = col;
        int offset = 0;
        this.elements = new ArrayList<>();
        int rawBucketSegmentByteSize = rawBucketSegment.size();
        if (rawBucketSegmentByteSize > 0) {
            requireBucketBytes(rawBucketSegmentByteSize, offset, Integer.BYTES, "element count");
            var elements = rawBucketSegment.getInt(offset);
            if (elements < 0) {
                throw malformedBucket("Bucket segment element count must be non-negative, got " + elements);
            }
            offset += Integer.BYTES;
            int elementI = 0;
            while (elementI < elements) {
                requireBucketBytes(rawBucketSegmentByteSize, offset, Integer.BYTES, "element size at index " + elementI);
                var elementKVSize = rawBucketSegment.getInt(offset);
                if (elementKVSize < 0) {
                    throw malformedBucket("Bucket element size must be non-negative at index "
                            + elementI + ", got " + elementKVSize);
                }
                offset += Integer.BYTES;
                requireBucketBytes(rawBucketSegmentByteSize, offset, elementKVSize, "element payload at index " + elementI);

                Buf[] bucketElementKeys;
                {
                    int segmentOffset = 0;
                    var elementEnd = offset + elementKVSize;
                    var elementKVSegment = rawBucketSegment.subList(offset, elementEnd);
                    int readKeys = 0;
                    bucketElementKeys = new Buf[col.schema().variableLengthKeysCount()];
                    while (readKeys < col.schema().variableLengthKeysCount()) {
                        requireBucketBytes(elementKVSize,
                                segmentOffset,
                                Character.BYTES,
                                "key length for element " + elementI + " variable key " + readKeys);
                        var keyISize = elementKVSegment.getChar(segmentOffset);
                        segmentOffset += Character.BYTES;
                        requireBucketBytes(elementKVSize,
                                segmentOffset,
                                keyISize,
                                "key content for element " + elementI + " variable key " + readKeys);
                        var elementKeyISegment = elementKVSegment.subList(segmentOffset, segmentOffset + keyISize);
                        bucketElementKeys[readKeys] = elementKeyISegment;
                        segmentOffset += keyISize;
                        readKeys++;
                    }

                    Buf bucketElementValues;
                    if (col.schema().hasValue()) {
                        bucketElementValues = elementKVSegment.subList(segmentOffset, elementKVSize);
                        segmentOffset = elementKVSize;
                    } else {
                        bucketElementValues = emptyBuf();
                        if (segmentOffset != elementKVSize) {
                            throw malformedBucket("Bucket element " + elementI
                                    + " has " + (elementKVSize - segmentOffset)
                                    + " trailing value bytes for value-less schema");
                        }
                    }

                    var entry = Map.entry(bucketElementKeys, bucketElementValues);
                    this.elements.add(entry);
                    offset = elementEnd;
                }
                elementI++;
            }
            if (offset != rawBucketSegmentByteSize) {
                throw malformedBucket("Bucket segment has " + (rawBucketSegmentByteSize - offset)
                        + " trailing bytes after " + elements + " elements");
            }
        }
    }

    /**
     * Add or replace an element
     *
     * @return Return the previous value ({@link Utils#emptyBuf()} if no value is expected),
     * return null if no element was present
     */
    @Nullable
    public Buf addElement(Buf[] bucketVariableKeys, @Nullable Buf value) {
        validateBucketVariableKeys(bucketVariableKeys);
        var element = Map.entry(bucketVariableKeys, value != null ? value : emptyBuf());
        var i = indexOf(bucketVariableKeys);
        if (i == -1) {
            this.elements.add(element);
            return null;
        } else {
            return this.elements.set(i, element).getValue();
        }
    }

    /**
     * Remove an element
     *
     * @return Return the previous value ({@link Utils#emptyBuf()} if no value is expected),
     * return null if no element was present
     */
    @Nullable
    public Buf removeElement(Buf[] bucketVariableKeys) {
        validateBucketVariableKeys(bucketVariableKeys);
        var i = indexOf(bucketVariableKeys);
        if (i == -1) {
            return null;
        } else {
            return this.elements.remove(i).getValue();
        }
    }

    /**
     * Get an element
     *
     * @return Return the value ({@link Utils#emptyBuf()} if no value is expected),
     * return null if no element was present
     */
    @Nullable
    public Buf getElement(Buf[] bucketVariableKeys) {
        validateBucketVariableKeys(bucketVariableKeys);
        var i = indexOf(bucketVariableKeys);
        if (i == -1) {
            return null;
        } else {
            return this.elements.get(i).getValue();
        }
    }

    private int indexOf(Buf[] bucketVariableKeys) {
        nextElement:
        for (int i = 0; i < elements.size(); i++) {
            var elem = elements.get(i);
            var arrayKeys = elem.getKey();
            if (arrayKeys.length != bucketVariableKeys.length) {
                throw malformedBucket("Stored bucket element " + i
                        + " has " + arrayKeys.length
                        + " variable keys, expected " + bucketVariableKeys.length);
            }
            for (int j = 0; j < arrayKeys.length; j++) {
                if (!Utils.valueEquals(arrayKeys[j], bucketVariableKeys[j])) {
                    continue nextElement;
                }
            }
            return i;
        }
        return -1;
    }

    private void validateBucketVariableKeys(Buf[] bucketVariableKeys) {
        if (bucketVariableKeys == null) {
            throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Bucket variable keys must not be null");
        }
        var expectedVariableKeys = col.schema().variableLengthKeysCount();
        if (bucketVariableKeys.length != expectedVariableKeys) {
            throw RocksDBException.of(RocksDBErrorType.KEYS_COUNT_MISMATCH,
                    "Bucket variable keys count must be equal to the column variable-length keys count. Expected: "
                            + expectedVariableKeys + ", got: " + bucketVariableKeys.length);
        }
        for (int i = 0; i < bucketVariableKeys.length; i++) {
            if (bucketVariableKeys[i] == null) {
                throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT,
                        "Bucket variable key at index " + i + " must not be null");
            }
        }
    }

    private static void requireBucketBytes(int totalSize, int offset, int requiredBytes, String fieldDescription) {
        if (requiredBytes < 0) {
            throw malformedBucket("Bucket segment required byte count for " + fieldDescription
                    + " must be non-negative, got " + requiredBytes);
        }
        if (offset < 0 || offset > totalSize || requiredBytes > totalSize - offset) {
            throw malformedBucket("Bucket segment too short for " + fieldDescription
                    + ": need " + requiredBytes + " bytes at offset " + offset + ", size " + totalSize);
        }
    }

    private static RocksDBException malformedBucket(String message) {
        return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, message);
    }

    public Buf toSegment() {
        if (this.elements.isEmpty()) {
            return emptyBuf();
        }
        Buf[] serializedElements = new Buf[this.elements.size()];
        ArrayList<Entry<Buf[], Buf>> entries = this.elements;
        for (int i = 0; i < entries.size(); i++) {
            Entry<Buf[], Buf> element = entries.get(i);
            var computedBucketElementKey = col.computeBucketElementKey(element.getKey());
            var computedBucketElementValue = col.computeBucketElementValue(element.getValue());
            serializedElements[i] = col.computeBucketElementKeyValue(computedBucketElementKey, computedBucketElementValue);
        }
        int totalSize = Integer.BYTES;
        for (Buf serializedElement : serializedElements) {
            totalSize += Integer.BYTES + serializedElement.size();
        }
        var segment = Buf.createZeroes(totalSize);
        int offset = 0;
        segment.setInt(offset, serializedElements.length);
        offset += Integer.BYTES;
        for (Buf elementAtI : serializedElements) {
            var elementSize = elementAtI.size();
            segment.setInt(offset, toIntExact(elementSize));
            offset += Integer.BYTES;
            segment.setBytesFromBuf(offset, elementAtI, 0, elementSize);
            offset += elementSize;
        }
        return segment;
    }
}
