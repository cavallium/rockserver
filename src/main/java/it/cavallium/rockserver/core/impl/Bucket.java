package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_INT;
import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_INT_UNALIGNED;
import static java.lang.Math.toIntExact;

import it.cavallium.rockserver.core.common.Utils;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;

public class Bucket {

	private final ColumnInstance col;
	private final ArrayList<Entry<MemorySegment[], MemorySegment>> elements;

	public Bucket(ColumnInstance col) {
		this(col, MemorySegment.NULL);
	}

	public Bucket(ColumnInstance col, MemorySegment rawBucketSegment) {
		this.col = col;
		long offset = 0;
		this.elements = new ArrayList<>();
		long rawBucketSegmentByteSize = rawBucketSegment.byteSize();
		if (rawBucketSegmentByteSize > 0) {
			var elements = rawBucketSegment.get(ColumnInstance.BIG_ENDIAN_INT_UNALIGNED, offset);
			offset += Integer.BYTES;
			int elementI = 0;
			while (elementI < elements) {
				var elementKVSize = rawBucketSegment.get(ColumnInstance.BIG_ENDIAN_INT_UNALIGNED, offset);
				offset += Integer.BYTES;

				MemorySegment[] bucketElementKeys;
				{
					int segmentOffset = 0;
					var elementKVSegment = rawBucketSegment.asSlice(offset, elementKVSize, 1);
					int readKeys = 0;
					bucketElementKeys = new MemorySegment[col.schema().variableLengthKeysCount()];
					while (readKeys < col.schema().variableLengthKeysCount()) {
						var keyISize = elementKVSegment.get(ColumnInstance.BIG_ENDIAN_CHAR_UNALIGNED, segmentOffset);
						segmentOffset += Character.BYTES;
						var elementKeyISegment = elementKVSegment.asSlice(segmentOffset, keyISize);
						bucketElementKeys[readKeys] = elementKeyISegment;
						segmentOffset += keyISize;
						readKeys++;
					}

					MemorySegment bucketElementValues;
					if (col.schema().hasValue()) {
						bucketElementValues = elementKVSegment.asSlice(segmentOffset, elementKVSize - segmentOffset);
						segmentOffset = elementKVSize;
					} else {
						bucketElementValues = MemorySegment.NULL;
						assert segmentOffset == elementKVSize;
					}

					var entry = Map.entry(bucketElementKeys, bucketElementValues);
					this.elements.add(entry);
					offset += segmentOffset;
				}
				elementI++;
			}
			assert offset == rawBucketSegmentByteSize;
		}
	}

	/**
	 * Add or replace an element
	 * @return Return the previous value ({@link MemorySegment#NULL} if no value is expected),
	 * return null if no element was present
	 */
	@Nullable
	public MemorySegment addElement(MemorySegment[] bucketVariableKeys, @Nullable MemorySegment value) {
		var element = Map.entry(bucketVariableKeys, value != null ? value : MemorySegment.NULL);
		var i = indexOf(bucketVariableKeys);
		if (i == -1) {
			this.elements.add(element);
			return null;
		} else {
			var val = this.elements.set(i, element).getValue();
			assert val != null;
			return val;
		}
	}

	/**
	 * Remove an element
	 * @return Return the previous value ({@link MemorySegment#NULL} if no value is expected),
	 * return null if no element was present
	 */
	@Nullable
	public MemorySegment removeElement(MemorySegment[] bucketVariableKeys) {
		var i = indexOf(bucketVariableKeys);
		if (i == -1) {
			return null;
		} else {
			var val = this.elements.remove(i).getValue();
			assert val != null;
			return val;
		}
	}

	/**
	 * Get an element
	 * @return Return the value ({@link MemorySegment#NULL} if no value is expected),
	 * return null if no element was present
	 */
	@Nullable
	public MemorySegment getElement(MemorySegment[] bucketVariableKeys) {
		var i = indexOf(bucketVariableKeys);
		if (i == -1) {
			return null;
		} else {
			var val = this.elements.get(i).getValue();
			assert val != null;
			return val;
		}
	}

	private int indexOf(MemorySegment[] bucketVariableKeys) {
		nextElement: for (int i = 0; i < elements.size(); i++) {
			var elem = elements.get(i);
			var arrayKeys = elem.getKey();
			assert arrayKeys.length == bucketVariableKeys.length;
			for (int j = 0; j < arrayKeys.length; j++) {
				if (!Utils.valueEquals(arrayKeys[j], bucketVariableKeys[j])) {
					continue nextElement;
				}
			}
			return i;
		}
		return -1;
	}

	public MemorySegment toSegment(Arena arena) {
		if (this.elements.isEmpty()) {
			return MemorySegment.NULL;
		}
		MemorySegment[] serializedElements = new MemorySegment[this.elements.size()];
		ArrayList<Entry<MemorySegment[], MemorySegment>> entries = this.elements;
		for (int i = 0; i < entries.size(); i++) {
			Entry<MemorySegment[], MemorySegment> element = entries.get(i);
			var computedBucketElementKey = col.computeBucketElementKey(arena, element.getKey());
			var computedBucketElementValue = col.computeBucketElementValue(element.getValue());
			serializedElements[i] = col.computeBucketElementKeyValue(arena, computedBucketElementKey, computedBucketElementValue);
		}
		long totalSize = Integer.BYTES;
		for (MemorySegment serializedElement : serializedElements) {
			totalSize += Integer.BYTES + serializedElement.byteSize();
		}
		var segment = arena.allocate(totalSize);
		long offset = 0;
		segment.set(BIG_ENDIAN_INT, offset, serializedElements.length);
		offset += Integer.BYTES;
		for (MemorySegment elementAtI : serializedElements) {
			var elementSize = elementAtI.byteSize();
			segment.set(BIG_ENDIAN_INT_UNALIGNED, offset, toIntExact(elementSize));
			offset += Integer.BYTES;
			MemorySegment.copy(elementAtI, 0, segment, offset, elementSize);
			offset += elementSize;
		}
		return segment;
	}
}
