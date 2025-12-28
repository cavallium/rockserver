package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static java.lang.Math.toIntExact;

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
		this.col = col;
		int offset = 0;
		this.elements = new ArrayList<>();
		int rawBucketSegmentByteSize = rawBucketSegment.size();
		if (rawBucketSegmentByteSize > 0) {
			var elements = rawBucketSegment.getInt(offset);
			offset += Integer.BYTES;
			int elementI = 0;
			while (elementI < elements) {
				var elementKVSize = rawBucketSegment.getInt(offset);
				offset += Integer.BYTES;

				Buf[] bucketElementKeys;
				{
					int segmentOffset = 0;
					var elementKVSegment = rawBucketSegment.subList(offset, offset + elementKVSize);
					int readKeys = 0;
					bucketElementKeys = new Buf[col.schema().variableLengthKeysCount()];
					while (readKeys < col.schema().variableLengthKeysCount()) {
						var keyISize = elementKVSegment.getChar(segmentOffset);
						segmentOffset += Character.BYTES;
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
	 * @return Return the previous value ({@link Utils#emptyBuf()} if no value is expected),
	 * return null if no element was present
	 */
	@Nullable
	public Buf addElement(Buf[] bucketVariableKeys, @Nullable Buf value) {
		var element = Map.entry(bucketVariableKeys, value != null ? value : emptyBuf());
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
	 * @return Return the previous value ({@link Utils#emptyBuf()} if no value is expected),
	 * return null if no element was present
	 */
	@Nullable
	public Buf removeElement(Buf[] bucketVariableKeys) {
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
	 * @return Return the value ({@link Utils#emptyBuf()} if no value is expected),
	 * return null if no element was present
	 */
	@Nullable
	public Buf getElement(Buf[] bucketVariableKeys) {
		var i = indexOf(bucketVariableKeys);
		if (i == -1) {
			return null;
		} else {
			var val = this.elements.get(i).getValue();
			assert val != null;
			return val;
		}
	}

	private int indexOf(Buf[] bucketVariableKeys) {
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
