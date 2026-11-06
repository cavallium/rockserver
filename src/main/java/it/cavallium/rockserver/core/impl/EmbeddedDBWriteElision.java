package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;

import org.jetbrains.annotations.Nullable;
import org.rocksdb.ByteBufferGetStatus;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.Status;

/**
 * Write elision of {@link EmbeddedDB}: probes the cached logical value of keys before a write so that writes
 * which would not change the stored value can be skipped.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBWriteElision {

	private final EmbeddedDB edb;

	EmbeddedDBWriteElision(EmbeddedDB edb) {
		this.edb = edb;
	}

	enum WriteElisionRequest {
		ENSURE("ensure"),
		PREVIOUS_PRESENCE("previous_presence");

		final String tag;

		WriteElisionRequest(String tag) {
			this.tag = tag;
		}

		static @Nullable WriteElisionRequest from(RequestPut<?, ?> requestType) {
			if (requestType instanceof RequestType.RequestEnsure<?>) {
				return ENSURE;
			}
			if (requestType instanceof RequestType.RequestPreviousPresence<?>) {
				return PREVIOUS_PRESENCE;
			}
			return null;
		}
	}

	enum WriteElisionDecision {
		ELIDED("elided"),
		FALLBACK_NOT_FOUND("fallback_not_found"),
		FALLBACK_DIFFERENT("fallback_different"),
		FALLBACK_INCOMPLETE("fallback_incomplete"),
		BYPASS_OVERSIZED("bypass_oversized"),
		BYPASS_WRITER("bypass_writer");

		final String tag;

		WriteElisionDecision(String tag) {
			this.tag = tag;
		}
	}

	static final class PhysicalKey {

		private final byte[] bytes;
		private final int hashCode;

		PhysicalKey(byte[] bytes) {
			this.bytes = bytes;
			this.hashCode = Arrays.hashCode(bytes);
		}

		@Override
		public boolean equals(Object other) {
			return this == other || other instanceof PhysicalKey key && Arrays.equals(bytes, key.bytes);
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	record MultiWriteElisionProbe(PhysicalKey[] physicalKeys, WriteElisionDecision[] decisions) {
	}

	private static final class BucketWriteElisionProbe {

		private final PhysicalKey physicalKey;
		private final ArrayList<Integer> logicalIndexes = new ArrayList<>();
		private int requiredSize;

		private BucketWriteElisionProbe(PhysicalKey physicalKey) {
			this.physicalKey = physicalKey;
		}
	}

	private ReadOptions newWriteElisionReadOptions() {
		var readOptions = edb.newReadOptions("write-elision-cache-probe");
		readOptions.setReadTier(ReadTier.BLOCK_CACHE_TIER);
		readOptions.setFillCache(false);
		return readOptions;
	}

	void recordWriteElisionDecision(WriteElisionRequest request, WriteElisionDecision decision) {
		edb.writeElisionDecisionCounters.get(request).get(decision).increment();
	}

	WriteElisionDecision probeCachedLogicalValue(ColumnInstance col,
			PhysicalKey physicalKey,
			Keys logicalKeys,
			Buf requestedValue) {
		try (var readOptions = newWriteElisionReadOptions()) {
			byte[] rawValue;
			try {
				rawValue = edb.db.get().get(col.cfh(), readOptions, physicalKey.bytes);
			} catch (org.rocksdb.RocksDBException exception) {
				return switch (exception.getStatus().getCode()) {
					case NotFound -> WriteElisionDecision.FALLBACK_NOT_FOUND;
					case Incomplete -> WriteElisionDecision.FALLBACK_INCOMPLETE;
					default -> throw mapWriteElisionProbeFailure(exception);
				};
			}
			if (rawValue == null) {
				return WriteElisionDecision.FALLBACK_NOT_FOUND;
			}
			return cachedLogicalValueEquals(col, logicalKeys, requestedValue, Buf.wrap(rawValue))
					? WriteElisionDecision.ELIDED
					: WriteElisionDecision.FALLBACK_DIFFERENT;
		}
	}

	private static boolean cachedLogicalValueEquals(ColumnInstance col,
			Keys logicalKeys,
			Buf requestedValue,
			Buf rawValue) {
		if (col.hasBuckets()) {
			var bucket = new Bucket(col, rawValue);
			var existing = bucket.getElement(col.getBucketElementKeys(logicalKeys.keys()));
			return existing != null && (!col.schema().hasValue() || Utils.valueEquals(existing, requestedValue));
		}
		return !col.schema().hasValue() || Utils.valueEquals(rawValue, requestedValue);
	}

	private static RocksDBException mapWriteElisionProbeFailure(org.rocksdb.RocksDBException exception) {
		return RocksDBException.of(RocksDBErrorType.PUT_2, exception);
	}

	private static RocksDBException mapWriteElisionProbeFailure(Status status) {
		return mapWriteElisionProbeFailure(new org.rocksdb.RocksDBException(status));
	}

	MultiWriteElisionProbe probeCachedLogicalValues(ColumnInstance col,
			List<Keys> logicalKeys,
			List<Buf> requestedValues) {
		var physicalKeys = new PhysicalKey[logicalKeys.size()];
		var decisions = new WriteElisionDecision[logicalKeys.size()];
		for (int i = 0; i < logicalKeys.size(); i++) {
			col.checkNullableValue(requestedValues.get(i));
			physicalKeys[i] = new PhysicalKey(col.calculateKeyBytes(logicalKeys.get(i).keys()));
		}
		if (col.hasBuckets()) {
			probeCachedBuckets(col, logicalKeys, requestedValues, physicalKeys, decisions);
		} else {
			probeCachedDirectValues(col, requestedValues, physicalKeys, decisions);
		}
		return new MultiWriteElisionProbe(physicalKeys, decisions);
	}

	private void probeCachedDirectValues(ColumnInstance col,
			List<Buf> requestedValues,
			PhysicalKey[] physicalKeys,
			WriteElisionDecision[] decisions) {
		int offset = 0;
		try (var readOptions = newWriteElisionReadOptions()) {
			while (offset < physicalKeys.length) {
				var indexes = new ArrayList<Integer>();
				long probeBytes = 0L;
				while (offset < physicalKeys.length && indexes.size() < EmbeddedDB.WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL) {
					var physicalKey = physicalKeys[offset];
					var requestedValue = requestedValues.get(offset);
					long entryBytes = EmbeddedDB.saturatingAdd(physicalKey.bytes.length,
							col.schema().hasValue() ? requestedValue.size() : 0L);
					if (entryBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
						decisions[offset++] = WriteElisionDecision.BYPASS_OVERSIZED;
						continue;
					}
					if (!indexes.isEmpty()
							&& entryBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL - probeBytes) {
						break;
					}
					indexes.add(offset);
					probeBytes += entryBytes;
					offset++;
				}
				if (indexes.isEmpty()) {
					continue;
				}
				try (var arena = Arena.ofConfined()) {
					var keyBytes = new byte[indexes.size()][];
					// Each value slot is exactly the requested size, so a longer stored value reports its requiredSize
					var valueSizes = new long[indexes.size()];
					for (int i = 0; i < indexes.size(); i++) {
						int logicalIndex = indexes.get(i);
						keyBytes[i] = physicalKeys[logicalIndex].bytes;
						valueSizes[i] = col.schema().hasValue() ? requestedValues.get(logicalIndex).size() : 0;
					}
					var nativeKeys = NativeStaging.stageKeys(arena, keyBytes);
					var nativeValues = NativeStaging.allocateSlices(arena, valueSizes);
					var statuses = runWriteElisionMultiGet(col, readOptions, nativeKeys, nativeValues);
					for (int i = 0; i < statuses.size(); i++) {
						int logicalIndex = indexes.get(i);
						var status = statuses.get(i);
						decisions[logicalIndex] = switch (status.status.getCode()) {
							case Ok -> !col.schema().hasValue()
									|| status.requiredSize == requestedValues.get(logicalIndex).size()
									&& NativeStaging.contentEquals(nativeValues[i],
									requestedValues.get(logicalIndex), status.requiredSize)
									? WriteElisionDecision.ELIDED
									: WriteElisionDecision.FALLBACK_DIFFERENT;
							case NotFound -> WriteElisionDecision.FALLBACK_NOT_FOUND;
							case Incomplete -> WriteElisionDecision.FALLBACK_INCOMPLETE;
							default -> throw mapWriteElisionProbeFailure(status.status);
						};
					}
				}
			}
		} catch (org.rocksdb.RocksDBException exception) {
			throw mapWriteElisionProbeFailure(exception);
		}
	}

	private void probeCachedBuckets(ColumnInstance col,
			List<Keys> logicalKeys,
			List<Buf> requestedValues,
			PhysicalKey[] physicalKeys,
			WriteElisionDecision[] decisions) {
		var groupsByKey = new LinkedHashMap<PhysicalKey, BucketWriteElisionProbe>();
		for (int i = 0; i < physicalKeys.length; i++) {
			long individualProbeBytes = EmbeddedDB.saturatingAdd(physicalKeys[i].bytes.length, requestedValues.get(i).size());
			if (individualProbeBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
				decisions[i] = WriteElisionDecision.BYPASS_OVERSIZED;
				continue;
			}
			groupsByKey.computeIfAbsent(physicalKeys[i], BucketWriteElisionProbe::new).logicalIndexes.add(i);
		}
		var groups = new ArrayList<>(groupsByKey.values());
		var valuePassGroups = new ArrayList<BucketWriteElisionProbe>();
		try (var readOptions = newWriteElisionReadOptions()) {
			int offset = 0;
			while (offset < groups.size()) {
				var chunk = new ArrayList<BucketWriteElisionProbe>();
				long probeBytes = 0L;
				while (offset < groups.size() && chunk.size() < EmbeddedDB.WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL) {
					var group = groups.get(offset);
					long entryBytes = group.physicalKey.bytes.length;
					if (entryBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
						setBucketDecision(group, decisions, WriteElisionDecision.BYPASS_OVERSIZED);
						offset++;
						continue;
					}
					if (!chunk.isEmpty()
							&& entryBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL - probeBytes) {
						break;
					}
					chunk.add(group);
					probeBytes += entryBytes;
					offset++;
				}
				if (chunk.isEmpty()) {
					continue;
				}
				try (var arena = Arena.ofConfined()) {
					var keyBytes = new byte[chunk.size()][];
					for (int i = 0; i < chunk.size(); i++) {
						keyBytes[i] = chunk.get(i).physicalKey.bytes;
					}
					var nativeKeys = NativeStaging.stageKeys(arena, keyBytes);
					// Size-only pass: one shared zero-capacity value segment
					var nativeValues = new MemorySegment[chunk.size()];
					Arrays.fill(nativeValues, arena.allocate(0));
					var statuses = runWriteElisionMultiGet(col, readOptions, nativeKeys, nativeValues);
					for (int i = 0; i < statuses.size(); i++) {
						var group = chunk.get(i);
						var status = statuses.get(i);
						switch (status.status.getCode()) {
							case Ok -> {
								group.requiredSize = status.requiredSize;
								long valuePassBytes = EmbeddedDB.saturatingAdd(group.physicalKey.bytes.length,
										status.requiredSize);
								if (valuePassBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
									setBucketDecision(group, decisions, WriteElisionDecision.BYPASS_OVERSIZED);
								} else {
									valuePassGroups.add(group);
								}
							}
							case NotFound -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_NOT_FOUND);
							case Incomplete -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_INCOMPLETE);
							default -> throw mapWriteElisionProbeFailure(status.status);
						}
					}
				}
			}

			offset = 0;
			while (offset < valuePassGroups.size()) {
				var chunk = new ArrayList<BucketWriteElisionProbe>();
				long probeBytes = 0L;
				while (offset < valuePassGroups.size() && chunk.size() < EmbeddedDB.WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL) {
					var group = valuePassGroups.get(offset);
					long entryBytes = EmbeddedDB.saturatingAdd(group.physicalKey.bytes.length, group.requiredSize);
					if (!chunk.isEmpty()
							&& entryBytes > EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL - probeBytes) {
						break;
					}
					chunk.add(group);
					probeBytes += entryBytes;
					offset++;
				}
				try (var arena = Arena.ofConfined()) {
					var keyBytes = new byte[chunk.size()][];
					var valueSizes = new long[chunk.size()];
					for (int i = 0; i < chunk.size(); i++) {
						var group = chunk.get(i);
						keyBytes[i] = group.physicalKey.bytes;
						valueSizes[i] = group.requiredSize;
					}
					var nativeKeys = NativeStaging.stageKeys(arena, keyBytes);
					var nativeValues = NativeStaging.allocateSlices(arena, valueSizes);
					var statuses = runWriteElisionMultiGet(col, readOptions, nativeKeys, nativeValues);
					for (int i = 0; i < statuses.size(); i++) {
						var group = chunk.get(i);
						var status = statuses.get(i);
						switch (status.status.getCode()) {
							case Ok -> {
								if (status.requiredSize > nativeValues[i].byteSize()) {
									var decision = EmbeddedDB.saturatingAdd(group.physicalKey.bytes.length, status.requiredSize)
											> EmbeddedDB.WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL
											? WriteElisionDecision.BYPASS_OVERSIZED
											: WriteElisionDecision.FALLBACK_DIFFERENT;
									setBucketDecision(group, decisions, decision);
									continue;
								}
								var rawBucket = Buf.wrap(copyMemorySegment(nativeValues[i], status.requiredSize));
								var bucket = new Bucket(col, rawBucket);
								for (int logicalIndex : group.logicalIndexes) {
									var existing = bucket.getElement(
											col.getBucketElementKeys(logicalKeys.get(logicalIndex).keys()));
									decisions[logicalIndex] = existing != null
											&& (!col.schema().hasValue()
											|| Utils.valueEquals(existing, requestedValues.get(logicalIndex)))
											? WriteElisionDecision.ELIDED
											: WriteElisionDecision.FALLBACK_DIFFERENT;
								}
							}
							case NotFound -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_NOT_FOUND);
							case Incomplete -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_INCOMPLETE);
							default -> throw mapWriteElisionProbeFailure(status.status);
						}
					}
				}
			}
		} catch (org.rocksdb.RocksDBException exception) {
			throw mapWriteElisionProbeFailure(exception);
		}
	}

	private List<ByteBufferGetStatus> runWriteElisionMultiGet(ColumnInstance col,
			ReadOptions readOptions,
			MemorySegment[] nativeKeys,
			MemorySegment[] nativeValues) throws org.rocksdb.RocksDBException {
		var observer = edb.writeElisionMultiGetObserver;
		if (observer != null) {
			long probeBytes = 0L;
			for (int i = 0; i < nativeKeys.length; i++) {
				probeBytes = EmbeddedDB.saturatingAdd(probeBytes, nativeKeys[i].byteSize());
				probeBytes = EmbeddedDB.saturatingAdd(probeBytes, nativeValues[i].byteSize());
			}
			observer.accept(nativeKeys.length, probeBytes);
		}
		long readStarted = System.nanoTime();
		edb.activeNativeMultiGets.incrementAndGet();
		try {
			return edb.db.get().multiGetByteBuffers(readOptions, List.of(col.cfh()), nativeKeys, nativeValues);
		} finally {
			edb.activeNativeMultiGets.decrementAndGet();
			edb.storageMetrics.recordCompactionForegroundRead(readStarted, nativeKeys.length);
		}
	}

	private static void setBucketDecision(BucketWriteElisionProbe group,
			WriteElisionDecision[] decisions,
			WriteElisionDecision decision) {
		for (int logicalIndex : group.logicalIndexes) {
			decisions[logicalIndex] = decision;
		}
	}

	/**
	 * The bytes of {@code value} for a JNI call that only reads them during the call: the backing array itself when
	 * it is exact, a copy otherwise. Never store the result, since it may alias the caller's buffer.
	 */
	static byte[] jniArray(Buf value) {
		byte[] array = value.asArrayStrict();
		return array != null ? array : value.toByteArray();
	}

	private static byte[] copyMemorySegment(MemorySegment source, int length) {
		return source.asSlice(0, length).toArray(ValueLayout.JAVA_BYTE);
	}
}
