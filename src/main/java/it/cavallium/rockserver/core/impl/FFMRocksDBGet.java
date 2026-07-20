package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBMetadata;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

/**
 * MemorySegment-based access to RocksDB's pinned C Get API.
 *
 * <p>RocksJava exposes raw {@code DB*}, {@code ColumnFamilyHandle*}, and {@code ReadOptions*} handles, while the
 * RocksDB C API accepts opaque wrappers. In the pinned {@code 11.1.2.2:linux64} ABI, {@code rocksdb_t} and
 * {@code rocksdb_column_family_handle_t} start with those respective pointers, and {@code rocksdb_readoptions_t}
 * starts with an inline {@code ReadOptions}. This adapter supplies the two pointer wrappers and passes the raw
 * ReadOptions address as that first inline member. The artifact checks below accept the exact release and its
 * full-commit Maven snapshots, while deliberately turning a future ABI change into a clear startup failure instead
 * of memory corruption.
 *
 * <p>The Get downcall is intentionally not marked critical: a RocksDB read can block, decompress data, or execute a
 * merge operator. Keys are copied into bounded, reusable pooled native memory. Values remain pinned only while they
 * are copied once into their final Java array, after which the native pin is deterministically destroyed.
 *
 * <p>Like RocksJava's native-owning option and handle types, this adapter must not be closed concurrently with a Get.
 * {@link EmbeddedDB} enforces that ownership rule by closing its operation gate and draining active operations first.
 */
final class FFMRocksDBGet implements AutoCloseable {

	private static final String SUPPORTED_ROCKSDBJNI_VERSION = "11.1.2.2";
	private static final Pattern SUPPORTED_ROCKSDBJNI_COORDINATE = Pattern.compile(
			Pattern.quote(SUPPORTED_ROCKSDBJNI_VERSION) + "(?:-master\\.[0-9a-f]{40}-SNAPSHOT)?");
	private static final int INITIAL_KEY_CAPACITY = 256;
	private static final int MAX_RETAINED_STATES = 256;
	private static final long MAX_ERROR_BYTES = 64L * 1024;
	private static final int EXPECTED_NATIVE_MAJOR = 11;
	private static final int EXPECTED_NATIVE_MINOR = 1;
	private static final int EXPECTED_NATIVE_PATCH = 2;
	private static final Linker LINKER = Linker.nativeLinker();
	private static final MethodHandle GET_PINNED_CF;
	private static final MethodHandle PINNED_GET_VALUE;
	private static final MethodHandle PINNED_DESTROY;
	private static final MethodHandle ROCKSDB_FREE;
	private static final MethodHandle NATIVE_MALLOC;
	private static final MethodHandle NATIVE_FREE;
	private static final MethodHandle READ_OPTIONS_CREATE;
	private static final MethodHandle READ_OPTIONS_SET_ASYNC_IO;
	private static final MethodHandle READ_OPTIONS_DESTROY;

	static {
		RocksDB.loadLibrary();
		String artifactCoordinateVersion = RocksDBMetadata.getRocksDBVersionHash();
		if (!SUPPORTED_ROCKSDBJNI_COORDINATE.matcher(artifactCoordinateVersion).matches()) {
			throw new ExceptionInInitializerError("FFM fast-get requires rocksdbjni "
					+ SUPPORTED_ROCKSDBJNI_VERSION + " or its full-commit master snapshot, found "
					+ artifactCoordinateVersion);
		}
		String packageVersion = RocksDB.class.getPackage().getImplementationVersion();
		if (packageVersion != null && !SUPPORTED_ROCKSDBJNI_VERSION.equals(packageVersion)) {
			throw new ExceptionInInitializerError("FFM fast-get was built for rocksdbjni "
					+ SUPPORTED_ROCKSDBJNI_VERSION + ", but the runtime package is " + packageVersion);
		}
		var nativeVersion = RocksDB.rocksdbVersion();
		if (nativeVersion.getMajor() != EXPECTED_NATIVE_MAJOR
				|| nativeVersion.getMinor() != EXPECTED_NATIVE_MINOR
				|| nativeVersion.getPatch() != EXPECTED_NATIVE_PATCH) {
			throw new ExceptionInInitializerError("FFM fast-get requires native RocksDB "
					+ EXPECTED_NATIVE_MAJOR + "." + EXPECTED_NATIVE_MINOR + "." + EXPECTED_NATIVE_PATCH
					+ ", found " + nativeVersion);
		}
		SymbolLookup lookup = name -> SymbolLookup.loaderLookup().find(name)
				.or(() -> LINKER.defaultLookup().find(name));
		GET_PINNED_CF = LINKER.downcallHandle(find(lookup, "rocksdb_get_pinned_cf_v2"),
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));
		PINNED_GET_VALUE = LINKER.downcallHandle(find(lookup, "rocksdb_pinnable_handle_get_value"),
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
		PINNED_DESTROY = LINKER.downcallHandle(find(lookup, "rocksdb_pinnable_handle_destroy"),
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
		ROCKSDB_FREE = LINKER.downcallHandle(find(lookup, "rocksdb_free"),
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
		MemorySegment malloc;
		MemorySegment free;
		var jeMalloc = lookup.find("je_malloc");
		var jeFree = lookup.find("je_free");
		if (jeMalloc.isPresent() && jeFree.isPresent()) {
			malloc = jeMalloc.orElseThrow();
			free = jeFree.orElseThrow();
		} else {
			malloc = find(lookup, "malloc");
			free = find(lookup, "free");
		}
		NATIVE_MALLOC = LINKER.downcallHandle(malloc,
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
		NATIVE_FREE = LINKER.downcallHandle(free,
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
		READ_OPTIONS_CREATE = LINKER.downcallHandle(find(lookup, "rocksdb_readoptions_create"),
				FunctionDescriptor.of(ValueLayout.ADDRESS));
		READ_OPTIONS_SET_ASYNC_IO = LINKER.downcallHandle(find(lookup, "rocksdb_readoptions_set_async_io"),
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
		READ_OPTIONS_DESTROY = LINKER.downcallHandle(find(lookup, "rocksdb_readoptions_destroy"),
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	static void ensureRuntimeCompatible() {
		// Calling this method forces the compatibility and symbol checks in the static initializer
		// to finish before EmbeddedDB acquires metrics, native handles, the DB lock, or threads.
	}

	private final MemorySegment databaseWrapper;
	private final MemorySegment defaultReadOptions;
	private final MethodHandle getPinnedForDatabase;
	private final MethodHandle getPinnedWithDefaultOptions;
	private final AtomicReferenceArray<State> availableStates;
	private final AtomicReferenceArray<State> allRetainedStates;
	private final AtomicInteger retainedStateCount = new AtomicInteger();
	private final LongAdder releasedTransientCalls = new LongAdder();
	private final AtomicBoolean closed = new AtomicBoolean();

	FFMRocksDBGet(RocksDB database, long requestedRetainedStateCapacity) {
		long databaseHandle = database.getNativeHandle();
		if (databaseHandle == 0) {
			throw new IllegalArgumentException("RocksDB has already been closed");
		}
		if (requestedRetainedStateCapacity <= 0) {
			throw new IllegalArgumentException("requestedRetainedStateCapacity must be positive");
		}
		int retainedStateCapacity = (int) Math.min(requestedRetainedStateCapacity, MAX_RETAINED_STATES);
		MemorySegment newDatabaseWrapper = allocateNative(ValueLayout.ADDRESS.byteSize());
		MemorySegment newDefaultReadOptions = null;
		try {
			newDatabaseWrapper.set(ValueLayout.JAVA_LONG, 0, databaseHandle);
			newDefaultReadOptions = createDefaultReadOptions();
			this.databaseWrapper = newDatabaseWrapper;
			this.defaultReadOptions = newDefaultReadOptions;
			this.getPinnedForDatabase = MethodHandles.insertArguments(GET_PINNED_CF, 0, databaseWrapper);
			this.getPinnedWithDefaultOptions = MethodHandles.insertArguments(GET_PINNED_CF,
					0,
					databaseWrapper,
					defaultReadOptions);
			this.availableStates = new AtomicReferenceArray<>(retainedStateCapacity);
			this.allRetainedStates = new AtomicReferenceArray<>(retainedStateCapacity);
		} catch (RuntimeException | Error throwable) {
			if (newDefaultReadOptions != null) {
				destroyReadOptions(newDefaultReadOptions);
			}
			freeNative(newDatabaseWrapper);
			throw throwable;
		}
	}

	byte @Nullable [] get(ColumnFamilyHandle columnFamily,
			ReadOptions readOptions,
			byte[] key,
			int keyOffset,
			int keyLength) throws NativeError {
		long readOptionsHandle = readOptions.getNativeHandle();
		if (readOptionsHandle == 0) {
			throw new IllegalArgumentException("ReadOptions has already been closed");
		}
		return getInternal(columnFamily,
				MemorySegment.ofAddress(readOptionsHandle),
				key,
				keyOffset,
				keyLength);
	}

	byte @Nullable [] get(ColumnFamilyHandle columnFamily,
			byte[] key,
			int keyOffset,
			int keyLength) throws NativeError {
		return getInternal(columnFamily, null, key, keyOffset, keyLength);
	}

	private byte @Nullable [] getInternal(ColumnFamilyHandle columnFamily,
			@Nullable MemorySegment readOptionsAddress,
			byte[] key,
			int keyOffset,
			int keyLength) throws NativeError {
		Objects.checkFromIndexSize(keyOffset, keyLength, key.length);
		if (closed.get()) {
			throw new IllegalStateException("FFM fast-get has already been closed");
		}
		long columnFamilyHandle = columnFamily.getNativeHandle();
		if (columnFamilyHandle == 0) {
			throw new IllegalArgumentException("ColumnFamilyHandle has already been closed");
		}
		State state = acquireState();
		try {
			return getPinned(state,
					columnFamilyHandle,
					readOptionsAddress,
					key,
					keyOffset,
					keyLength);
		} finally {
			releaseState(state);
		}
	}

	private byte @Nullable [] getPinned(State state,
			long columnFamilyHandle,
			@Nullable MemorySegment readOptionsAddress,
			byte[] key,
			int keyOffset,
			int keyLength) throws NativeError {
		MemorySegment nativeKey = state.copyKey(key, keyOffset, keyLength);
		state.columnFamilyWrapper.set(ValueLayout.JAVA_LONG, 0, columnFamilyHandle);
		state.errorOut.set(ValueLayout.JAVA_LONG, 0, 0L);
		state.valueLengthOut.set(ValueLayout.JAVA_LONG, 0, 0L);

		MemorySegment pinnedHandle;
		try {
			if (readOptionsAddress != null) {
				pinnedHandle = (MemorySegment) getPinnedForDatabase.invokeExact(readOptionsAddress,
						state.columnFamilyWrapper,
						nativeKey,
						(long) keyLength,
						state.errorOut);
			} else {
				pinnedHandle = (MemorySegment) getPinnedWithDefaultOptions.invokeExact(state.columnFamilyWrapper,
						nativeKey,
						(long) keyLength,
						state.errorOut);
			}
		} catch (RuntimeException | Error throwable) {
			throw throwable;
		} catch (Throwable throwable) {
			throw new NativeError("FFM pinned Get invocation failed", throwable);
		}
		state.calls++;

		if (pinnedHandle.address() == 0) {
			String nativeError = takeNativeError(state.errorOut);
			if (nativeError != null) {
				throw NativeError.rocksDBStatus(nativeError);
			}
			return null;
		}

		Throwable failure = null;
		try {
			MemorySegment valueAddress = (MemorySegment) PINNED_GET_VALUE.invokeExact(pinnedHandle,
					state.valueLengthOut);
			long valueLength = state.valueLengthOut.get(ValueLayout.JAVA_LONG, 0);
			byte[] value = new byte[Math.toIntExact(valueLength)];
			if (valueLength != 0) {
				if (valueAddress.address() == 0) {
					throw new NativeError("Pinned Get returned a null address for " + valueLength + " bytes");
				}
				MemorySegment.copy(valueAddress.reinterpret(valueLength),
						ValueLayout.JAVA_BYTE,
						0,
						value,
						0,
						value.length);
			}
			return value;
		} catch (NativeError throwable) {
			failure = throwable;
			throw throwable;
		} catch (RuntimeException | Error throwable) {
			failure = throwable;
			throw throwable;
		} catch (Throwable throwable) {
			var wrapped = new NativeError("Failed to copy the pinned RocksDB value", throwable);
			failure = wrapped;
			throw wrapped;
		} finally {
			try {
				PINNED_DESTROY.invokeExact(pinnedHandle);
			} catch (Throwable destroyFailure) {
				if (failure != null) {
					failure.addSuppressed(destroyFailure);
				} else {
					throw new NativeError("Failed to destroy the pinned RocksDB value", destroyFailure);
				}
			}
		}
	}

	long getCallsCount() {
		long calls = releasedTransientCalls.sum();
		for (int i = 0; i < allRetainedStates.length(); i++) {
			State state = allRetainedStates.get(i);
			if (state != null) {
				calls += state.calls;
			}
		}
		return calls;
	}

	int getRetainedStateCount() {
		return retainedStateCount.get();
	}

	int getRetainedStateCapacity() {
		return allRetainedStates.length();
	}

	boolean isClosed() {
		return closed.get();
	}

	@Override
	public void close() {
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		Throwable failure = null;
		for (int i = 0; i < allRetainedStates.length(); i++) {
			State state = allRetainedStates.get(i);
			if (state != null) {
				failure = closeState(state, failure);
			}
		}
		try {
			destroyReadOptions(defaultReadOptions);
		} catch (Throwable throwable) {
			failure = addFailure(failure, throwable);
		}
		try {
			freeNative(databaseWrapper);
		} catch (Throwable throwable) {
			failure = addFailure(failure, throwable);
		}
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure instanceof RuntimeException runtimeException) {
			throw runtimeException;
		}
		if (failure != null) {
			throw new IllegalStateException("Failed to release FFM fast-get resources", failure);
		}
	}

	private State acquireState() {
		int capacity = availableStates.length();
		int start = (int) Math.floorMod(Thread.currentThread().threadId(), (long) capacity);
		for (int offset = 0; offset < capacity; offset++) {
			int slot = (start + offset) % capacity;
			State state = availableStates.get(slot);
			if (state != null && availableStates.compareAndSet(slot, state, null)) {
				state.preferredSlot = slot;
				return state;
			}
		}

		State state = new State();
		int retainedSlot = reserveRetainedStateSlot();
		if (retainedSlot >= 0) {
			state.retain(retainedSlot);
			allRetainedStates.set(retainedSlot, state);
		}
		return state;
	}

	private int reserveRetainedStateSlot() {
		while (true) {
			int count = retainedStateCount.get();
			if (count >= allRetainedStates.length()) {
				return -1;
			}
			if (retainedStateCount.compareAndSet(count, count + 1)) {
				return count;
			}
		}
	}

	private void releaseState(State state) {
		if (state.retainedSlot < 0) {
			try {
				state.close();
			} finally {
				releasedTransientCalls.add(state.calls);
			}
			return;
		}
		int capacity = availableStates.length();
		int start = state.preferredSlot;
		for (int offset = 0; offset < capacity; offset++) {
			int slot = (start + offset) % capacity;
			if (availableStates.compareAndSet(slot, null, state)) {
				state.preferredSlot = slot;
				return;
			}
		}
		throw new IllegalStateException("No free slot for a retained FFM fast-get state");
	}

	private static Throwable closeState(State state, @Nullable Throwable failure) {
		try {
			state.close();
			return failure;
		} catch (Throwable throwable) {
			return addFailure(failure, throwable);
		}
	}

	private static Throwable addFailure(@Nullable Throwable failure, Throwable additional) {
		if (failure == null) {
			return additional;
		}
		failure.addSuppressed(additional);
		return failure;
	}

	private static MemorySegment find(SymbolLookup lookup, String name) {
		return lookup.find(name).orElseThrow(() -> new UnsatisfiedLinkError("Native symbol not found: " + name));
	}

	private static MemorySegment allocateNative(long size) {
		try {
			MemorySegment address = (MemorySegment) NATIVE_MALLOC.invokeExact(size);
			if (address.equals(MemorySegment.NULL)) {
				throw new OutOfMemoryError("Native allocation failed for " + size + " bytes");
			}
			return address.reinterpret(size);
		} catch (RuntimeException | Error throwable) {
			throw throwable;
		} catch (Throwable throwable) {
			throw new IllegalStateException("Native allocation failed for " + size + " bytes", throwable);
		}
	}

	private static MemorySegment createDefaultReadOptions() {
		MemorySegment readOptions = null;
		try {
			readOptions = (MemorySegment) READ_OPTIONS_CREATE.invokeExact();
			if (readOptions.equals(MemorySegment.NULL)) {
				throw new OutOfMemoryError("rocksdb_readoptions_create returned null");
			}
			READ_OPTIONS_SET_ASYNC_IO.invokeExact(readOptions, (byte) 1);
			return readOptions;
		} catch (RuntimeException | Error throwable) {
			if (readOptions != null && !readOptions.equals(MemorySegment.NULL)) {
				destroyReadOptions(readOptions);
			}
			throw throwable;
		} catch (Throwable throwable) {
			if (readOptions != null && !readOptions.equals(MemorySegment.NULL)) {
				destroyReadOptions(readOptions);
			}
			throw new IllegalStateException("Failed to create reusable RocksDB ReadOptions", throwable);
		}
	}

	private static void destroyReadOptions(MemorySegment readOptions) {
		try {
			READ_OPTIONS_DESTROY.invokeExact(readOptions);
		} catch (RuntimeException | Error throwable) {
			throw throwable;
		} catch (Throwable throwable) {
			throw new IllegalStateException("Failed to destroy reusable RocksDB ReadOptions", throwable);
		}
	}

	private static void freeNative(MemorySegment memory) {
		try {
			NATIVE_FREE.invokeExact(memory);
		} catch (RuntimeException | Error throwable) {
			throw throwable;
		} catch (Throwable throwable) {
			throw new IllegalStateException("Native free failed", throwable);
		}
	}

	@Nullable
	private static String takeNativeError(MemorySegment errorOut) throws NativeError {
		long errorAddress = errorOut.get(ValueLayout.JAVA_LONG, 0);
		if (errorAddress == 0) {
			return null;
		}
		MemorySegment error = MemorySegment.ofAddress(errorAddress);
		String decoded = null;
		NativeError failure = null;
		try {
			decoded = error.reinterpret(MAX_ERROR_BYTES).getString(0);
		} catch (Throwable throwable) {
			failure = new NativeError("Failed to decode RocksDB's native error", throwable);
		}
		try {
			ROCKSDB_FREE.invokeExact(error);
		} catch (Throwable freeFailure) {
			if (failure != null) {
				failure.addSuppressed(freeFailure);
			} else {
				failure = new NativeError("Failed to free RocksDB's native error", freeFailure);
			}
		}
		errorOut.set(ValueLayout.JAVA_LONG, 0, 0L);
		if (failure != null) {
			throw failure;
		}
		return decoded;
	}

	static final class NativeError extends Exception {
		private final boolean rocksDBStatus;

		private NativeError(String message) {
			this(message, null, false);
		}

		private NativeError(String message, Throwable cause) {
			this(message, cause, false);
		}

		private NativeError(String message, @Nullable Throwable cause, boolean rocksDBStatus) {
			super(message, cause);
			this.rocksDBStatus = rocksDBStatus;
		}

		private static NativeError rocksDBStatus(String message) {
			return new NativeError(message, null, true);
		}

		boolean isRocksDBStatus() {
			return rocksDBStatus;
		}
	}

	private static final class State implements AutoCloseable {

		private static final long COLUMN_FAMILY_WRAPPER_SIZE = 2 * ValueLayout.ADDRESS.byteSize();
		private static final long VALUE_LENGTH_OFFSET = COLUMN_FAMILY_WRAPPER_SIZE;
		private static final long ERROR_OFFSET = VALUE_LENGTH_OFFSET + ValueLayout.JAVA_LONG.byteSize();
		private static final long METADATA_SIZE = ERROR_OFFSET + ValueLayout.JAVA_LONG.byteSize();

		private int retainedSlot = -1;
		private final MemorySegment metadata;
		private final MemorySegment columnFamilyWrapper;
		private final MemorySegment valueLengthOut;
		private final MemorySegment errorOut;
		private MemorySegment key;
		private int preferredSlot;
		private long calls;
		private boolean closed;

		private State() {
			MemorySegment newMetadata = allocateNative(METADATA_SIZE);
			try {
				this.key = allocateNative(INITIAL_KEY_CAPACITY);
			} catch (Throwable throwable) {
				freeNative(newMetadata);
				throw throwable;
			}
			this.metadata = newMetadata;
			this.columnFamilyWrapper = metadata.asSlice(0, COLUMN_FAMILY_WRAPPER_SIZE);
			this.valueLengthOut = metadata.asSlice(VALUE_LENGTH_OFFSET, ValueLayout.JAVA_LONG.byteSize());
			this.errorOut = metadata.asSlice(ERROR_OFFSET, ValueLayout.JAVA_LONG.byteSize());
		}

		private void retain(int slot) {
			if (retainedSlot >= 0) {
				throw new IllegalStateException("FFM fast-get state is already retained");
			}
			retainedSlot = slot;
			preferredSlot = slot;
		}

		private MemorySegment copyKey(byte[] source, int offset, int length) {
			if (length > key.byteSize()) {
				long newCapacity = Math.max(length, Math.multiplyExact(key.byteSize(), 2));
				MemorySegment oldKey = key;
				key = allocateNative(newCapacity);
				freeNative(oldKey);
			}
			MemorySegment.copy(source, offset, key, ValueLayout.JAVA_BYTE, 0, length);
			return key;
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}
			closed = true;
			Throwable failure = null;
			try {
				freeNative(key);
			} catch (Throwable throwable) {
				failure = throwable;
			}
			try {
				freeNative(metadata);
			} catch (Throwable throwable) {
				failure = addFailure(failure, throwable);
			}
			if (failure instanceof Error error) {
				throw error;
			}
			if (failure instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (failure != null) {
				throw new IllegalStateException("Failed to release FFM fast-get state", failure);
			}
		}
	}
}
