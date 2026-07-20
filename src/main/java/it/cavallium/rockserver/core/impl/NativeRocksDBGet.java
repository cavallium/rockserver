package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.MemorySegmentBuf;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.PinnedGet;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Pooled access to Rockserver's Java 25 pinned-Get JNI API.
 *
 * <p>The ordinary heap operation returns an exact independently owned array.
 * The leased operation instead exposes RocksDB's native value through a
 * read-only {@link MemorySegmentBuf}. That view is valid only until its
 * {@link PinnedGetLease} is closed or copied. Closing a lease releases the pin
 * and returns its reusable native holder to this adapter.
 *
 * <p>This adapter must not be closed concurrently with a Get or an open lease.
 * {@link EmbeddedDB} enforces that rule with its operation shutdown gate.
 */
final class NativeRocksDBGet implements AutoCloseable {

	private static final int MAX_RETAINED_STATES = 256;

	private final RocksDB database;
	private final ReadOptions defaultReadOptions;
	private final AtomicReferenceArray<State> availableStates;
	private final AtomicReferenceArray<State> allRetainedStates;
	private final AtomicInteger retainedStateCount = new AtomicInteger();
	private final AtomicBoolean closed = new AtomicBoolean();

	NativeRocksDBGet(RocksDB database, long requestedRetainedStateCapacity) {
		if (database.getNativeHandle() == 0) {
			throw new IllegalArgumentException("RocksDB has already been closed");
		}
		if (requestedRetainedStateCapacity <= 0) {
			throw new IllegalArgumentException("requestedRetainedStateCapacity must be positive");
		}
		int capacity = (int) Math.min(requestedRetainedStateCapacity, MAX_RETAINED_STATES);
		this.database = database;
		this.availableStates = new AtomicReferenceArray<>(capacity);
		this.allRetainedStates = new AtomicReferenceArray<>(capacity);
		this.defaultReadOptions = new ReadOptions();
	}

	byte @Nullable [] getHeap(ColumnFamilyHandle columnFamily,
			ReadOptions readOptions,
			byte[] key,
			int keyOffset,
			int keyLength) throws RocksDBException {
		return getHeapInternal(columnFamily, readOptions, key, keyOffset, keyLength);
	}

	byte @Nullable [] getHeap(ColumnFamilyHandle columnFamily,
			byte[] key,
			int keyOffset,
			int keyLength) throws RocksDBException {
		return getHeapInternal(columnFamily, defaultReadOptions, key, keyOffset, keyLength);
	}

	@Nullable PinnedGetLease getPinned(ColumnFamilyHandle columnFamily,
			byte[] key,
			int keyOffset,
			int keyLength) throws RocksDBException {
		State state = acquireState();
		boolean leased = false;
		try {
			if (!state.getPinned(database, columnFamily, defaultReadOptions, key, keyOffset, keyLength)) {
				return null;
			}
			var lease = new PinnedGetLease(this, state);
			leased = true;
			return lease;
		} finally {
			if (!leased) {
				releaseState(state);
			}
		}
	}

	private byte @Nullable [] getHeapInternal(ColumnFamilyHandle columnFamily,
			ReadOptions readOptions,
			byte[] key,
			int keyOffset,
			int keyLength) throws RocksDBException {
		State state = acquireState();
		try {
			return state.getHeap(database, columnFamily, readOptions, key, keyOffset, keyLength);
		} finally {
			releaseState(state);
		}
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
			defaultReadOptions.close();
		} catch (Throwable throwable) {
			failure = addFailure(failure, throwable);
		}
		throwIfFailed("Failed to release native fast-get resources", failure);
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
			state.retain(retainedSlot, start);
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
			state.close();
			return;
		}
		int capacity = availableStates.length();
		for (int offset = 0; offset < capacity; offset++) {
			int slot = (state.preferredSlot + offset) % capacity;
			if (availableStates.compareAndSet(slot, null, state)) {
				state.preferredSlot = slot;
				return;
			}
		}
		throw new IllegalStateException("No free slot for a retained native fast-get state");
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

	private static void throwIfFailed(String message, @Nullable Throwable failure) {
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure instanceof RuntimeException runtimeException) {
			throw runtimeException;
		}
		if (failure != null) {
			throw new IllegalStateException(message, failure);
		}
	}

	static final class PinnedGetLease implements AutoCloseable {

		private final NativeRocksDBGet owner;
		private final State state;
		private final MemorySegmentBuf value;
		private final AtomicBoolean closed = new AtomicBoolean();

		private PinnedGetLease(NativeRocksDBGet owner, State state) {
			this.owner = owner;
			this.state = state;
			this.value = new MemorySegmentBuf(state.pinnedGet.value());
		}

		MemorySegmentBuf value() {
			if (closed.get()) {
				throw new IllegalStateException("pinned Get lease has already been closed");
			}
			return value;
		}

		byte[] copyAndClose() {
			if (!closed.compareAndSet(false, true)) {
				throw new IllegalStateException("pinned Get lease has already been closed");
			}
			try {
				return state.pinnedGet.copyAndReset();
			} finally {
				owner.releaseState(state);
			}
		}

		@Override
		public void close() {
			if (!closed.compareAndSet(false, true)) {
				return;
			}
			try {
				state.pinnedGet.reset();
			} finally {
				owner.releaseState(state);
			}
		}
	}

	private static final class State implements AutoCloseable {

		private final PinnedGet pinnedGet = new PinnedGet();
		private int retainedSlot = -1;
		private int preferredSlot;
		private boolean closed;

		private void retain(int slot, int initialPreferredSlot) {
			if (retainedSlot >= 0) {
				throw new IllegalStateException("native fast-get state is already retained");
			}
			retainedSlot = slot;
			preferredSlot = initialPreferredSlot;
		}

		private byte @Nullable [] getHeap(RocksDB database,
				ColumnFamilyHandle columnFamily,
				ReadOptions readOptions,
				byte[] key,
				int keyOffset,
				int keyLength) throws RocksDBException {
			return database.getPinnedCopy(columnFamily, readOptions, key, keyOffset, keyLength, pinnedGet);
		}

		private boolean getPinned(RocksDB database,
				ColumnFamilyHandle columnFamily,
				ReadOptions readOptions,
				byte[] key,
				int keyOffset,
				int keyLength) throws RocksDBException {
			return database.getPinned(columnFamily, readOptions, key, keyOffset, keyLength, pinnedGet);
		}

		@Override
		public void close() {
			if (!closed) {
				closed = true;
				pinnedGet.close();
			}
		}
	}
}
