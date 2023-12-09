package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RocksDBAPI {

	/**
	 * Open a transaction
	 * @param timeoutMs timeout in milliseconds
	 * @return transaction id
	 */
	long openTransaction(long timeoutMs) throws RocksDBException;

	/**
	 * Close a transaction
	 *
	 * @param transactionId transaction id to close
	 * @param commit true to commit the transaction, false to rollback it
	 * @return true if committed, if false, you should try again
	 */
	boolean closeTransaction(long transactionId, boolean commit) throws RocksDBException;

	/**
	 * Close a failed update, discarding all changes
	 *
	 * @param updateId update id to close
	 */
	void closeFailedUpdate(long updateId) throws RocksDBException;

	/**
	 * Create a column
	 * @param name   column name
	 * @param schema column key-value schema
	 * @return column id
	 */
	long createColumn(String name, @NotNull ColumnSchema schema) throws RocksDBException;

	/**
	 * Delete a column
	 * @param columnId column id
	 */
	void deleteColumn(long columnId) throws RocksDBException;

	/**
	 * Get column id by name
	 * @param name column name
	 * @return column id
	 */
	long getColumnId(@NotNull String name) throws RocksDBException;

	/**
	 * Put an element into the specified position
	 * @param arena arena
	 * @param transactionOrUpdateId transaction id, update id, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param value value, or null if not needed
	 * @param callback the callback will be executed on the same thread, exactly once.
	 */
	<T> T put(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull MemorySegment @NotNull[] keys,
			@NotNull MemorySegment value,
			PutCallback<? super MemorySegment, T> callback) throws RocksDBException;

	/**
	 * Get an element from the specified position
	 * @param arena arena
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param callback the callback will be executed on the same thread, exactly once.
	 */
	<T> T get(Arena arena,
			long transactionId,
			long columnId,
			@NotNull MemorySegment @NotNull[] keys,
			GetCallback<? super MemorySegment, T> callback) throws RocksDBException;

	/**
	 * Open an iterator
	 * @param arena arena
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
	 * @param endKeysExclusive end keys, exclusive. Null means "the end"
	 * @param reverse if true, seek in reverse direction
	 * @param timeoutMs timeout in milliseconds
	 * @return iterator id
	 */
	long openIterator(Arena arena,
			long transactionId,
			long columnId,
			@NotNull MemorySegment @NotNull[] startKeysInclusive,
			@NotNull MemorySegment @Nullable[] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException;

	/**
	 * Close an iterator
	 * @param iteratorId iterator id
	 */
	void closeIterator(long iteratorId) throws RocksDBException;

	/**
	 * Seek to the specific element during an iteration, or the subsequent one if not found
	 * @param arena arena
	 * @param iterationId iteration id
	 * @param keys keys, inclusive. [] means "the beginning"
	 */
	void seekTo(Arena arena, long iterationId, @NotNull MemorySegment @NotNull[] keys) throws RocksDBException;

	/**
	 * Get the subsequent element during an iteration
	 * @param arena arena
	 * @param iterationId iteration id
	 * @param skipCount number of elements to skip
	 * @param takeCount number of elements to take
	 * @param callback the callback will be executed on the same thread, exactly once.
	 */
	<T> T subsequent(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull IteratorCallback<? super MemorySegment, T> callback) throws RocksDBException;
}
