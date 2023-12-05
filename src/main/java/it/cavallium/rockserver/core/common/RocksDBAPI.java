package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import java.lang.foreign.MemorySegment;
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
	 * @param transactionId transaction id to close
	 */
	void closeTransaction(long transactionId) throws RocksDBException;

	/**
	 * Create a column
	 * @param name   column name
	 * @param schema column key-value schema
	 * @return column id
	 */
	long createColumn(String name, ColumnSchema schema) throws RocksDBException;

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
	long getColumnId(String name) throws RocksDBException;

	/**
	 * Put an element into the specified position
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param value value, or null if not needed
	 * @param callback the callback will be executed on the same thread, exactly once.
	 */
	void put(long transactionId,
			long columnId,
			MemorySegment[] keys,
			@Nullable MemorySegment value,
			PutCallback<? super MemorySegment> callback) throws RocksDBException;

	/**
	 * Get an element from the specified position
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param callback the callback will be executed on the same thread, exactly once.
	 */
	void get(long transactionId,
			long columnId,
			MemorySegment[] keys,
			GetCallback<? super MemorySegment> callback) throws RocksDBException;

	/**
	 * Open an iterator
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
	 * @param endKeysExclusive end keys, exclusive. Null means "the end"
	 * @param reverse if true, seek in reverse direction
	 * @param timeoutMs timeout in milliseconds
	 * @return iterator id
	 */
	long openIterator(long transactionId,
			long columnId,
			MemorySegment[] startKeysInclusive,
			@Nullable MemorySegment[] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException;

	/**
	 * Close an iterator
	 * @param iteratorId iterator id
	 */
	void closeIterator(long iteratorId) throws RocksDBException;

	/**
	 * Seek to the specific element during an iteration, or the subsequent one if not found
	 * @param iterationId iteration id
	 * @param keys keys, inclusive. [] means "the beginning"
	 */
	void seekTo(long iterationId, MemorySegment[] keys) throws RocksDBException;

	/**
	 * Get the subsequent element during an iteration
	 * @param iterationId iteration id
	 * @param skipCount number of elements to skip
	 * @param takeCount number of elements to take
	 * @param callback the callback will be executed on the same thread, exactly once.
	 */
	void subsequent(long iterationId,
			long skipCount,
			long takeCount,
			IteratorCallback<? super MemorySegment> callback) throws RocksDBException;
}
