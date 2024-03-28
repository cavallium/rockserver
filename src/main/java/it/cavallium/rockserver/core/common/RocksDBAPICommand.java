package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestIterate;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public sealed interface RocksDBAPICommand<R> {

	enum CommandTypeId {
		OPEN_TX,
		CLOSE_TX,
		CLOSE_FAILED_UPDATE,
		CREATE_COLUMN,
		DELETE_COLUMN,
		GET_COLUMN_ID,
		PUT,
		GET,
		OPEN_ITERATOR,
		CLOSE_ITERATOR,
		SEEK_TO,
		SUBSEQUENT
	}

	R handleSync(RocksDBSyncAPI api);
	CompletionStage<R> handleAsync(RocksDBAsyncAPI api);
	void serializeToBuffer(BufDataOutput out);
	CommandTypeId getCommandTypeId();

	static <T> RocksDBAPICommand<T> deserializeCommand(Arena arena, BufDataInput in) {
		return (RocksDBAPICommand<T>) switch (CommandTypeId.values()[in.readUnsignedByte()]) {
			case OPEN_TX -> new OpenTransaction(in.readLong());
			case CLOSE_TX -> new CloseTransaction(in.readLong(), in.readBoolean());
			case CLOSE_FAILED_UPDATE -> new CloseFailedUpdate(in.readLong());
			case CREATE_COLUMN -> {
				var name = in.readShortText(StandardCharsets.UTF_8);

				var keys = new int[in.readInt()];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = in.readInt();
				}
				ColumnHashType[] variableTailKeys = new ColumnHashType[in.readInt()];
				for (int i = 0; i < variableTailKeys.length; i++) {
					variableTailKeys[i] = ColumnHashType.values()[in.readUnsignedByte()];
				}
				boolean hasValue = in.readBoolean();
				var columnSchema = new ColumnSchema(IntArrayList.wrap(keys),
						ObjectArrayList.wrap(variableTailKeys),
						hasValue
				);
				yield new CreateColumn(name, columnSchema);
			}
			case DELETE_COLUMN -> new DeleteColumn(in.readLong());
			case GET_COLUMN_ID -> new GetColumnId(in.readShortText(StandardCharsets.UTF_8));
			case PUT -> {
				var transactionOrUpdateId = in.readLong();
				var columnId = in.readLong();
				var keys = SerializationUtils.deserializeMemorySegmentArray(in);
				var value = SerializationUtils.deserializeMemorySegment(in);
				//noinspection unchecked
				var requestType = (RequestPut<? super MemorySegment, ?>)
						RequestTypeId.values()[in.readUnsignedByte()].getRequestType();
				yield new Put<>(arena, transactionOrUpdateId, columnId, keys, value, requestType);
			}
			case GET -> {
				var transactionOrUpdateId = in.readLong();
				var columnId = in.readLong();
				var keys = SerializationUtils.deserializeMemorySegmentArray(in);
				//noinspection unchecked
				var requestType = (RequestGet<? super MemorySegment, ?>)
						RequestTypeId.values()[in.readUnsignedByte()].getRequestType();
				yield new Get<>(arena, transactionOrUpdateId, columnId, keys, requestType);
			}
			case OPEN_ITERATOR -> {
				var transactionId = in.readLong();
				var columnId = in.readLong();
				var startKeysInclusive = SerializationUtils.deserializeMemorySegmentArray(in);
				var endKeysExclusive = SerializationUtils.deserializeNullableMemorySegmentArray(in);
				var reverse = in.readBoolean();
				var timeoutMs = in.readLong();
				yield new OpenIterator(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
			}
			case CLOSE_ITERATOR -> new CloseIterator(in.readLong());
			case SEEK_TO -> {
				var iterationId = in.readLong();
				var keys = SerializationUtils.deserializeMemorySegmentArray(in);
				yield new SeekTo(arena, iterationId, keys);
			}
			case SUBSEQUENT -> {
				var iterationId = in.readLong();
				var skipCount = in.readLong();
				var takeCount = in.readLong();
				//noinspection unchecked
				var requestType = (RequestIterate<? super MemorySegment, ?>)
						RequestTypeId.values()[in.readUnsignedByte()].getRequestType();
				yield new Subsequent<>(arena, iterationId, skipCount, takeCount, requestType);
			}
		};
	}

	/**
	 * Open a transaction
	 * @param timeoutMs timeout in milliseconds
	 * @return transaction id
	 */
	record OpenTransaction(long timeoutMs) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.openTransaction(timeoutMs);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.openTransactionAsync(timeoutMs);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(timeoutMs);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.OPEN_TX;
		}
	}
	/**
	 * Close a transaction
	 *
	 * @param transactionId transaction id to close
	 * @param commit true to commit the transaction, false to rollback it
	 * @return true if committed, if false, you should try again
	 */
	record CloseTransaction(long transactionId, boolean commit) implements RocksDBAPICommand<Boolean> {

		@Override
		public Boolean handleSync(RocksDBSyncAPI api) {
			return api.closeTransaction(transactionId, commit);
		}

		@Override
		public CompletionStage<Boolean> handleAsync(RocksDBAsyncAPI api) {
			return api.closeTransactionAsync(transactionId, commit);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(transactionId);
			out.writeBoolean(commit);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.CLOSE_TX;
		}
	}
	/**
	 * Close a failed update, discarding all changes
	 *
	 * @param updateId update id to close
	 */
	record CloseFailedUpdate(long updateId) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.closeFailedUpdate(updateId);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.closeFailedUpdateAsync(updateId);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(updateId);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.CLOSE_FAILED_UPDATE;
		}
	}
	/**
	 * Create a column
	 * @param name   column name
	 * @param schema column key-value schema
	 * @return column id
	 */
	record CreateColumn(String name, @NotNull ColumnSchema schema) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.createColumn(name, schema);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.createColumnAsync(name, schema);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeShortText(name, StandardCharsets.UTF_8);
			var keys = schema.keys();
			out.writeInt(keys.size());
			keys.forEach(out::writeInt);
			var variableTailKeys = schema.variableTailKeys();
			out.writeInt(variableTailKeys.size());
			for (ColumnHashType variableTailKey : variableTailKeys) {
				out.writeByte(variableTailKey.ordinal());
			}
			out.writeBoolean(schema.hasValue());
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.CREATE_COLUMN;
		}
	}
	/**
	 * Delete a column
	 * @param columnId column id
	 */
	record DeleteColumn(long columnId) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.deleteColumn(columnId);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.deleteColumnAsync(columnId);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(columnId);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.DELETE_COLUMN;
		}
	}
	/**
	 * Get column id by name
	 * @param name column name
	 * @return column id
	 */
	record GetColumnId(@NotNull String name) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.getColumnId(name);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.getColumnIdAsync(name);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeShortText(name, StandardCharsets.UTF_8);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.GET_COLUMN_ID;
		}
	}
	/**
	 * Put an element into the specified position
	 * @param arena arena
	 * @param transactionOrUpdateId transaction id, update id, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param value value, or null if not needed
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record Put<T>(Arena arena,
								long transactionOrUpdateId,
								long columnId,
								@NotNull MemorySegment @NotNull [] keys,
								@NotNull MemorySegment value,
								RequestPut<? super MemorySegment, T> requestType) implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.put(arena, transactionOrUpdateId, columnId, keys, value, requestType);
		}

		@Override
		public CompletionStage<T> handleAsync(RocksDBAsyncAPI api) {
			return api.putAsync(arena, transactionOrUpdateId, columnId, keys, value, requestType);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(transactionOrUpdateId);
			out.writeLong(columnId);
			out.writeInt(keys.length);
			for (MemorySegment key : keys) {
				var array = key.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
				out.writeInt(array.length);
				out.writeBytes(array, 0, array.length);
			}
			var valueArray = value.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
			out.writeInt(valueArray.length);
			out.writeBytes(valueArray, 0, valueArray.length);
			out.writeByte(requestType.getRequestTypeId().ordinal());
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.PUT;
		}
	}
	/**
	 * Get an element from the specified position
	 * @param arena arena
	 * @param transactionOrUpdateId transaction id, update id for retry operations, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record Get<T>(Arena arena,
								long transactionOrUpdateId,
								long columnId,
								@NotNull MemorySegment @NotNull [] keys,
								RequestGet<? super MemorySegment, T> requestType) implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.get(arena, transactionOrUpdateId, columnId, keys, requestType);
		}

		@Override
		public CompletionStage<T> handleAsync(RocksDBAsyncAPI api) {
			return api.getAsync(arena, transactionOrUpdateId, columnId, keys, requestType);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(transactionOrUpdateId);
			out.writeLong(columnId);
			out.writeInt(keys.length);
			for (MemorySegment key : keys) {
				var array = key.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
				out.writeInt(array.length);
				out.writeBytes(array, 0, array.length);
			}
			out.writeByte(requestType.getRequestTypeId().ordinal());
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.GET;
		}
	}
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
	record OpenIterator(Arena arena,
											long transactionId,
											long columnId,
											@NotNull MemorySegment @NotNull [] startKeysInclusive,
											@NotNull MemorySegment @Nullable [] endKeysExclusive,
											boolean reverse,
											long timeoutMs) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.openIterator(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.openIteratorAsync(arena,
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					timeoutMs
			);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(transactionId);
			out.writeLong(columnId);
			out.writeInt(startKeysInclusive.length);
			SerializationUtils.serializeMemorySegmentArray(out, startKeysInclusive);
			for (MemorySegment key : startKeysInclusive) {
				var array = key.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
				out.writeInt(array.length);
				out.writeBytes(array, 0, array.length);
			}
			out.writeInt(endKeysExclusive == null ? -1 : endKeysExclusive.length);
			if (endKeysExclusive != null) {
				for (MemorySegment key : endKeysExclusive) {
					var array = key.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
					out.writeInt(array.length);
					out.writeBytes(array, 0, array.length);
				}
			}
			out.writeBoolean(reverse);
			out.writeLong(timeoutMs);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.OPEN_ITERATOR;
		}
	}
	/**
	 * Close an iterator
	 * @param iteratorId iterator id
	 */
	record CloseIterator(long iteratorId) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.closeIterator(iteratorId);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.closeIteratorAsync(iteratorId);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(iteratorId);
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.CLOSE_ITERATOR;
		}
	}
	/**
	 * Seek to the specific element during an iteration, or the subsequent one if not found
	 * @param arena arena
	 * @param iterationId iteration id
	 * @param keys keys, inclusive. [] means "the beginning"
	 */
	record SeekTo(Arena arena, long iterationId, @NotNull MemorySegment @NotNull [] keys) implements
			RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.seekTo(arena, iterationId, keys);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.seekToAsync(arena, iterationId, keys);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(iterationId);
			out.writeInt(keys.length);
			for (MemorySegment key : keys) {
				var array = key.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
				out.writeInt(array.length);
				out.writeBytes(array, 0, array.length);
			}
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.SEEK_TO;
		}
	}
	/**
	 * Get the subsequent element during an iteration
	 * @param arena arena
	 * @param iterationId iteration id
	 * @param skipCount number of elements to skip
	 * @param takeCount number of elements to take
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record Subsequent<T>(Arena arena,
										long iterationId,
										long skipCount,
										long takeCount,
										@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType)
			implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.subsequent(arena, iterationId, skipCount, takeCount, requestType);
		}

		@Override
		public CompletionStage<T> handleAsync(RocksDBAsyncAPI api) {
			return api.subsequentAsync(arena, iterationId, skipCount, takeCount, requestType);
		}

		@Override
		public void serializeToBuffer(BufDataOutput out) {
			out.writeLong(iterationId);
			out.writeLong(skipCount);
			out.writeLong(takeCount);
			out.writeByte(requestType.getRequestTypeId().ordinal());
		}

		@Override
		public CommandTypeId getCommandTypeId() {
			return CommandTypeId.SUBSEQUENT;
		}
	}
}
