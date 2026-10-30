package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Binary encoding of {@link LogEntry}. The format is versioned by a leading byte; there is one current version.
 * Must be deterministic: equal entries encode to equal bytes.
 *
 * <p>Layout: the version byte, then one tag byte for the entry type, then its fields in declaration order.
 * Numbers are big-endian and fixed width, as written by {@link java.io.DataOutputStream}. A byte string is an int
 * length followed by the bytes; a nullable value uses length -1 for null. A string is a byte string of UTF-8. A list
 * is an int count followed by the elements. A nullable non-byte-string field is preceded by a presence boolean.</p>
 *
 * <p>A {@link LogEntry.Guarded} ends with one flags byte, then its optional fields. Without an update id and a guard,
 * so on every entry of a cluster with pipelined updates off, that byte is what used to be a boolean, 0 or 1: such
 * entries, and logs written before the flags existed, keep their bytes, and the version did not change. A node that
 * does not know these flags fails on an entry that has them, so every node must run a version that does before the
 * option is turned on anywhere.</p>
 */
final class LogEntryCodec {

	static final byte VERSION = 1;

	private static final byte PUT = 1;
	private static final byte PUT_MULTI = 2;
	private static final byte DELETE = 3;
	private static final byte DELETE_MULTI = 4;
	private static final byte MERGE = 5;
	private static final byte MERGE_MULTI = 6;
	private static final byte DELETE_RANGE = 7;
	private static final byte CREATE_COLUMN = 8;
	private static final byte DELETE_COLUMN = 9;
	private static final byte DELETE_COLUMN_IF_EXISTS = 10;
	private static final byte UPLOAD_MERGE_OPERATOR = 11;
	private static final byte GUARDED = 12;
	private static final byte GENESIS = 13;
	private static final byte VERIFY_COLUMN = 14;

	/** Flags of a {@link LogEntry.Guarded}: {@link LogEntry.Guarded#singleResult()}. */
	private static final int GUARDED_SINGLE_RESULT = 1;
	/** Flags of a {@link LogEntry.Guarded}: a {@link LogEntry.Guarded#updateId()} follows; it is not 0. */
	private static final int GUARDED_UPDATE_ID = 1 << 1;
	/** Flags of a {@link LogEntry.Guarded}: a {@link LogEntry.Guard} follows. */
	private static final int GUARDED_GUARD = 1 << 2;
	private static final int GUARDED_FLAGS = GUARDED_SINGLE_RESULT | GUARDED_UPDATE_ID | GUARDED_GUARD;

	/**
	 * Request type of each code: the code is the index. Append only; never reorder. An explicit table instead of
	 * {@code ordinal()} so that reordering or inserting enum constants in core cannot change the log format.
	 */
	private static final RequestTypeId[] REQUEST_TYPES = {
			RequestTypeId.NOTHING,
			RequestTypeId.PREVIOUS,
			RequestTypeId.CURRENT,
			RequestTypeId.FOR_UPDATE,
			RequestTypeId.EXISTS,
			RequestTypeId.DELTA,
			RequestTypeId.MULTI,
			RequestTypeId.CHANGED,
			RequestTypeId.PREVIOUS_PRESENCE,
			RequestTypeId.MERGED,
			RequestTypeId.FIRST_AND_LAST,
			RequestTypeId.ALL_IN_RANGE,
			RequestTypeId.ENTRIES_COUNT,
			RequestTypeId.ALL_IN_RANGE_NO_CACHE,
			RequestTypeId.ENSURE
	};
	private static final Map<RequestTypeId, Byte> REQUEST_TYPE_CODES = new EnumMap<>(RequestTypeId.class);

	static {
		for (int code = 0; code < REQUEST_TYPES.length; code++) {
			REQUEST_TYPE_CODES.put(REQUEST_TYPES[code], (byte) code);
		}
	}

	/**
	 * Column hash types are written by ordinal. That is stable because {@code EmbeddedDB} already persists column
	 * schemas with these ordinals, so they cannot change without breaking every existing database.
	 */
	private static final ColumnHashType[] HASH_TYPES = ColumnHashType.values();

	private LogEntryCodec() {}

	static byte[] encode(LogEntry entry) {
		Objects.requireNonNull(entry, "entry");
		var out = new Output(1 + entrySize(entry));
		out.writeByte(VERSION);
		writeEntry(out, entry);
		return out.finish();
	}

	/**
	 * Leading byte of a batch: the payload of every Raft log entry is a batch of requests of one proposer, see
	 * {@link #encodeBatch}.
	 */
	static final byte BATCH_VERSION = 2;

	/** The body of {@code entry} (its tag and fields, no version byte), to be put in a batch by {@link #encodeBatch}. */
	static byte[] encodeEntryBody(LogEntry entry) {
		Objects.requireNonNull(entry, "entry");
		var out = new Output(entrySize(entry));
		writeEntry(out, entry);
		return out.finish();
	}

	/** Size of a batch header: {@link #BATCH_VERSION}, proposer id, {@code completedBelow} and request count. */
	private static final int BATCH_HEADER_BYTES = 1 + 2 * Long.BYTES + Integer.BYTES;

	/**
	 * Encodes requests of one proposer as one log entry: {@link #BATCH_VERSION}, the proposer id, the batch's
	 * {@code completedBelow} (see {@link Request}), the request count, then each request's sequence followed by its
	 * entry body ({@link #encodeEntryBody}).
	 */
	static byte[] encodeBatch(long proposerId, long completedBelow, long[] sequences, List<byte[]> entryBodies) {
		if (sequences.length != entryBodies.size() || sequences.length == 0) {
			throw new IllegalArgumentException("A batch needs one entry per sequence, and at least one");
		}
		long size = BATCH_HEADER_BYTES;
		for (byte[] body : entryBodies) {
			size += Long.BYTES + body.length;
		}
		var out = new Output(size);
		writeBatchHeader(out, proposerId, completedBelow, sequences.length);
		for (int i = 0; i < sequences.length; i++) {
			checkSequence(completedBelow, sequences[i]);
			byte[] body = entryBodies.get(i);
			out.writeLong(sequences[i]);
			out.write(body, 0, body.length);
		}
		return out.finish();
	}

	/**
	 * Encodes requests of one proposer as one log entry, see {@link #encodeBatch(long, long, long[], List)}. Each
	 * entry is written in place, with no body of its own.
	 */
	static byte[] encodeBatch(List<Request> requests) {
		var first = requests.getFirst();
		long size = BATCH_HEADER_BYTES;
		for (var request : requests) {
			if (request.proposerId() != first.proposerId() || request.completedBelow() != first.completedBelow()) {
				throw new IllegalArgumentException("A batch holds requests of one proposer, with one completedBelow");
			}
			checkSequence(first.completedBelow(), request.sequence());
			size += Long.BYTES + entrySize(Objects.requireNonNull(request.entry(), "entry"));
		}
		var out = new Output(size);
		writeBatchHeader(out, first.proposerId(), first.completedBelow(), requests.size());
		for (var request : requests) {
			out.writeLong(request.sequence());
			writeEntry(out, request.entry());
		}
		return out.finish();
	}

	private static void writeBatchHeader(Output out, long proposerId, long completedBelow, int count) {
		out.writeByte(BATCH_VERSION);
		out.writeLong(proposerId);
		out.writeLong(completedBelow);
		out.writeInt(count);
	}

	private static void checkSequence(long completedBelow, long sequence) {
		if (completedBelow > sequence) {
			throw new IllegalArgumentException("Sequence " + sequence + " is below completedBelow " + completedBelow);
		}
	}

	/** @throws IllegalArgumentException on malformed input or unknown version */
	static List<Request> decodeBatch(byte[] data) {
		return decodeBatch(new Input(data));
	}

	/**
	 * Decodes a batch read from {@code data}'s remaining bytes, such as a read-only view of a Raft log entry's
	 * payload: nothing of the whole batch is copied, only each field into its own array.
	 *
	 * @throws IllegalArgumentException on malformed input or unknown version
	 */
	static List<Request> decodeBatch(ByteBuffer data) {
		return decodeBatch(new Input(data));
	}

	private static List<Request> decodeBatch(Input in) {
		byte version = in.readByte();
		if (version != BATCH_VERSION) {
			throw new IllegalArgumentException("Unknown log entry version: " + version);
		}
		long proposerId = in.readLong();
		long completedBelow = in.readLong();
		// A request takes at least its sequence and an entry tag.
		int count = in.readCount(Long.BYTES + 1);
		if (count == 0) {
			throw new IllegalArgumentException("Empty batch");
		}
		var requests = new Request[count];
		for (int i = 0; i < count; i++) {
			long sequence = in.readLong();
			requests[i] = new Request(proposerId, sequence, completedBelow, readEntry(in));
		}
		in.finish();
		return List.of(requests);
	}

	/** @throws IllegalArgumentException on malformed input or unknown version */
	static LogEntry decode(byte[] data) {
		var in = new Input(data);
		byte version = in.readByte();
		if (version != VERSION) {
			throw new IllegalArgumentException("Unknown log entry version: " + version);
		}
		LogEntry entry = readEntry(in);
		in.finish();
		return entry;
	}

	private static void writeEntry(Output out, LogEntry entry) {
		switch (entry) {
			case LogEntry.Write write -> writeWrite(out, write);
			case LogEntry.CreateColumn(String name, ColumnSchema schema) -> {
				out.writeByte(CREATE_COLUMN);
				writeString(out, name);
				writeSchema(out, schema);
			}
			case LogEntry.DeleteColumn(long columnId) -> {
				out.writeByte(DELETE_COLUMN);
				out.writeLong(columnId);
			}
			case LogEntry.DeleteColumnIfExists(String name) -> {
				out.writeByte(DELETE_COLUMN_IF_EXISTS);
				writeString(out, name);
			}
			case LogEntry.UploadMergeOperator(String name, String className, byte[] jarData) -> {
				out.writeByte(UPLOAD_MERGE_OPERATOR);
				writeString(out, name);
				writeString(out, className);
				out.writeBytes(jarData);
			}
			case LogEntry.Guarded guarded -> writeGuarded(out, guarded);
			case LogEntry.VerifyColumn(String name, long columnId) -> {
				out.writeByte(VERIFY_COLUMN);
				writeString(out, name);
				out.writeLong(columnId);
			}
			case LogEntry.Genesis(String clusterId, boolean seeded) -> {
				out.writeByte(GENESIS);
				writeString(out, clusterId);
				out.writeBoolean(seeded);
			}
		}
	}

	private static void writeWrite(Output out, LogEntry.Write write) {
		switch (write) {
			case LogEntry.Put(long columnId, Keys keys, Buf value, RequestTypeId requestType) ->
					writeSingle(out, PUT, columnId, keys, value, requestType);
			case LogEntry.PutMulti(long columnId, List<Keys> keys, List<Buf> values, RequestTypeId requestType) ->
					writeMulti(out, PUT_MULTI, columnId, keys, values, requestType);
			case LogEntry.Delete(long columnId, Keys keys, RequestTypeId requestType) -> {
				out.writeByte(DELETE);
				out.writeLong(columnId);
				writeKeys(out, keys);
				writeRequestType(out, requestType);
			}
			case LogEntry.DeleteMulti(long columnId, List<Keys> keys, RequestTypeId requestType) -> {
				out.writeByte(DELETE_MULTI);
				out.writeLong(columnId);
				out.writeInt(keys.size());
				for (Keys key : keys) {
					writeKeys(out, key);
				}
				writeRequestType(out, requestType);
			}
			case LogEntry.Merge(long columnId, Keys keys, Buf value, RequestTypeId requestType) ->
					writeSingle(out, MERGE, columnId, keys, value, requestType);
			case LogEntry.MergeMulti(long columnId, List<Keys> keys, List<Buf> values, RequestTypeId requestType) ->
					writeMulti(out, MERGE_MULTI, columnId, keys, values, requestType);
			case LogEntry.DeleteRange(long columnId, Keys start, Keys end) -> {
				out.writeByte(DELETE_RANGE);
				out.writeLong(columnId);
				writeNullableKeys(out, start);
				writeNullableKeys(out, end);
			}
		}
	}

	private static void writeSingle(Output out,
			byte tag,
			long columnId,
			Keys keys,
			@Nullable Buf value,
			RequestTypeId requestType) {
		out.writeByte(tag);
		out.writeLong(columnId);
		writeKeys(out, keys);
		out.writeNullableBuf(value);
		writeRequestType(out, requestType);
	}

	/** Keys and values are written as pairs, so a decoded entry always has matching sizes. Values are non-null. */
	private static void writeMulti(Output out,
			byte tag,
			long columnId,
			List<Keys> keys,
			List<Buf> values,
			RequestTypeId requestType) {
		out.writeByte(tag);
		out.writeLong(columnId);
		out.writeInt(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			writeKeys(out, keys.get(i));
			out.writeBuf(values.get(i));
		}
		writeRequestType(out, requestType);
	}

	private static void writeGuarded(Output out, LogEntry.Guarded guarded) {
		out.writeByte(GUARDED);
		out.writeLong(guarded.leaderEpoch());
		out.writeLong(guarded.snapshotIndex());
		long[] columns = guarded.conflictColumns();
		long[] hashes = guarded.conflictHashes();
		out.writeInt(columns.length);
		for (int i = 0; i < columns.length; i++) {
			out.writeLong(columns[i]);
			out.writeLong(hashes[i]);
		}
		out.writeInt(guarded.writes().size());
		for (LogEntry.Write write : guarded.writes()) {
			writeWrite(out, write);
		}
		long updateId = guarded.updateId();
		var guard = guarded.guard();
		out.writeByte((guarded.singleResult() ? GUARDED_SINGLE_RESULT : 0)
				| (updateId != 0 ? GUARDED_UPDATE_ID : 0)
				| (guard != null ? GUARDED_GUARD : 0));
		if (updateId != 0) {
			out.writeLong(updateId);
		}
		if (guard != null) {
			out.writeLong(guard.columnId());
			writeKeys(out, guard.keys());
			out.writeInt(guard.length());
			if (guard.sha256() != null) {
				out.write(guard.sha256(), 0, LogEntry.Guard.DIGEST_BYTES);
			}
		}
	}

	/** Encoded size of a {@link LogEntry.Guarded} body without conflict keys, writes, update id and guard. */
	static final int GUARDED_BASE_BYTES = 1 + 2 * Long.BYTES + 2 * Integer.BYTES + 1;
	/** Encoded size of one conflict key of a {@link LogEntry.Guarded}: its column id and key hash. */
	static final int CONFLICT_KEY_BYTES = 2 * Long.BYTES;

	/**
	 * Encoded size of {@code entry}'s body as {@link #writeEntry} writes it, computed without encoding it.
	 *
	 * @throws IllegalArgumentException if a string of it is not valid Unicode
	 */
	static long entrySize(LogEntry entry) {
		return switch (entry) {
			case LogEntry.Write write -> writeSize(write);
			case LogEntry.CreateColumn(String name, ColumnSchema schema) -> 1 + stringSize(name) + schemaSize(schema);
			case LogEntry.DeleteColumn _ -> 1 + Long.BYTES;
			case LogEntry.DeleteColumnIfExists(String name) -> 1 + stringSize(name);
			case LogEntry.UploadMergeOperator(String name, String className, byte[] jarData) ->
					1 + stringSize(name) + stringSize(className) + Integer.BYTES + jarData.length;
			case LogEntry.Guarded guarded -> {
				long size = GUARDED_BASE_BYTES + (long) guarded.conflictColumns().length * CONFLICT_KEY_BYTES;
				for (LogEntry.Write write : guarded.writes()) {
					size += writeSize(write);
				}
				if (guarded.updateId() != 0) {
					size += Long.BYTES;
				}
				if (guarded.guard() != null) {
					size += guardSize(guarded.guard());
				}
				yield size;
			}
			case LogEntry.VerifyColumn(String name, long _) -> 1 + stringSize(name) + Long.BYTES;
			case LogEntry.Genesis(String clusterId, boolean _) -> 1 + stringSize(clusterId) + 1;
		};
	}

	/** Encoded size of the fields {@code guard} adds to a {@link LogEntry.Guarded}. */
	static long guardSize(LogEntry.Guard guard) {
		return Long.BYTES + keysSize(guard.keys()) + Integer.BYTES
				+ (guard.sha256() != null ? LogEntry.Guard.DIGEST_BYTES : 0);
	}

	/** Encoded size of {@code write} as {@link #writeWrite} writes it, inside a {@link LogEntry.Guarded}. */
	static long writeSize(LogEntry.Write write) {
		// Tag, column id and, except for deleteRange, the request type.
		long size = 1 + Long.BYTES;
		switch (write) {
			case LogEntry.Put(_, Keys keys, Buf value, _) -> size += keysSize(keys) + nullableBufSize(value) + 1;
			case LogEntry.Merge(_, Keys keys, Buf value, _) -> size += keysSize(keys) + nullableBufSize(value) + 1;
			case LogEntry.Delete(_, Keys keys, _) -> size += keysSize(keys) + 1;
			case LogEntry.PutMulti(_, List<Keys> keys, List<Buf> values, _) -> size += multiSize(keys, values) + 1;
			case LogEntry.MergeMulti(_, List<Keys> keys, List<Buf> values, _) -> size += multiSize(keys, values) + 1;
			case LogEntry.DeleteMulti(_, List<Keys> keys, _) -> {
				size += Integer.BYTES + 1;
				for (Keys key : keys) {
					size += keysSize(key);
				}
			}
			case LogEntry.DeleteRange(_, Keys start, Keys end) ->
					size += 2 + (start != null ? keysSize(start) : 0) + (end != null ? keysSize(end) : 0);
		}
		return size;
	}

	private static long multiSize(List<Keys> keys, List<Buf> values) {
		long size = Integer.BYTES;
		for (int i = 0; i < keys.size(); i++) {
			size += keysSize(keys.get(i)) + Integer.BYTES + values.get(i).size();
		}
		return size;
	}

	private static long keysSize(Keys keys) {
		Buf[] segments = keys.keys();
		long size = Integer.BYTES;
		for (Buf segment : segments) {
			size += Integer.BYTES + segment.size();
		}
		return size;
	}

	private static long nullableBufSize(@Nullable Buf buf) {
		return Integer.BYTES + (buf != null ? buf.size() : 0);
	}

	private static long schemaSize(ColumnSchema schema) {
		return Integer.BYTES + (long) schema.keys().size() * Integer.BYTES
				+ Integer.BYTES + schema.variableTailKeys().size()
				+ 1
				+ nullableStringSize(schema.mergeOperatorName())
				+ 1 + (schema.mergeOperatorVersion() != null ? Long.BYTES : 0)
				+ nullableStringSize(schema.mergeOperatorClass());
	}

	/** Strings are rare (column and operator names): they are encoded once to be measured, then again to be written. */
	private static long stringSize(String value) {
		return Integer.BYTES + utf8(value).length;
	}

	private static long nullableStringSize(@Nullable String value) {
		return 1 + (value != null ? stringSize(value) : 0);
	}

	private static void writeKeys(Output out, Keys keys) {
		Buf[] segments = keys.keys();
		out.writeInt(segments.length);
		for (Buf segment : segments) {
			out.writeBuf(segment);
		}
	}

	private static void writeNullableKeys(Output out, @Nullable Keys keys) {
		out.writeBoolean(keys != null);
		if (keys != null) {
			writeKeys(out, keys);
		}
	}

	private static void writeRequestType(Output out, RequestTypeId requestType) {
		Byte code = REQUEST_TYPE_CODES.get(Objects.requireNonNull(requestType, "requestType"));
		if (code == null) {
			throw new IllegalArgumentException("Request type has no log code: " + requestType);
		}
		out.writeByte(code);
	}

	private static void writeSchema(Output out, ColumnSchema schema) {
		out.writeInt(schema.keys().size());
		for (int i = 0; i < schema.keys().size(); i++) {
			out.writeInt(schema.keys().getInt(i));
		}
		out.writeInt(schema.variableTailKeys().size());
		for (ColumnHashType hashType : schema.variableTailKeys()) {
			out.writeByte(hashType.ordinal());
		}
		out.writeBoolean(schema.hasValue());
		writeNullableString(out, schema.mergeOperatorName());
		out.writeBoolean(schema.mergeOperatorVersion() != null);
		if (schema.mergeOperatorVersion() != null) {
			out.writeLong(schema.mergeOperatorVersion());
		}
		writeNullableString(out, schema.mergeOperatorClass());
	}

	/** Strict UTF-8: a string with unpaired surrogates is rejected instead of silently altered. */
	static byte[] utf8(String value) {
		ByteBuffer encoded;
		try {
			encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(value));
		} catch (CharacterCodingException e) {
			throw new IllegalArgumentException("String is not valid Unicode", e);
		}
		return Arrays.copyOfRange(encoded.array(), encoded.arrayOffset() + encoded.position(),
				encoded.arrayOffset() + encoded.limit());
	}

	static void writeString(Output out, String value) {
		out.writeBytes(utf8(value));
	}

	private static void writeNullableString(Output out, @Nullable String value) {
		out.writeBoolean(value != null);
		if (value != null) {
			writeString(out, value);
		}
	}

	private static LogEntry readEntry(Input in) {
		byte tag = in.readByte();
		return switch (tag) {
			case CREATE_COLUMN -> new LogEntry.CreateColumn(in.readString(), readSchema(in));
			case DELETE_COLUMN -> new LogEntry.DeleteColumn(in.readLong());
			case DELETE_COLUMN_IF_EXISTS -> new LogEntry.DeleteColumnIfExists(in.readString());
			case UPLOAD_MERGE_OPERATOR ->
					new LogEntry.UploadMergeOperator(in.readString(), in.readString(), in.readBytes());
			case GUARDED -> readGuarded(in);
			case GENESIS -> new LogEntry.Genesis(in.readString(), in.readBoolean());
			case VERIFY_COLUMN -> new LogEntry.VerifyColumn(in.readString(), in.readLong());
			default -> readWrite(in, tag);
		};
	}

	private static LogEntry.Write readWrite(Input in, byte tag) {
		return switch (tag) {
			case PUT -> new LogEntry.Put(in.readLong(), readKeys(in), in.readNullableBuf(), readRequestType(in));
			case PUT_MULTI -> {
				long columnId = in.readLong();
				var keys = new ArrayList<Keys>();
				var values = new ArrayList<Buf>();
				readPairs(in, keys, values);
				yield new LogEntry.PutMulti(columnId, keys, values, readRequestType(in));
			}
			case DELETE -> new LogEntry.Delete(in.readLong(), readKeys(in), readRequestType(in));
			case DELETE_MULTI -> {
				long columnId = in.readLong();
				int count = in.readCount(Integer.BYTES);
				var keys = new ArrayList<Keys>();
				for (int i = 0; i < count; i++) {
					keys.add(readKeys(in));
				}
				yield new LogEntry.DeleteMulti(columnId, keys, readRequestType(in));
			}
			case MERGE -> new LogEntry.Merge(in.readLong(), readKeys(in), in.readNullableBuf(), readRequestType(in));
			case MERGE_MULTI -> {
				long columnId = in.readLong();
				var keys = new ArrayList<Keys>();
				var values = new ArrayList<Buf>();
				readPairs(in, keys, values);
				yield new LogEntry.MergeMulti(columnId, keys, values, readRequestType(in));
			}
			case DELETE_RANGE -> new LogEntry.DeleteRange(in.readLong(), readNullableKeys(in), readNullableKeys(in));
			default -> throw new IllegalArgumentException("Unknown log entry tag: " + tag);
		};
	}

	private static void readPairs(Input in, List<Keys> keys, List<Buf> values) {
		int count = in.readCount(2 * Integer.BYTES);
		for (int i = 0; i < count; i++) {
			keys.add(readKeys(in));
			values.add(in.readBuf());
		}
	}

	private static LogEntry.Guarded readGuarded(Input in) {
		long leaderEpoch = in.readLong();
		long snapshotIndex = in.readLong();
		int conflictCount = in.readCount(2 * Long.BYTES);
		long[] columns = new long[conflictCount];
		long[] hashes = new long[conflictCount];
		for (int i = 0; i < conflictCount; i++) {
			columns[i] = in.readLong();
			hashes[i] = in.readLong();
		}
		int writeCount = in.readCount(1);
		// Not presized from the untrusted count, nor is the key list of a nested write: together they could
		// reserve several times the input size before a truncation is noticed.
		var writes = new ArrayList<LogEntry.Write>();
		for (int i = 0; i < writeCount; i++) {
			writes.add(readWrite(in, in.readByte()));
		}
		int flags = in.readByte() & 0xFF;
		if ((flags & ~GUARDED_FLAGS) != 0) {
			throw new IllegalArgumentException("Unknown guarded entry flags: " + flags + ", from a newer version?");
		}
		long updateId = 0;
		if ((flags & GUARDED_UPDATE_ID) != 0) {
			updateId = in.readLong();
			if (updateId == 0) {
				// Equal entries encode to equal bytes: 0 has only one encoding.
				throw new IllegalArgumentException("Explicit update id 0");
			}
		}
		boolean singleResult = (flags & GUARDED_SINGLE_RESULT) != 0;
		var guard = (flags & GUARDED_GUARD) != 0 ? readGuard(in) : null;
		// The constructor rejects inconsistent combinations with IllegalArgumentException
		return new LogEntry.Guarded(leaderEpoch, snapshotIndex, columns, hashes, writes, singleResult, updateId, guard);
	}

	private static LogEntry.Guard readGuard(Input in) {
		long columnId = in.readLong();
		var keys = readKeys(in);
		int length = in.readInt();
		return new LogEntry.Guard(columnId, keys, length, length >= 0 ? in.readFixed(LogEntry.Guard.DIGEST_BYTES) : null);
	}

	private static Keys readKeys(Input in) {
		int count = in.readCount(Integer.BYTES);
		var segments = new Buf[count];
		for (int i = 0; i < count; i++) {
			segments[i] = in.readBuf();
		}
		return new Keys(segments);
	}

	private static @Nullable Keys readNullableKeys(Input in) {
		return in.readBoolean() ? readKeys(in) : null;
	}

	private static RequestTypeId readRequestType(Input in) {
		int code = in.readByte() & 0xFF;
		if (code >= REQUEST_TYPES.length) {
			throw new IllegalArgumentException("Unknown request type code: " + code);
		}
		return REQUEST_TYPES[code];
	}

	private static ColumnSchema readSchema(Input in) {
		int keyCount = in.readCount(Integer.BYTES);
		var keys = new IntArrayList(keyCount);
		for (int i = 0; i < keyCount; i++) {
			keys.add(in.readInt());
		}
		int hashTypeCount = in.readCount(1);
		var hashTypes = new ObjectArrayList<ColumnHashType>();
		for (int i = 0; i < hashTypeCount; i++) {
			int ordinal = in.readByte() & 0xFF;
			if (ordinal >= HASH_TYPES.length) {
				throw new IllegalArgumentException("Unknown column hash type: " + ordinal);
			}
			hashTypes.add(HASH_TYPES[ordinal]);
		}
		boolean hasValue = in.readBoolean();
		String mergeOperatorName = in.readBoolean() ? in.readString() : null;
		Long mergeOperatorVersion = in.readBoolean() ? in.readLong() : null;
		String mergeOperatorClass = in.readBoolean() ? in.readString() : null;
		try {
			return new ColumnSchema(keys,
					hashTypes,
					hasValue,
					mergeOperatorName,
					mergeOperatorVersion,
					mergeOperatorClass
			);
		} catch (RocksDBException e) {
			throw new IllegalArgumentException("Invalid column schema: " + e.getMessage(), e);
		}
	}

	/**
	 * Writer of an encoding into an array of exactly its size, computed beforehand, so that nothing is copied but
	 * the fields themselves. Numbers are big-endian and fixed width, the bytes {@link java.io.DataOutputStream}
	 * would write.
	 */
	static final class Output {

		private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
		private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
		/** Largest array length every JVM allocates. */
		private static final int MAX_LENGTH = Integer.MAX_VALUE - 8;

		private final byte[] array;
		private final MemorySegment segment;
		private int position;

		/** @throws IllegalArgumentException if {@code size} is too large for an array */
		Output(long size) {
			if (size > MAX_LENGTH) {
				throw new IllegalArgumentException("Encoding too large: " + size + " bytes");
			}
			this.array = new byte[(int) size];
			this.segment = MemorySegment.ofArray(array);
		}

		void writeByte(int value) {
			array[position] = (byte) value;
			position++;
		}

		void writeBoolean(boolean value) {
			writeByte(value ? 1 : 0);
		}

		void writeInt(int value) {
			segment.set(INT, position, value);
			position += Integer.BYTES;
		}

		void writeLong(long value) {
			segment.set(LONG, position, value);
			position += Long.BYTES;
		}

		void write(byte[] bytes, int offset, int length) {
			System.arraycopy(bytes, offset, array, position, length);
			position += length;
		}

		/** A byte string: the length, then the bytes. */
		void writeBytes(byte[] bytes) {
			writeInt(bytes.length);
			write(bytes, 0, bytes.length);
		}

		/** A byte string, copied straight from the buffer's backing array or segment. */
		void writeBuf(Buf buf) {
			int size = buf.size();
			writeInt(size);
			byte[] backing = buf.getBackingByteArrayStrict();
			if (backing != null) {
				System.arraycopy(backing, buf.getBackingByteArrayOffset(), array, position, size);
			} else {
				MemorySegment.copy(buf.asMemorySegment(), 0, segment, position, size);
			}
			position += size;
		}

		void writeNullableBuf(@Nullable Buf buf) {
			if (buf == null) {
				writeInt(-1);
			} else {
				writeBuf(buf);
			}
		}

		/** The encoding. @throws IllegalStateException if fewer bytes were written than the computed size */
		byte[] finish() {
			if (position != array.length) {
				throw new IllegalStateException("Wrote " + position + " bytes, computed " + array.length);
			}
			return array;
		}
	}

	/** Bounds-checked reader of an encoding: every malformation is reported as {@link IllegalArgumentException}. */
	static final class Input {

		private final ByteBuffer buffer;

		Input(byte[] data) {
			this.buffer = ByteBuffer.wrap(Objects.requireNonNull(data, "data"));
		}

		/** Reads the remaining bytes of {@code data}, which may be a read-only view; {@code data} is not moved. */
		Input(ByteBuffer data) {
			this.buffer = data.slice().order(ByteOrder.BIG_ENDIAN);
		}

		private void require(int length) {
			if (buffer.remaining() < length) {
				throw new IllegalArgumentException("Truncated input at byte " + buffer.position());
			}
		}

		byte readByte() {
			require(1);
			return buffer.get();
		}

		boolean readBoolean() {
			byte value = readByte();
			return switch (value) {
				case 0 -> false;
				case 1 -> true;
				default -> throw new IllegalArgumentException("Invalid boolean: " + value);
			};
		}

		int readInt() {
			require(Integer.BYTES);
			return buffer.getInt();
		}

		long readLong() {
			require(Long.BYTES);
			return buffer.getLong();
		}

		/**
		 * Reads a list size. Each element takes at least {@code minElementBytes}, so a size that cannot fit in the
		 * remaining input is rejected before anything is allocated for it.
		 */
		int readCount(int minElementBytes) {
			int count = readInt();
			if (count < 0 || (long) count * minElementBytes > buffer.remaining()) {
				throw new IllegalArgumentException("Invalid size " + count + " at byte " + (buffer.position() - 4));
			}
			return count;
		}

		byte[] readBytes() {
			return readBytes(readCount(1));
		}

		/** Exactly {@code length} bytes, with no length before them. */
		byte[] readFixed(int length) {
			require(length);
			return readBytes(length);
		}

		/**
		 * A reader of the next byte string, which it skips: its bytes are read in place, without being copied first.
		 */
		Input readNested() {
			int length = readCount(1);
			var nested = new Input(buffer.slice(buffer.position(), length));
			buffer.position(buffer.position() + length);
			return nested;
		}

		Buf readBuf() {
			return Buf.wrap(readBytes());
		}

		@Nullable Buf readNullableBuf() {
			int length = readInt();
			if (length == -1) {
				return null;
			}
			if (length < 0 || length > buffer.remaining()) {
				throw new IllegalArgumentException("Invalid length " + length + " at byte " + (buffer.position() - 4));
			}
			return Buf.wrap(readBytes(length));
		}

		private byte[] readBytes(int length) {
			var bytes = new byte[length];
			buffer.get(bytes);
			return bytes;
		}

		String readString() {
			try {
				return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(readBytes())).toString();
			} catch (CharacterCodingException e) {
				throw new IllegalArgumentException("Invalid UTF-8 string", e);
			}
		}

		void finish() {
			if (buffer.hasRemaining()) {
				throw new IllegalArgumentException(buffer.remaining() + " trailing bytes");
			}
		}
	}
}
