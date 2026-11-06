package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
 * Numbers are big-endian and fixed width, as written by {@link DataOutputStream}. A byte string is an int length
 * followed by the bytes; a nullable value uses length -1 for null. A string is a byte string of UTF-8. A list is
 * an int count followed by the elements. A nullable non-byte-string field is preceded by a presence boolean.</p>
 *
 * <p>Frozen copy of the stream-based codec that the exact-size one replaced, kept as the reference of the
 * wire format for {@link CodecEquivalenceTest} and {@link CodecBench}. Never change it.</p>
 */
final class OldLogEntryCodec {

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

	private OldLogEntryCodec() {}

	static byte[] encode(LogEntry entry) {
		Objects.requireNonNull(entry, "entry");
		var bytes = new ByteArrayOutputStream();
		try (var out = new DataOutputStream(bytes)) {
			out.writeByte(VERSION);
			writeEntry(out, entry);
		} catch (IOException e) {
			// ByteArrayOutputStream never throws
			throw new UncheckedIOException(e);
		}
		return bytes.toByteArray();
	}

	/**
	 * Leading byte of a batch: the payload of every Raft log entry is a batch of requests of one proposer, see
	 * {@link #encodeBatch}.
	 */
	static final byte BATCH_VERSION = 2;

	/** The body of {@code entry} (its tag and fields, no version byte), to be put in a batch by {@link #encodeBatch}. */
	static byte[] encodeEntryBody(LogEntry entry) {
		Objects.requireNonNull(entry, "entry");
		var bytes = new ByteArrayOutputStream(64);
		try (var out = new DataOutputStream(bytes)) {
			writeEntry(out, entry);
		} catch (IOException e) {
			// ByteArrayOutputStream never throws
			throw new UncheckedIOException(e);
		}
		return bytes.toByteArray();
	}

	/**
	 * Encodes requests of one proposer as one log entry: {@link #BATCH_VERSION}, the proposer id, the batch's
	 * {@code completedBelow} (see {@link Request}), the request count, then each request's sequence followed by its
	 * entry body ({@link #encodeEntryBody}).
	 */
	static byte[] encodeBatch(long proposerId, long completedBelow, long[] sequences, List<byte[]> entryBodies) {
		if (sequences.length != entryBodies.size() || sequences.length == 0) {
			throw new IllegalArgumentException("A batch needs one entry per sequence, and at least one");
		}
		int size = 1 + 2 * Long.BYTES + Integer.BYTES;
		for (byte[] body : entryBodies) {
			size = Math.addExact(size, Long.BYTES + body.length);
		}
		var buffer = ByteBuffer.allocate(size);
		buffer.put(BATCH_VERSION).putLong(proposerId).putLong(completedBelow).putInt(sequences.length);
		for (int i = 0; i < sequences.length; i++) {
			if (completedBelow > sequences[i]) {
				throw new IllegalArgumentException("Sequence " + sequences[i] + " is below completedBelow " + completedBelow);
			}
			buffer.putLong(sequences[i]).put(entryBodies.get(i));
		}
		return buffer.array();
	}

	/** Encodes requests of one proposer as one log entry, see {@link #encodeBatch(long, long, long[], List)}. */
	static byte[] encodeBatch(List<Request> requests) {
		var first = requests.getFirst();
		var sequences = new long[requests.size()];
		var bodies = new ArrayList<byte[]>(requests.size());
		for (int i = 0; i < sequences.length; i++) {
			var request = requests.get(i);
			if (request.proposerId() != first.proposerId() || request.completedBelow() != first.completedBelow()) {
				throw new IllegalArgumentException("A batch holds requests of one proposer, with one completedBelow");
			}
			sequences[i] = request.sequence();
			bodies.add(encodeEntryBody(request.entry()));
		}
		return encodeBatch(first.proposerId(), first.completedBelow(), sequences, bodies);
	}

	/** @throws IllegalArgumentException on malformed input or unknown version */
	static List<Request> decodeBatch(byte[] data) {
		var in = new Input(data);
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
		var requests = new ArrayList<Request>(count);
		for (int i = 0; i < count; i++) {
			long sequence = in.readLong();
			requests.add(new Request(proposerId, sequence, completedBelow, readEntry(in)));
		}
		in.finish();
		return List.copyOf(requests);
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

	private static void writeEntry(DataOutputStream out, LogEntry entry) throws IOException {
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
				writeBytes(out, jarData);
			}
			case LogEntry.Guarded guarded -> writeGuarded(out, guarded);
			case LogEntry.VerifyColumn(String name, long columnId) -> {
				out.writeByte(VERIFY_COLUMN);
				writeString(out, name);
				out.writeLong(columnId);
			}
			case LogEntry.Genesis(String clusterId, boolean seeded, String _) -> {
				out.writeByte(GENESIS);
				writeString(out, clusterId);
				out.writeBoolean(seeded);
			}
			case LogEntry.RegisterClient _ -> throw new IllegalArgumentException("Not in the old format");
		}
	}

	private static void writeWrite(DataOutputStream out, LogEntry.Write write) throws IOException {
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

	private static void writeSingle(DataOutputStream out,
			byte tag,
			long columnId,
			Keys keys,
			@Nullable Buf value,
			RequestTypeId requestType) throws IOException {
		out.writeByte(tag);
		out.writeLong(columnId);
		writeKeys(out, keys);
		writeNullableBuf(out, value);
		writeRequestType(out, requestType);
	}

	/** Keys and values are written as pairs, so a decoded entry always has matching sizes. Values are non-null. */
	private static void writeMulti(DataOutputStream out,
			byte tag,
			long columnId,
			List<Keys> keys,
			List<Buf> values,
			RequestTypeId requestType) throws IOException {
		out.writeByte(tag);
		out.writeLong(columnId);
		out.writeInt(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			writeKeys(out, keys.get(i));
			writeBuf(out, values.get(i));
		}
		writeRequestType(out, requestType);
	}

	private static void writeGuarded(DataOutputStream out, LogEntry.Guarded guarded) throws IOException {
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
		out.writeByte((guarded.singleResult() ? 1 : 0) | (updateId != 0 ? 2 : 0) | (guard != null ? 4 : 0));
		if (updateId != 0) {
			out.writeLong(updateId);
		}
		if (guard != null) {
			out.writeLong(guard.columnId());
			writeKeys(out, guard.keys());
			out.writeInt(guard.length());
			if (guard.sha256() != null) {
				out.write(guard.sha256());
			}
		}
	}

	/** Encoded size of a {@link LogEntry.Guarded} body without conflict keys, writes, update id and guard. */
	static final int GUARDED_BASE_BYTES = 1 + 2 * Long.BYTES + 2 * Integer.BYTES + 1;
	/** Encoded size of one conflict key of a {@link LogEntry.Guarded}: its column id and key hash. */
	static final int CONFLICT_KEY_BYTES = 2 * Long.BYTES;

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

	private static void writeKeys(DataOutputStream out, Keys keys) throws IOException {
		Buf[] segments = keys.keys();
		out.writeInt(segments.length);
		for (Buf segment : segments) {
			writeBuf(out, segment);
		}
	}

	private static void writeNullableKeys(DataOutputStream out, @Nullable Keys keys) throws IOException {
		out.writeBoolean(keys != null);
		if (keys != null) {
			writeKeys(out, keys);
		}
	}

	private static void writeRequestType(DataOutputStream out, RequestTypeId requestType) throws IOException {
		Byte code = REQUEST_TYPE_CODES.get(Objects.requireNonNull(requestType, "requestType"));
		if (code == null) {
			throw new IllegalArgumentException("Request type has no log code: " + requestType);
		}
		out.writeByte(code);
	}

	private static void writeSchema(DataOutputStream out, ColumnSchema schema) throws IOException {
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

	static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	static void writeBuf(DataOutputStream out, Buf buf) throws IOException {
		writeBytes(out, buf.toByteArray());
	}

	static void writeNullableBuf(DataOutputStream out, @Nullable Buf buf) throws IOException {
		if (buf == null) {
			out.writeInt(-1);
		} else {
			writeBuf(out, buf);
		}
	}

	/** Strict UTF-8: a string with unpaired surrogates is rejected instead of silently altered. */
	static void writeString(DataOutputStream out, String value) throws IOException {
		ByteBuffer encoded;
		try {
			encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(value));
		} catch (CharacterCodingException e) {
			throw new IllegalArgumentException("String is not valid Unicode", e);
		}
		out.writeInt(encoded.remaining());
		out.write(encoded.array(), encoded.arrayOffset() + encoded.position(), encoded.remaining());
	}

	private static void writeNullableString(DataOutputStream out, @Nullable String value) throws IOException {
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
		if ((flags & ~7) != 0) {
			throw new IllegalArgumentException("Unknown guarded entry flags: " + flags);
		}
		boolean singleResult = (flags & 1) != 0;
		long updateId = (flags & 2) != 0 ? in.readLong() : 0;
		LogEntry.Guard guard = null;
		if ((flags & 4) != 0) {
			long columnId = in.readLong();
			var keys = readKeys(in);
			int length = in.readInt();
			guard = new LogEntry.Guard(columnId, keys, length, length >= 0 ? in.readFixed(32) : null);
		}
		// The constructor rejects inconsistent combinations with IllegalArgumentException
		return new LogEntry.Guarded(leaderEpoch, snapshotIndex, columns, hashes, writes, singleResult, updateId, guard);
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

	/** Bounds-checked reader of an encoding: every malformation is reported as {@link IllegalArgumentException}. */
	static final class Input {

		private final ByteBuffer buffer;

		Input(byte[] data) {
			this.buffer = ByteBuffer.wrap(Objects.requireNonNull(data, "data"));
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

		byte[] readFixed(int length) {
			require(length);
			return readBytes(length);
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
