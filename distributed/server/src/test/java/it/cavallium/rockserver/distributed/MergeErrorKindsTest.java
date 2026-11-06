package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Which {@link Error}s thrown by a merge operator are a deterministic result and which stop the replica. An
 * {@link AssertionError} or a {@link LinkageError} follows from the operator's code and the operands, so every replica
 * throws it at the same entry: the write fails with {@code MERGE_OPERATOR_FAILED}, the failure is recorded, and the
 * replica goes on. A {@link StackOverflowError} (a {@link VirtualMachineError}) depends on this JVM's stack and
 * state: it stops the replica instead of becoming a result another replica might not have.
 */
class MergeErrorKindsTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	/** Appends operands with commas; throws the error an operand names. */
	public static final class ErrorOperator extends FFMByteArrayMergeOperator {
		public ErrorOperator() {
			super("ErrorOperator");
		}

		@Override
		public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
			var out = new ByteArrayOutputStream();
			if (existingValue != null) {
				out.writeBytes(existingValue);
			}
			for (byte[] operand : operands) {
				switch (new String(operand, StandardCharsets.UTF_8)) {
					case "assert" -> throw new AssertionError("operator invariant broken");
					case "linkage" -> throw new NoSuchMethodError("com.example.Missing.method()V");
					case "overflow" -> throw new StackOverflowError();
					default -> {
					}
				}
				if (out.size() > 0) {
					out.write(',');
				}
				out.writeBytes(operand);
			}
			return out.toByteArray();
		}

		@Override
		protected void onMergeError(Throwable t, boolean partial) {
			// Expected here.
		}
	}

	@TempDir
	Path temp;

	private NodeStore open() throws Exception {
		var config = temp.resolve("errors.conf");
		if (!Files.exists(config)) {
			Files.writeString(config, PoisonEntryTest.POISON_CONFIG.replace(PoisonEntryTest.PoisonOperator.class.getName(),
					ErrorOperator.class.getName()));
		}
		return NodeStore.open(temp.resolve("db"), config, "merge-errors");
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static ApplyResult merge(Applier applier, long sequence, long column, String operand) {
		return applier.apply(new Request(1, sequence, 0, new LogEntry.Merge(column, key(1), text(operand),
				RequestTypeId.MERGED)), TermIndex.valueOf(1, sequence + 1));
	}

	@Test
	void anAssertionErrorOrALinkageErrorIsTheResultAndTheReplicaGoesOn() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			for (var operand : List.of("assert", "linkage")) {
				long sequence = operand.equals("assert") ? 1 : 2;
				var failed = assertInstanceOf(ApplyResult.Failed.class, merge(applier, sequence, column, operand));
				assertEquals(RocksDBErrorType.MERGE_OPERATOR_FAILED, failed.type(), failed.message());
			}
			// Recorded like any result, and the next entry applies.
			assertEquals("a", store.db().get(0, column, key(1), RequestType.current()).toString(StandardCharsets.UTF_8));
			var ok = assertInstanceOf(ApplyResult.Ok.class, merge(applier, 3, column, "b"));
			assertEquals("a,b", ((Buf) ok.value()).toString(StandardCharsets.UTF_8));
			assertEquals(TermIndex.valueOf(1, 4), store.applied());
		}
	}

	@Test
	void aStackOverflowInTheOperatorStopsTheReplica() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			assertThrows(Applier.ReplicaFailedException.class, () -> merge(applier, 1, column, "overflow"));
		}
		var wrapped = it.cavallium.rockserver.core.impl.EmbeddedDB.mergeOperatorFailed(new AssertionError("x"));
		assertTrue(!Applier.isLocalFailure(wrapped), "an AssertionError of the operator was taken as local");
	}
}
