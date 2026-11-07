package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.jetbrains.annotations.Nullable;

/**
 * The committed configurations this node applied (R11): every configuration entry that Ratis applied here, with its
 * term and both halves, since {@link #completeFrom()}. The answers to {@link TermService#CONFIGURATION2} read it.
 *
 * <p>Each one is written by the apply thread into the database's meta column ({@link NodeStore#CONFIGURATION}), in the
 * same atomic write as the applied position that covers it, so a crash keeps both or neither, and a snapshot carries
 * them (a follower that installs it gets the leader's history up to the snapshot). Ratis' own notification is not used
 * for the record: on a snapshot install it reports the leader's latest configuration, which may not be committed, with
 * no term ({@code SnapshotInstallationHandler}); only a configuration that the apply path passes on together with its
 * position counts ({@link RockserverStateMachine#notifyConfigurationChanged}).</p>
 *
 * <p>{@code completeFrom}: the first index from which every committed configuration applied here is recorded. A
 * database that a version without this history applied (or a snapshot of one) has none below the first entry this
 * version applied, see {@link NodeStore#configurationHistoryFrom}. Kept forever: one entry per term start and per
 * membership change, about a hundred bytes each.</p>
 */
final class CommittedConfigurations {

	private final ConcurrentSkipListMap<Long, Conf> byIndex = new ConcurrentSkipListMap<>();
	private final long completeFrom;

	CommittedConfigurations(Collection<Conf> recorded, long completeFrom) {
		for (var conf : recorded) {
			byIndex.put(conf.index(), conf);
		}
		this.completeFrom = completeFrom;
	}

	/** An empty history, complete from nowhere: before the database is open. */
	static CommittedConfigurations none() {
		return new CommittedConfigurations(List.of(), Long.MAX_VALUE);
	}

	/** Records a configuration the apply thread committed to the database. */
	void add(Conf conf) {
		byIndex.put(conf.index(), conf);
	}

	/** The newest recorded configuration, or null. */
	@Nullable Conf last() {
		var last = byIndex.lastEntry();
		return last == null ? null : last.getValue();
	}

	/** The recorded configuration at {@code index}, or null. */
	@Nullable Conf at(long index) {
		return byIndex.get(index);
	}

	/** See the class comment. */
	long completeFrom() {
		return completeFrom;
	}

	/** Tests only. */
	List<Conf> all() {
		return List.copyOf(byIndex.values());
	}

	/** Encodes the halves of {@code conf}: its new half, then its old half (absent if unknown). */
	static byte[] encodeHalves(Conf conf) {
		var bytes = new ByteArrayOutputStream();
		try (var out = new DataOutputStream(bytes)) {
			writeHalves(out, conf);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return bytes.toByteArray();
	}

	static Conf decodeHalves(long term, long index, byte[] value) {
		try (var in = new DataInputStream(new java.io.ByteArrayInputStream(value))) {
			return readHalves(in, term, index);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/** An int count then UTF strings for the new half; one byte 1 then the same for a known old half, or 0. */
	static void writeHalves(DataOutputStream out, Conf conf) throws IOException {
		writeIds(out, conf.newHalf());
		if (conf.oldHalf() == null) {
			out.writeByte(0);
		} else {
			out.writeByte(1);
			writeIds(out, conf.oldHalf());
		}
	}

	static Conf readHalves(DataInputStream in, long term, long index) throws IOException {
		var newHalf = readIds(in);
		var oldHalf = in.readByte() != 0 ? readIds(in) : null;
		return new Conf(term, index, newHalf, oldHalf);
	}

	static void writeIds(DataOutputStream out, List<String> ids) throws IOException {
		out.writeInt(ids.size());
		for (var id : ids) {
			out.writeUTF(id);
		}
	}

	static List<String> readIds(DataInputStream in) throws IOException {
		int count = in.readInt();
		if (count < 0 || count > 10_000) {
			throw new IOException("Bad member count " + count);
		}
		var ids = new ArrayList<String>(count);
		for (int i = 0; i < count; i++) {
			ids.add(in.readUTF());
		}
		return ids;
	}

	/** An int count then UTF id and address pairs. */
	static void writeAddresses(DataOutputStream out, Map<String, String> addresses) throws IOException {
		out.writeInt(addresses.size());
		for (var entry : addresses.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeUTF(entry.getValue());
		}
	}

	static Map<String, String> readAddresses(DataInputStream in) throws IOException {
		int count = in.readInt();
		if (count < 0 || count > 10_000) {
			throw new IOException("Bad address count " + count);
		}
		var addresses = new java.util.LinkedHashMap<String, String>();
		for (int i = 0; i < count; i++) {
			addresses.put(in.readUTF(), in.readUTF());
		}
		return addresses;
	}
}
