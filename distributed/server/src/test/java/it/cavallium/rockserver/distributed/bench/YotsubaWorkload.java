package it.cavallium.rockserver.distributed.bench;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Operations shaped after what production Yotsuba sends to Rockserver (Grafana, 7 days to 2026-10-30, per second:
 * ~5.6k update commits, ~0.3k failed updates, ~2.2k other puts, ~2.1k putMulti, ~0.14k existsMulti of 256 keys,
 * a few range reads), on the same column layouts:
 * <ul>
 *   <li>{@code messages-v2}: key [8 chat, 4 message], a serialized message value; updated with
 *   {@code get(forUpdate)} then {@code put(updateId)} (or {@code closeFailedUpdate} when nothing changed);</li>
 *   <li>{@code sender-index}: key [8, 8, 3], no value, written with {@code put(ensure)} on every update;</li>
 *   <li>{@code history-archive-chunks}: key [14, 16, 1, 8], written with {@code putMulti}, mostly of one 1-3 KiB
 *   chunk;</li>
 *   <li>{@code file-locations}: key [4] + two XXHASH32 tails, no value, written with {@code putMulti};</li>
 *   <li>{@code monitor-log}: key [8, 8], small transactions of two {@code putMulti}.</li>
 * </ul>
 * Chats are picked with a Zipf-like skew so that hot chats produce real update conflicts. {@link #hotKeys} sends a
 * share of the updates to a few single keys instead, to measure what one key sustains.
 */
final class YotsubaWorkload {

	/** Operation kinds and their production rate per second at scale 1. */
	enum Kind {
		UPDATE(5900),
		ENSURE(2200),
		HISTORY_PUT_MULTI(1800),
		FILE_PUT_MULTI(300),
		GET(200),
		EXISTS_MULTI(136),
		FIRST_LAST(40),
		RANGE_PAGE(4),
		MONITOR_TX(34),
		/** Not in the production mix: 1 MiB putMulti calls, to measure bandwidth (use with --only). */
		BULK_PUT(0);

		final double ratePerSecond;

		Kind(double ratePerSecond) {
			this.ratePerSecond = ratePerSecond;
		}
	}

	static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final int UPDATE_RETRY_ATTEMPTS = 16;

	final int chats;
	final int messagesPerChat;
	final int valueBytes;
	final double skew;
	/** Keys that {@link #hotShare} of the updates go to, 0 for none. */
	final int hotKeys;
	final double hotShare;
	long messages;
	long senderIndex;
	long history;
	long files;
	long monitorLog;

	YotsubaWorkload(int chats, int messagesPerChat, int valueBytes, double skew) {
		this(chats, messagesPerChat, valueBytes, skew, 0, 0);
	}

	YotsubaWorkload(int chats, int messagesPerChat, int valueBytes, double skew, int hotKeys, double hotShare) {
		if (hotKeys < 0 || hotKeys > chats || hotShare < 0 || hotShare > 1) {
			throw new IllegalArgumentException("Invalid hot keys: " + hotKeys + " keys, share " + hotShare);
		}
		this.chats = chats;
		this.messagesPerChat = messagesPerChat;
		this.valueBytes = valueBytes;
		this.skew = skew;
		this.hotKeys = hotKeys;
		this.hotShare = hotShare;
	}

	/** Creates (or finds) the columns. */
	void createColumns(RocksDBAsyncAPI api) {
		messages = api.createColumnAsync("messages-v2", ColumnSchema.of(IntList.of(8, 4), ObjectList.of(), true)).join();
		senderIndex = api.createColumnAsync("sender-index", ColumnSchema.of(IntList.of(8, 8, 3), ObjectList.of(), false))
				.join();
		history = api.createColumnAsync("history-archive-chunks",
				ColumnSchema.of(IntList.of(14, 16, 1, 8), ObjectList.of(), true)).join();
		files = api.createColumnAsync("file-locations",
				ColumnSchema.of(IntList.of(4), ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32), false)).join();
		monitorLog = api.createColumnAsync("monitor-log-v1", ColumnSchema.of(IntList.of(8, 8), ObjectList.of(), true))
				.join();
	}

	/** Writes the initial messages, {@code batch} keys per putMulti, with {@code parallelism} batches in flight. */
	void preload(RocksDBAsyncAPI api, int batch, int parallelism) {
		var inFlight = new java.util.concurrent.Semaphore(parallelism);
		var failures = new java.util.concurrent.atomic.AtomicReference<Throwable>();
		var keys = new ArrayList<Keys>(batch);
		var values = new ArrayList<Buf>(batch);
		for (int chat = 0; chat < chats; chat++) {
			for (int message = 0; message < messagesPerChat; message++) {
				keys.add(messageKey(chat, message));
				values.add(value(valueBytes));
				if (keys.size() == batch) {
					submitPreload(api, keys, values, inFlight, failures);
					keys = new ArrayList<>(batch);
					values = new ArrayList<>(batch);
				}
			}
		}
		if (!keys.isEmpty()) {
			submitPreload(api, keys, values, inFlight, failures);
		}
		inFlight.acquireUninterruptibly(parallelism);
		if (failures.get() != null) {
			throw new IllegalStateException("Preload failed", failures.get());
		}
	}

	private void submitPreload(RocksDBAsyncAPI api, List<Keys> keys, List<Buf> values,
			java.util.concurrent.Semaphore inFlight, java.util.concurrent.atomic.AtomicReference<Throwable> failures) {
		inFlight.acquireUninterruptibly();
		api.putMultiAsync(0, messages, keys, values, RequestType.none()).whenComplete((_, error) -> {
			if (error != null) {
				failures.compareAndSet(null, error);
			}
			inFlight.release();
		});
	}

	/** Runs one operation; returns the payload bytes it moved. Throws on failure. */
	long run(Kind kind, RocksDBAsyncAPI api, BenchStats.Op stats) {
		var random = ThreadLocalRandom.current();
		return switch (kind) {
			case UPDATE -> update(api, stats, random);
			case ENSURE -> {
				api.putAsync(0, senderIndex, senderKey(random), Buf.create(0), RequestType.ensure()).join();
				yield 19;
			}
			case HISTORY_PUT_MULTI -> {
				// Mostly the single chunk spilled after a message update, sometimes a few.
				int count = random.nextInt(8) == 0 ? 2 + random.nextInt(3) : 1;
				var keys = new ArrayList<Keys>(count);
				var values = new ArrayList<Buf>(count);
				long bytes = 0;
				for (int i = 0; i < count; i++) {
					keys.add(historyKey(random));
					var value = value(1024 + random.nextInt(2048));
					values.add(value);
					bytes += 39 + value.size();
				}
				api.putMultiAsync(0, history, keys, values, RequestType.none()).join();
				yield bytes;
			}
			case FILE_PUT_MULTI -> {
				int count = 1 + random.nextInt(16);
				var keys = new ArrayList<Keys>(count);
				var values = new ArrayList<Buf>(count);
				for (int i = 0; i < count; i++) {
					keys.add(new Keys(fixed(4, random.nextLong()), randomBuf(random, 40 + random.nextInt(80)),
							randomBuf(random, 20 + random.nextInt(40))));
					values.add(Buf.create(0));
				}
				api.putMultiAsync(0, files, keys, values, RequestType.none()).join();
				yield count * 100L;
			}
			case GET -> {
				Buf value = api.getAsync(0, messages, messageKey(chat(random), random.nextInt(messagesPerChat)),
						RequestType.current()).join();
				yield value != null ? value.size() + 12 : 12;
			}
			case EXISTS_MULTI -> {
				int chat = chat(random);
				var keys = new ArrayList<Keys>(256);
				int first = random.nextInt(Math.max(1, messagesPerChat));
				for (int i = 0; i < 256; i++) {
					keys.add(messageKey(chat, first + i * 7));
				}
				api.existsMultiAsync(0, messages, keys).join();
				yield 256 * 12;
			}
			case FIRST_LAST -> {
				int chat = chat(random);
				api.reduceRangeAsync(0, messages, new Keys(fixed(8, chat), fixed(4, 0)),
						new Keys(fixed(8, chat + 1L), fixed(4, 0)), false, RequestType.<KV>firstAndLast()).join();
				yield 2L * (valueBytes + 12);
			}
			case RANGE_PAGE -> {
				int chat = chat(random);
				var page = api.getRangePageAsync(0, messages, new Keys(fixed(8, chat), fixed(4, 0)),
						new Keys(fixed(8, chat + 1L), fixed(4, 0)), false, null, RequestType.<KV>allInRange(),
						RangeBudget.DEFAULT).join();
				yield (long) page.items().size() * (valueBytes + 12);
			}
			case MONITOR_TX -> monitorTransaction(api, random);
			case BULK_PUT -> {
				var keys = new ArrayList<Keys>(16);
				var values = new ArrayList<Buf>(16);
				for (int i = 0; i < 16; i++) {
					keys.add(historyKey(random));
					values.add(value(64 * 1024));
				}
				api.putMultiAsync(0, history, keys, values, RequestType.none()).join();
				yield 16L * (39 + 64 * 1024);
			}
		};
	}

	/**
	 * The message update of {@code MessagesImpl}: {@code get(forUpdate)}, then the commit {@code put(updateId)} or,
	 * when nothing changed (about 5%), {@code closeFailedUpdate}; conflicts are retried with 10-250 ms backoff, like a
	 * no-change close told to retry, which needs pipelined updates. A failed {@code get}, such as the lock wait of a hot
	 * key timing out, is a failed operation, as it always was here, so that runs stay comparable: {@code lockWaitRetries}
	 * counts the ones that say to retry.
	 */
	private long update(RocksDBAsyncAPI api, BenchStats.Op stats, ThreadLocalRandom random) {
		var key = updateKey(random);
		long backoffMs = 10;
		for (int attempt = 0; ; attempt++) {
			UpdateContext<Buf> context;
			try {
				context = api.<UpdateContext<Buf>>getAsync(0, messages, key, RequestType.forUpdate()).join();
			} catch (CompletionException | RocksDBException e) {
				if (isRetry(e)) {
					stats.lockWaitRetries.increment();
				}
				throw e;
			}
			try {
				boolean unchanged = random.nextInt(20) == 0;
				if (unchanged) {
					api.closeFailedUpdateAsync(context.updateId()).join();
					return 12;
				}
				var value = value(valueBytes / 2 + random.nextInt(valueBytes * 2));
				api.putAsync(context.updateId(), messages, key, value, RequestType.none()).join();
				return 12 + value.size();
			} catch (CompletionException | RocksDBException e) {
				if (!isRetry(e) || attempt + 1 >= UPDATE_RETRY_ATTEMPTS) {
					throw e;
				}
				stats.retries.increment();
				sleep(Math.min(250, backoffMs) + random.nextInt(5));
				backoffMs *= 2;
			}
		}
	}

	private static boolean isRetry(RuntimeException error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		return cause instanceof RocksDBRetryException
				|| (cause instanceof RocksDBException r && r.getErrorUniqueId() == RocksDBErrorType.UPDATE_RETRY);
	}

	/** The key of one update: a hot one, or one of the skewed chats. */
	private Keys updateKey(ThreadLocalRandom random) {
		if (hotKeys > 0 && random.nextDouble() < hotShare) {
			return messageKey(random.nextInt(hotKeys), messagesPerChat - 1);
		}
		// Mostly recent messages of the chat: new messages and edits of the latest ones.
		int message = messagesPerChat - 1 - (int) Math.min(messagesPerChat - 1, Math.abs(random.nextGaussian()) * 20);
		return messageKey(chat(random), message);
	}

	/** A monitor batch: existsMulti, then a transaction with two putMulti. */
	private long monitorTransaction(RocksDBAsyncAPI api, ThreadLocalRandom random) {
		long monitor = random.nextInt(1000);
		var keys = new ArrayList<Keys>(16);
		var values = new ArrayList<Buf>(16);
		for (int i = 0; i < 16; i++) {
			keys.add(new Keys(fixed(8, monitor), fixed(8, random.nextLong())));
			values.add(value(200));
		}
		api.existsMultiAsync(0, monitorLog, keys).join();
		for (int attempt = 0; attempt < UPDATE_RETRY_ATTEMPTS; attempt++) {
			long tx = api.openTransactionAsync(TIMEOUT).join();
			boolean committed = false;
			try {
				api.putMultiAsync(tx, monitorLog, keys.subList(0, 8), values.subList(0, 8), RequestType.none()).join();
				api.putMultiAsync(tx, monitorLog, keys.subList(8, 16), values.subList(8, 16), RequestType.none()).join();
				committed = api.closeTransactionAsync(tx, true).join();
			} finally {
				if (!committed) {
					try {
						api.closeTransactionAsync(tx, false).join();
					} catch (RuntimeException ignored) {
						// Already closed.
					}
				}
			}
			if (committed) {
				return 16 * 216L;
			}
			sleep(10);
		}
		throw new IllegalStateException("Monitor transaction kept conflicting");
	}

	/** Chat index with a Zipf-like skew: a few chats get most of the traffic. */
	int chat(ThreadLocalRandom random) {
		if (skew <= 0) {
			return random.nextInt(chats);
		}
		// Inverse-CDF sampling of a power law, cheap and good enough for skew.
		double u = random.nextDouble();
		return (int) Math.min(chats - 1, Math.floor(chats * Math.pow(u, 1 + skew * 4)));
	}

	static Keys messageKey(long chat, int message) {
		return new Keys(fixed(8, chat), fixed(4, message));
	}

	private static Keys senderKey(ThreadLocalRandom random) {
		return new Keys(fixed(8, random.nextInt(100_000)), fixed(8, random.nextInt(1_000_000)), fixed(3, random.nextInt(4)));
	}

	private static Keys historyKey(ThreadLocalRandom random) {
		return new Keys(fixed(14, random.nextLong()), fixed(16, random.nextLong()), fixed(1, 0), fixed(8, random.nextLong()));
	}

	/** {@code size} bytes holding {@code value} big-endian in its last (up to) 8 bytes. */
	static Buf fixed(int size, long value) {
		var bytes = new byte[size];
		for (int i = size - 1, shift = 0; i >= 0 && shift < 64; i--, shift += 8) {
			bytes[i] = (byte) (value >>> shift);
		}
		return Buf.wrap(bytes);
	}

	private static Buf randomBuf(ThreadLocalRandom random, int size) {
		var bytes = new byte[size];
		random.nextBytes(bytes);
		return Buf.wrap(bytes);
	}

	/** A value of about {@code size} bytes, half random (incompressible) and half repetitive, like serialized records. */
	static Buf value(int size) {
		var bytes = new byte[size];
		var random = ThreadLocalRandom.current();
		for (int i = 0; i < size / 2; i++) {
			bytes[i] = (byte) random.nextInt();
		}
		return Buf.wrap(bytes);
	}

	/** Production rate per second of each kind at scale 1. */
	static Map<Kind, Double> productionMix() {
		var mix = new LinkedHashMap<Kind, Double>();
		for (var kind : Kind.values()) {
			mix.put(kind, kind.ratePerSecond);
		}
		return mix;
	}

	static void sleep(long millis) {
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new CompletionException(e);
		}
	}
}
