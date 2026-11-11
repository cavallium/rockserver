package it.cavallium.rockserver.distributed;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gives back what Ratis 3.3.1 keeps for good of a transaction that the state machine refuses in
 * {@code preAppendTransaction} (R62-3).
 *
 * <p>A leader counts the client requests it has not answered against two limits, a number and a size
 * ({@code raft.server.write.element-limit} and {@code byte-limit}, see {@link RaftSettings}). {@code
 * RaftServerImpl.appendTransaction} takes a permit from them, then appends, which calls
 * {@code preAppendTransaction}; and when that throws, it answers the client with the exception and returns without
 * giving the permit back (checked in the 3.3.1 bytecode: the handler of {@code StateMachineException} around
 * {@code ServerState.appendLog} never touches the permit; only {@code addPendingRequest}, after it, hands it on to the
 * request that is released when it is answered). This state machine refuses there every commit of a transaction that
 * conflicts, in log order, which is the only place where that can be decided. So every conflict took one request and
 * its bytes off the leader's limits until the end of its term, and after 4096 conflicts the leader refused every
 * write, of every node, with "Failed to acquire a pending write request", until it stopped leading.</p>
 *
 * <p>{@link #release} is called by {@code preAppendTransaction} right before it throws, on the thread and under the
 * server monitor of that {@code appendTransaction}. It does what Ratis does when it answers a request
 * ({@code PendingRequests.RequestMap.remove}): it takes the request's bytes off the count and releases one request and
 * the megabytes that count no longer needs. The permit itself is an empty object that Ratis keeps in a map until the
 * request is added, and nothing tells which one of the map is this thread's: other threads hold permits of their own
 * between taking them and entering the monitor. So the object stays, and is removed later, by a sweep that runs at a
 * refusal, at most every {@link #SWEEP_EVERY}: a permit that {@link #MIN_SIGHTINGS} sweeps found in the map, the first
 * of them {@link #STALE_AFTER} ago, counts as left behind. Without that the map would grow by one entry for every
 * conflict of a term.</p>
 *
 * <p>The time alone does not tell a permit that was left behind from one whose thread still waits for the monitor
 * (review of R62): a process that stood still for {@link #STALE_AFTER} (SIGSTOP, a frozen virtual machine) has such a
 * thread's permit in one sweep before the pause and in the next right after it. Removing it would make Ratis answer
 * that thread's request, after appending its entry, with "Failed to add a pending write request" (an unknown outcome
 * to {@link Proposer}, so nothing is lost), and keep its permit for good. Hence the number of sweeps: a pause
 * shortens the time between two of them, not their number, and each sweep runs under the monitor that the waiting
 * thread is next in line for. A thread that waits for that monitor through {@link #MIN_SIGHTINGS} refusals of others,
 * {@link #SWEEP_EVERY} apart each, is not excluded by anything but is not a thing a running process does.</p>
 *
 * <p>All of this reaches into classes that are private to Ratis, by reflection, resolved once when this class loads.
 * If a member is missing (another Ratis version) nothing is released and one error is logged; {@code
 * RatisContractTest} pins both that Ratis 3.3.1 leaks and that this gives back exactly what leaked, so that a version
 * of Ratis that releases the permit itself fails there and this class is removed with the upgrade.</p>
 */
final class RefusedPermits {

	private static final Logger LOG = LoggerFactory.getLogger(RefusedPermits.class);
	/** How long a permit stays in Ratis' map before it counts as left behind. */
	static final Duration STALE_AFTER = Duration.ofSeconds(60);
	static final Duration SWEEP_EVERY = Duration.ofSeconds(10);
	/**
	 * In how many sweeps a permit must have been found before it counts as left behind: as many as fit in
	 * {@link #STALE_AFTER} of a process that runs.
	 */
	static final int MIN_SIGHTINGS = (int) (STALE_AFTER.toNanos() / SWEEP_EVERY.toNanos()) + 1;

	private record Handles(Class<?> server, Method role, Method leaderState, Field pendingRequests, Field requestMap,
			Field permits, Field resource, Field requestSize, Method release, Method roundUpMb) {}

	private static final @Nullable Handles HANDLES = resolve();

	private static @Nullable Handles resolve() {
		try {
			var loader = RaftServer.class.getClassLoader();
			var server = Class.forName("org.apache.ratis.server.impl.RaftServerImpl", false, loader);
			var roleInfo = Class.forName("org.apache.ratis.server.impl.RoleInfo", false, loader);
			var leaderState = Class.forName("org.apache.ratis.server.impl.LeaderStateImpl", false, loader);
			var pending = Class.forName("org.apache.ratis.server.impl.PendingRequests", false, loader);
			var map = Class.forName("org.apache.ratis.server.impl.PendingRequests$RequestMap", false, loader);
			var limits = Class.forName("org.apache.ratis.server.impl.PendingRequests$RequestLimits", false, loader);
			var handles = new Handles(server,
					accessible(server.getDeclaredMethod("getRole")),
					accessible(roleInfo.getDeclaredMethod("getLeaderState")),
					accessible(typed(leaderState.getDeclaredField("pendingRequests"), pending)),
					accessible(typed(pending.getDeclaredField("pendingRequests"), map)),
					accessible(typed(map.getDeclaredField("permits"), Map.class)),
					accessible(typed(map.getDeclaredField("resource"), limits)),
					accessible(typed(map.getDeclaredField("requestSize"), AtomicLong.class)),
					accessible(limits.getDeclaredMethod("release", int.class)),
					accessible(pending.getDeclaredMethod("roundUpMb", long.class)));
			if (handles.roundUpMb().getReturnType() != int.class || handles.leaderState().getReturnType() != Optional.class) {
				throw new NoSuchMethodException("PendingRequests.roundUpMb or RoleInfo.getLeaderState changed its type");
			}
			return handles;
		} catch (ReflectiveOperationException | RuntimeException | LinkageError ex) {
			LOG.error("The pending-request permits of this Ratis version cannot be reached: a transaction refused before"
					+ " its append (a conflict) will keep its permit, and a leader that refuses about "
					+ RaftSettings.PENDING_REQUESTS_LIMIT + " of them stops accepting writes until it stops leading", ex);
			return null;
		}
	}

	private static Field typed(Field field, Class<?> type) throws NoSuchFieldException {
		if (field.getType() != type) {
			throw new NoSuchFieldException(field + " is not a " + type.getName());
		}
		return field;
	}

	private static <T extends java.lang.reflect.AccessibleObject> T accessible(T member) {
		member.setAccessible(true);
		return member;
	}

	/** Whether {@link #release} can reach Ratis' permits. */
	static boolean available() {
		return HANDLES != null;
	}

	/** When a sweep first found a permit in the map, and in how many sweeps it was found. */
	private static final class Sighting {
		final long firstNanos;
		int count = 1;

		Sighting(long firstNanos) {
			this.firstNanos = firstNanos;
		}
	}

	/** Ratis' map of permits not yet added as requests, in which the sweep last looked, and what it saw of each. */
	private @Nullable Object sweptMap;
	private final IdentityHashMap<Object, Sighting> sightings = new IdentityHashMap<>();
	private long lastSweepNanos;
	private boolean failureLogged;
	private final AtomicLong released = new AtomicLong();
	private final AtomicLong swept = new AtomicLong();

	/** Permits given back so far, for the metrics and tests. */
	long released() {
		return released.get();
	}

	/** Permit objects removed from Ratis' map so far, for tests. */
	long swept() {
		return swept.get();
	}

	/**
	 * Gives back the permit of {@code request}, which {@code preAppendTransaction} is about to refuse. Only from
	 * there: on the thread of Ratis' {@code appendTransaction}, which took the permit and holds the server's monitor.
	 * Does nothing for a division that is not Ratis' own (tests) or that does not lead.
	 */
	void release(@Nullable RaftServer.Division division, @Nullable RaftClientRequest request) {
		release(division, request, System.nanoTime());
	}

	void release(@Nullable RaftServer.Division division, @Nullable RaftClientRequest request, long nowNanos) {
		var handles = HANDLES;
		if (handles == null || division == null || request == null || !handles.server().isInstance(division)) {
			return;
		}
		try {
			var leaderState = ((Optional<?>) handles.leaderState().invoke(handles.role().invoke(division))).orElse(null);
			if (leaderState == null) {
				return;
			}
			var map = handles.requestMap().get(handles.pendingRequests().get(leaderState));
			var resource = handles.resource().get(map);
			var requestSize = (AtomicLong) handles.requestSize().get(map);
			@SuppressWarnings("unchecked")
			var permits = (Map<Object, Object>) handles.permits().get(map);
			// Ratis' own lock of this map: tryAcquire and put take it.
			synchronized (map) {
				if (permits.isEmpty()) {
					// Not Ratis' appendTransaction (it would hold a permit here): nothing was taken.
					return;
				}
				int size = Message.getSize(request.getMessage());
				long before = requestSize.getAndAdd(-size);
				int megabytes = (int) handles.roundUpMb().invoke(null, before)
						- (int) handles.roundUpMb().invoke(null, before - size);
				handles.release().invoke(resource, megabytes);
				released.incrementAndGet();
				sweep(map, permits, nowNanos);
			}
		} catch (ReflectiveOperationException | RuntimeException ex) {
			if (!failureLogged) {
				failureLogged = true;
				LOG.error("Failed to give back the pending-request permit of a refused transaction (logged once)", ex);
			}
		}
	}

	/**
	 * Runs the sweep alone, for tests: on a thread that holds the monitor of {@code division}, as a refusal does.
	 *
	 * @return the number of permits in Ratis' map after it, or -1 if the map cannot be reached
	 */
	int sweepForTesting(RaftServer.Division division, long nowNanos) throws ReflectiveOperationException {
		var handles = HANDLES;
		if (handles == null || !handles.server().isInstance(division)) {
			return -1;
		}
		var leaderState = ((Optional<?>) handles.leaderState().invoke(handles.role().invoke(division))).orElse(null);
		if (leaderState == null) {
			return -1;
		}
		var map = handles.requestMap().get(handles.pendingRequests().get(leaderState));
		@SuppressWarnings("unchecked")
		var permits = (Map<Object, Object>) handles.permits().get(map);
		synchronized (map) {
			sweep(map, permits, nowNanos);
			return permits.size();
		}
	}

	/**
	 * Removes the permits that {@link #MIN_SIGHTINGS} sweeps found in the map, the first of them {@link #STALE_AFTER}
	 * ago; at most every {@link #SWEEP_EVERY}.
	 */
	private void sweep(Object map, Map<Object, Object> permits, long nowNanos) {
		if (map != sweptMap) {
			// A new term has a new map.
			sweptMap = map;
			sightings.clear();
			lastSweepNanos = nowNanos - SWEEP_EVERY.toNanos();
		}
		if (nowNanos - lastSweepNanos < SWEEP_EVERY.toNanos()) {
			return;
		}
		lastSweepNanos = nowNanos;
		sightings.keySet().removeIf(permit -> !permits.containsKey(permit));
		var each = permits.keySet().iterator();
		while (each.hasNext()) {
			var permit = each.next();
			var seen = sightings.get(permit);
			if (seen == null) {
				sightings.put(permit, new Sighting(nowNanos));
			} else if (++seen.count >= MIN_SIGHTINGS && nowNanos - seen.firstNanos >= STALE_AFTER.toNanos()) {
				each.remove();
				sightings.remove(permit);
				swept.incrementAndGet();
			}
		}
	}
}
