package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.CompactionIoBudget;
import it.cavallium.rockserver.core.impl.CompactionIoBudget.State;
import java.util.Random;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The control law of {@link CompactionIoBudget} as properties over randomized traces of a small model of the server:
 * background I/O wants {@code demand} bytes/s and gets min(demand, budget); point reads take {@code BASE} microseconds
 * while that rate is at or under {@code capacity} and are slower in proportion to the excess above it. The model is
 * an assumption (a real device is not this regular); what the properties pin is the controller's side.
 * <p>A write stall (RocksDB stops or delays writes) is an input of the model, not a consequence of its debt: while
 * the limiter is unlimited the background moves at the device's rate and reads are several times slower.
 */
class CompactionIoBudgetInvariantsTest {
    private static final long BASE = 100;
    private static final long UNLIMITED = CompactionIoBudget.UNLIMITED_BYTES_PER_SECOND;
    private static final class Plant {
        final CompactionIoBudget policy;
        final long seed;
        long nanos, reads, micros, bytes, completions, nativePoints, nativeNanos;
        /** Background bytes/s wanted, and the background rate above which point reads slow down. */
        double demand, capacity = Double.MAX_VALUE;
        /** Outcome of the last window. */
        double rate; long latency;
        int windows;
        /** The rate the limiter is at: what the last poll returned (the budget, or unlimited around a write stall). */
        long limiter;
        Plant(long seed) { this.seed = seed; policy = new CompactionIoBudget(seed); poll(0, BASE, false); }
        void poll(long transferred, long pointMicros, boolean delayed) {
            nanos += 1_000_000_000L; reads += 40; micros += 40 * pointMicros; bytes += transferred;
            completions += 40; nativePoints += 40; nativeNanos += 40 * pointMicros * 1000;
            limiter = policy.sample(new CompactionIoBudget.Sample(nanos, reads, micros, bytes,
                    transferred > 0, delayed, false, true, true, completions, !delayed, delayed,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
                    pointMicros, 0, nativePoints, 0, 0, nativeNanos).withWriteDelayed(delayed));
            assertEquals(policy.state() == State.UNLIMITED ? UNLIMITED : policy.budget(), limiter);
            assertTrue(limiter >= seed, "the limiter under the seed: " + limiter);
        }
        /** One 5 s window at the budget in force at its start; {@code extra} is added to the modelled latency. */
        void window(long extra) {
            rate = Math.min(demand, policy.budget());
            latency = Math.round(BASE * Math.max(1, rate / capacity)) + extra;
            for (int i = 0; i < 5; i++) poll((long) rate, latency, false);
            windows++;
            assertTrue(policy.budget() >= seed && policy.budget() <= CompactionIoBudget.MAX_BYTES_PER_SECOND,
                    "budget " + policy.budget() + " outside [" + seed + ", MAX] at window " + windows);
            assertTrue(policy.shutdownBudget() >= seed);
        }
        void window() { window(0); }
        boolean slow() { return latency > policy.baselineReadMicros() * 1.2; }
    }
    private static long seedOf(Random random) { return 100_000L << random.nextInt(8); }

    @Test void aWindowThatDidNotSaturateTheLimiterNeverLowersTheBudgetWhileReadsAreHealthy() {
        for (int run = 0; run < 200; run++) {
            var random = new Random(run);
            var p = new Plant(seedOf(random));
            long previous = p.policy.budget();
            for (int i = 0; i < 300; i++) {
                long before = p.policy.budget();
                p.demand = switch (random.nextInt(6)) {
                    case 0 -> 0;
                    case 1 -> 1000;
                    case 2 -> before * .3;
                    case 3 -> before * .79;
                    case 4 -> before;
                    default -> before * 4d;
                };
                // Healthy means, with one slow window now and then (never two in a row).
                p.window(i % 7 == 3 ? 150 : random.nextInt(9) - 4);
                // The one thing a single slow window does: it takes back the last doubling, if no window with
                // background I/O has tested it yet.
                long floor = i % 7 == 3 ? Math.min(before, previous) : before;
                if (p.rate < before * .8) assertTrue(p.policy.budget() >= floor,
                        "run " + run + " window " + i + ": " + before + " -> " + p.policy.budget() + " at rate " + p.rate);
                if (p.policy.budget() > before) previous = before;
            }
        }
    }

    @Test void slowReadsThatAnswerToTheRateAreFollowedDownWithinABoundAndTheBudgetComesBack() {
        int checked = 0, halfUsed = 0;
        for (int run = 0; run < 300; run++) {
            var random = new Random(1000 + run);
            var p = new Plant(seedOf(random));
            // Healthy start: the budget grows to at least the demand (twice it when the last doubling overshoots).
            p.demand = p.seed * (2 + random.nextInt(60));
            for (int i = 0; i < 30; i++) p.window();
            assertTrue(p.policy.budget() >= p.demand, "run " + run + ": budget " + p.policy.budget());
            assertEquals(State.TRACKING, p.policy.state());
            double utilisation = p.demand / p.policy.budget();
            // The device now serves reads at full speed only up to a part of that rate (never under 1.3x the floor).
            double before = p.demand;
            p.capacity = Math.max(p.seed * 1.3, before * (.15 + .7 * random.nextDouble()));
            if (before <= p.capacity * 1.25) continue; // within the 20% tolerance (and the rounding of a mean): not harm
            // Each kept decrease is 0.7x and takes 4 windows (one that starts it, one to settle, two that measure);
            // the first one starts at the second slow window. Growth then returns to 90% of the rate that harmed and
            // goes on slowly, which harms again sooner or later (the price of finding out whether capacity moved).
            int steps = (int) Math.ceil(Math.log(before / (p.capacity * 1.2)) / Math.log(1 / .7));
            int bound = 2 + 4 * steps, phase = bound + 100, slowWindows = 0, healthyAt = -1;
            for (int i = 1; i <= phase; i++) {
                p.window();
                if (p.slow()) slowWindows++;
                else if (healthyAt < 0) healthyAt = i;
                assertTrue(p.policy.budget() >= p.capacity * .7 * .99, "run " + run + ": a decrease below what was needed");
            }
            String at = "run " + run + " (utilisation " + utilisation + ", capacity/rate " + p.capacity / before + ")";
            assertTrue(healthyAt > 0 && healthyAt <= bound + 1, at + ": healthy at window " + healthyAt + ", bound " + bound);
            assertTrue(slowWindows <= bound + 12, at + ": slow windows " + slowWindows + " of " + phase);
            // Healthy is up to 1.2x a baseline that may itself rise to 1.2x its lowest level (see followed()).
            assertTrue(p.rate <= p.capacity * 1.44, at + ": rate " + p.rate / p.capacity + " of capacity");
            double ceiling = p.policy.harmCeilingBytesPerSecond();
            assertTrue(ceiling > p.capacity && ceiling <= before, at + ": ceiling " + ceiling);

            // The cause goes away. The budget is back at the rate that last harmed within 3 windows of a reduction
            // still under test, 2 of +10% and 12 of +5% every 6th; at the rate before the harm after that rate is
            // forgotten (120 windows) and the doublings from there.
            p.capacity = Double.MAX_VALUE;
            int toCeiling = -1, toBefore = -1;
            long last = p.policy.budget();
            for (int i = 1; i <= 140 && toBefore < 0; i++) {
                p.window();
                assertFalse(p.slow());
                assertTrue(p.policy.budget() >= last, at + ": the budget fell without slow reads");
                last = p.policy.budget();
                ceiling = p.policy.harmCeilingBytesPerSecond();
                if (toCeiling < 0 && last >= ceiling * .999) toCeiling = i;
                if (last >= before) toBefore = i;
            }
            // 3 + 2 + 12, and one window when whole bytes leave the second +10% step a byte short of the 90% level.
            assertTrue(toCeiling > 0 && toCeiling <= 18, at + ": back at the harmful rate after " + toCeiling);
            assertTrue(toBefore > 0 && toBefore <= 120 + 8, at + ": back at the previous rate after " + toBefore);
            checked++;
            if (utilisation <= .6) halfUsed++;
        }
        assertTrue(checked >= 200 && halfUsed >= 50, "runs with harm: " + checked + ", of them under a budget used 60% or less: " + halfUsed);
    }


    @Test void aConstantLoadWithReadsJustUnderTheThresholdDoesNotMoveTheBudgetBackAndForth() {
        for (int run = 0; run < 200; run++) {
            var random = new Random(2000 + run);
            var p = new Plant(seedOf(random));
            boolean unlimited = run % 2 == 0, spikes = run % 4 >= 2;
            p.demand = unlimited ? Double.MAX_VALUE : p.seed * (1 + random.nextInt(40));
            int turns = 0, direction = 0, settledAt = 0;
            long last = p.policy.budget();
            for (int i = 0; i < 400; i++) {
                // 12% to 18% over the first window's mean; with spikes, a single window at 1.6x now and then.
                p.window(i == 0 ? 0 : spikes && i % 9 == 4 ? 60 : 12 + random.nextInt(7));
                long budget = p.policy.budget();
                int step = Long.compare(budget, last);
                if (step != 0) { if (direction != 0 && step != direction) turns++; direction = step; settledAt = i + 1; }
                last = budget;
            }
            String at = "run " + run + (unlimited ? " unlimited" : " limited") + (spikes ? " with spikes" : "");
            assertTrue(last >= Math.min(p.demand, CompactionIoBudget.MAX_BYTES_PER_SECOND), at);
            // Without spikes: one trial after BOOTSTRAP (down, up) when it starts above the floor, then growth only.
            if (!spikes) assertTrue(turns <= 2, at + ": the budget changed direction " + turns + " times");
            // With spikes a doubling can be taken back and tried again. A limited demand ends that: the budget
            // comes to rest above it (40 doublings at most, each tried twice with 3 windows of hold).
            if (!unlimited) assertTrue(settledAt <= 60, at + ": the budget still moved at window " + settledAt);
        }
    }

    @Test void aWriteStallOpensTheLimiterAtOnceAndTeachesNothing() {
        int stalls = 0, closings = 0;
        for (int run = 0; run < 200; run++) {
            var random = new Random(4000 + run);
            var p = new Plant(seedOf(random));
            p.demand = p.seed * (2 + random.nextInt(60));
            for (int i = 0; i < 30; i++) p.window();
            assertEquals(State.TRACKING, p.policy.state());
            // Without a limit the device moves 20x the demand and reads are 10x slower. Stalls last 4 polls on
            // average and are 25 polls apart, so some come back within the hold and some after BOOTSTRAP.
            long burst = (long) p.demand * 20, kept = 0;
            boolean stalled = false;
            int clear = 1000;
            for (int second = 0; second < 600; second++) {
                if (random.nextInt(stalled ? 4 : 25) == 0) stalled = !stalled;
                boolean open = p.limiter == UNLIMITED; // the limiter in the second this poll closes
                long before = p.policy.budget(), shutdown = p.policy.shutdownBudget();
                double baseline = p.policy.baselineReadMicros();
                p.poll(open ? burst : (long) Math.min(p.demand, p.limiter), open ? BASE * 10 : BASE, stalled);
                clear = stalled ? 0 : clear + 1;
                String at = "run " + run + " second " + second + " in " + p.policy.state();
                // Unlimited in the poll that sees the stall, and until 10 s lie between two polls without one.
                assertEquals(clear <= 10, p.limiter == UNLIMITED, at + ", polls without a stall " + clear);
                if (stalled && !open) {
                    stalls++;
                    kept = p.policy.budget();
                    assertTrue(kept >= before, at + ": the stall lowered the budget " + before + " -> " + kept);
                } else if (open) {
                    assertEquals(kept, p.policy.budget(), at + ": the budget moved while the limiter was unlimited");
                    assertEquals(shutdown, p.policy.shutdownBudget(), at);
                    assertEquals(baseline, p.policy.baselineReadMicros(), 0, at + ": the baseline moved");
                    if (p.limiter != UNLIMITED) { closings++; assertEquals(kept, p.limiter, at); }
                }
                // Limited windows never moved more than the demand: a larger budget was learned from the burst.
                assertTrue(p.policy.budget() <= p.demand * 2, at + ": budget " + p.policy.budget() + " for a demand of " + p.demand);
                assertEquals(0, p.policy.harmCeilingBytesPerSecond(), at + ": the slow reads of a burst are not harm");
            }
            for (int i = 0; i < 11; i++) p.poll(p.limiter == UNLIMITED ? burst : (long) Math.min(p.demand, p.limiter), BASE, false);
            for (int i = 0; i < 40; i++) p.window();
            String at = "run " + run;
            assertEquals(State.TRACKING, p.policy.state(), at);
            assertEquals(BASE, p.policy.baselineReadMicros(), 1, at);
            assertTrue(p.policy.budget() >= p.demand && p.policy.budget() <= p.demand * 2, at + ": budget " + p.policy.budget());
        }
        assertTrue(stalls >= 2000 && closings >= 1000, "stalls " + stalls + ", closings " + closings);
    }

    @Test void aDeviceWithAFixedCapacityIsProbedRarely() {
        // Unlimited demand over a fixed capacity: the budget must come back to the capacity to find out whether it
        // moved, and that costs slow reads. The price is bounded: slow windows and changes of direction per window.
        for (int run = 0; run < 100; run++) {
            var random = new Random(3000 + run);
            var p = new Plant(seedOf(random));
            p.demand = Double.MAX_VALUE;
            p.capacity = p.seed * (2 + 30 * random.nextDouble());
            for (int i = 0; i < 40; i++) p.window(); // the search from the floor
            int slow = 0, turns = 0, direction = 0, windows = 600;
            long last = p.policy.budget();
            double moved = 0;
            for (int i = 0; i < windows; i++) {
                p.window();
                if (p.slow()) slow++;
                moved += p.rate;
                long budget = p.policy.budget();
                int step = Long.compare(budget, last);
                if (step != 0) { if (direction != 0 && step != direction) turns++; direction = step; }
                last = budget;
            }
            String at = "run " + run + " capacity/seed " + p.capacity / p.seed;
            assertTrue(slow <= windows / 8, at + ": slow windows " + slow);
            assertTrue(turns <= windows / 8, at + ": changes of direction " + turns);
            assertTrue(moved / windows >= p.capacity * .9, at + ": mean rate " + moved / windows / p.capacity + " of capacity");
        }
    }
}
