# Adaptive compaction I/O budget

Code: `CompactionIoBudget` (the policy, single-threaded, no I/O), `CompactionIoController` (its thread, the
RocksDB `RateLimiter`, the gauges), `EmbeddedDBStorageMetrics.sampleCompactionIo` (the inputs).
Enabled by `database.global.adaptive-compaction-io=true`.

**Not validated on a real RocksDB under production load.** Everything below is pinned by unit tests on synthetic
traces, by properties over a small model of a server, and by integration tests that check the plumbing against a
real database (not its behaviour under load). See "Not verified" and "What to watch".

## What the limiter is for

One RocksDB `RateLimiter` in `ALL_IO` mode limits the bytes per second of flush and compaction. Its rate (the
*budget*) is the only thing this controller sets (plus the compaction readahead size, not described here).
Too high a budget lets compaction slow foreground reads; too low a budget delays flushes and lets compaction debt
grow into a write stall. The controller polls once per second and decides once per 5 s *window*.

Terms. *Seed*: the rate the limiter is built with, write buffer / 5 (one memtable flushes in about 5 s); never under
40,960 B/s. *Achieved*: bytes/s that actually went through the limiter in a window. *Saturated*: achieved >= 80% of
the budget (the limiter was the bottleneck). *Baseline*: the reference point-read mean (bulk reads have their own, per
key). *Slow*: a window with background I/O whose point mean is over 1.2x its baseline, or whose bulk mean per key is
over 1.2x its baseline. *Debt*: 0..1, the worst column's max(pending compaction bytes / soft pending limit,
L0 files / L0 slowdown trigger); 1 is where RocksDB starts to delay writes. *Write stall*: RocksDB stops writes
(`rocksdb.is-write-stopped`) or delays them (`rocksdb.actual-delayed-write-rate` != 0); `Sample.stopped`,
`Sample.writeDelayed`. *Urgent pressure* (`Sample.urgentPressure`): a write stall, or a column with L0 files at its
slowdown trigger, or (max write buffers <= 3) a column one immutable memtable short of a stop, which with the default
of 2 write buffers is every flush in progress.

## The control law

Two questions, two inputs. **Capacity** is learned only from saturated windows. **Harm** is evidence about the rate
in use, whatever the budget: slow reads lower the budget from the *achieved* rate, saturated or not.

| State (gauge value) | Event at the end of a window | Action |
|---|---|---|
| BOOTSTRAP (0) | first window with background I/O | baseline = its point mean; budget = achieved if saturated, else unchanged; -> TRACKING, "calibrating" |
| TRACKING (1) | saturated, reads healthy, no harmful rate known | budget = 2 x achieved |
| | saturated, reads healthy, harmful rate *c* known | +10% of *c* per window up to 0.9 *c*, then +5% of *c* every 6th window; never over 1.25 x achieved |
| | slow window right after a doubling | doubling taken back, 3 windows without growth; *c* = that rate only if the retried doubling is taken back too |
| | 2 consecutive slow windows | budget = max(floor, 0.7 x achieved) -> PROBE |
| | reads healthy, saturated, calibrating (or read pool saturated), 12 windows since the last trial | trial: budget = max(floor, 0.75 x achieved) -> PROBE |
| | new rate would not be 10% under achieved | no reduction. At the seed: slow reads become the baseline, retry in 12 windows. At a floor raised by debt: nothing is accepted |
| | not saturated, reads healthy | nothing |
| | no background I/O | baseline moves 20% towards the window's mean |
| PROBE (2) | 1 settle window, then 2 windows both with reads 10% faster and achieved 10% lower | reduction kept; *c* = the rate before it; baseline only falls. Still slow: next reduction at once |
| | a measured window without that | budget restored; baseline moves halfway to the current mean; after slow reads, 12 windows before the next reduction; calibration ends |
| any | poll under urgent pressure | -> RECOVERY (3): budget = max(seed, last known service, budget); x2 per saturated window while it lasts; never lowered |
| any | poll with a write stall | the budget as in the row above, then -> UNLIMITED (4): the limiter is set to 9,223,372,036,854 B/s in that poll; the budget is kept |
| UNLIMITED (4) | every poll | dropped: no window is formed from it, nothing is learned, the budget does not move (only the debt floor can raise it) |
| | 10 s between two polls without a write stall | limiter = the kept budget; -> RECOVERY, whose two windows start here |
| RECOVERY | 2 windows without pressure | -> BOOTSTRAP (baseline and *c* forgotten) |
| any | every valid poll | floor = seed for debt <= 0.5, linear to the last known service at debt 1; budget raised to the floor |

The controller writes the limiter only when the budget differs from its rate by 5% or more (always in RECOVERY and
UNLIMITED). `CompactionIoBudget.sample` returns the rate for the limiter; `budget()` and the `.budget` gauge are the
budget of the law, which in UNLIMITED is the rate the limiter returns to.

### Write stalls

Why unlimited and not a larger budget (reasoned from the RocksDB sources, not observed): a flush takes its limiter
priority once, when it starts (`db/flush_job.cc`): `IO_HIGH` unless writes are already stopped or delayed.
Compaction output files opened while writes are stopped or delayed get `IO_USER` (`db/compaction/compaction_job.cc`),
which the limiter always serves first. So a flush that started just before the stall gets no tokens for as long as
compaction asks for the whole budget, and the stall it has to clear does not clear. RECOVERY alone doubled the
budget once per saturated 5 s window: several windows with writes stopped. At the unlimited rate nothing waits.

- **When**: in the poll that sees the stall (polls are 1 s apart), from every state, also when the rest of the
  sample is unusable. Not for the other urgent pressure: there no priority is inverted, and with 2 write buffers
  it would switch the limiter off during every flush and the 10 s after it. That keeps the law of RECOVERY.
- **How long**: until 10 s lie between two polls that both saw no stall. A stall seen again in between starts the
  10 s again, so a flag that comes and goes changes the limiter twice per episode, not twice per poll.
- **Afterwards**: the budget RECOVERY set when the pressure began, which is the larger of the budget in force
  before the stall (from PROBE: the one before the reduction), the seed and the last background service measured
  under the limiter. Not the floor. Not the rate reached without a limit: that is the device with reads starved,
  and no window of it saturated a limiter.
- **Learning**: polls over an unlimited interval are dropped before any window is formed (the poll that ends the
  hold too: the second it closes was still unlimited). Capacity, known service, shutdown budget, point and bulk
  baselines and references, tail envelopes, the readahead service rate and the harmful rate *c* see none of it.
- **Harm detection around it**: none while unlimited (reads are expected to be slow). After the hold RECOVERY
  compares nothing and lowers nothing for at least two windows; BOOTSTRAP then takes a new baseline from the first
  window with background I/O, 10 s or more after the limiter came back. The baseline from before the stall is
  never used again, so the slow reads of the backlog draining are not harm; if they enter the new baseline, the
  calibration trials after BOOTSTRAP and the baseline's free fall correct it as after any RECOVERY.

### Constants and why

| Constant | Value | Reason |
|---|---|---|
| decrease on harm | x0.7 of achieved | From the rate in use, as TCP cuts from the flight size, not from an unused window. CUBIC's factor; Reno's 0.5 gives up half a device that is marginally over. Must be well over the 10% a kept reduction has to show. |
| slow windows before a decrease | 2 | One 5 s mean is noise (tests feed +-18%); two are 10 s of slow reads. |
| kept reduction | reads 10% faster and rate 10% lower, 2 windows after 1 settle | Slow reads that do not answer to the rate are a workset or media change, not the limiter's business. |
| cooldown after a reduction that did not help | 12 windows (60 s) | Existing value; bounds the cost of asking again to 3 windows in 15. |
| hold after a decrease | 3 windows | The settle window and the two measured ones; no decrease is answered by growth in the next window. |
| approach to the harmful rate *c* | +10% of *c* per window up to 0.9 *c* | Back near the old rate in 2 windows. Not up to *c* itself: that returns to the rate that just harmed, in a loop. |
| probing past 0.9 *c* | +5% of *c* every 6th window | Capacity may have moved; finding out costs slow reads, so it is done every 30 s, not every 5 s. |
| *c* forgotten | 120 windows (10 min) with background I/O and no 2 consecutive slow windows; RECOVERY | Old evidence; doubling resumes. |
| trial from a healthy state | x0.75, only while calibrating or the read pool is saturated | Existing factor. BOOTSTRAP takes the baseline with background I/O running; only a lower rate shows harm already in it. |
| healthy tolerance | 1.2 x baseline; the baseline may rise to 1.2 x its lowest level | Existing (aeede04). Effective tolerance while probing: up to 1.44x. |
| debt floor | from debt 0.5, linear to the last known service at 1.0 | Proportional opening before the stall instead of only at it; 0.5 leaves the lower half of the debt range to latency. |
| dead band | 5% | Every decrease of the law is >= 10%; only slow upward movement is batched. |
| unlimited rate | 9,223,372,036,854 B/s (`Long.MAX_VALUE / 1,000,000`) | The native limiter computes rate x refill period in microseconds / 1,000,000 in 64 bits: no overflow for any period up to 1 s without relying on its guard (here 100 ms: 859 GiB per refill). Never 0: the native setter only asserts it and then divides by it. `budget()` and `sample` are never under 40,960. |
| what opens the limiter | writes stopped or delayed, nothing else | The two write-controller states in which RocksDB raises compaction to `IO_USER`. |
| hold after a stall | 10 s between two polls without one | Two windows, what RECOVERY asks before it ends. RocksDB re-evaluates the stall at every flush or compaction that completes, so near the trigger it flickers within seconds; closing on the first clear poll throttles the jobs that are clearing it. |

## Invariants and the tests that pin them

`T` = `CompactionIoBudgetTest`, `I` = `CompactionIoBudgetInvariantsTest` (randomized, seeded), `R` =
`CompactionReadaheadTest`, `G` = `CompactionIoIntegrationTest` (real RocksDB).

| Invariant | Test |
|---|---|
| seed <= floor <= budget <= MAX at every poll, also with random debt, pressure and invalid samples | T `noTraceTakesTheBudgetUnderTheSeed`; I (every window of every property) |
| A window that did not saturate the limiter never lowers the budget while reads are healthy; one slow window lowers it at most to the level before the last doubling | I `aWindowThatDidNotSaturate...`; T `aDemandLimitedWindowWithoutSustainedSlowReadsChangesNothing`, `tinyFirstActiveWindowDoesNotCollapseTheBudget` |
| Slow reads under a half-used budget lower it from the achieved rate | T `slowReadsUnderAHalfUsedBudgetLowerItFromTheAchievedRate` |
| Slow reads that answer to the rate: healthy again within 2 + 4 x ceil(log(rate / 1.2 capacity) / log(1/0.7)) windows; no decrease under 0.7 x capacity | I `slowReadsThatAnswerToTheRate...` (279 traces with harm, 170 under a budget used <= 60%) |
| When the cause goes away: the budget never falls, is back at the last harmful rate within 18 windows and at the rate before the harm within 128 | same test |
| Slow reads that do not answer to the rate lower nothing for good and are accepted (2 reductions, both undone, about 20 windows) | T `aWorksetShiftIsAcceptedAfterDecreasesThatDidNotHelp`, `mediaShiftWithForegroundProgressDoesNotBackOffPermanently`, `highLatencyWithoutProbeBenefit...` |
| No back-and-forth: constant load, reads 12-18% over the first window: at most 2 changes of direction in 400 windows; with isolated 1.6x windows and limited demand the budget rests within 60 windows | I `aConstantLoadWithReadsJustUnderTheThreshold...` |
| Fixed capacity, unlimited demand: at most 1 window in 8 slow, 1 in 8 changes direction, mean rate >= 90% of capacity | I `aDeviceWithAFixedCapacityIsProbedRarely` |
| Urgent pressure always wins: every poll under pressure ends in RECOVERY, or in UNLIMITED for a write stall, with a budget not under the one before | T `urgentPressureAlwaysWins` |
| A poll with writes stopped or delayed returns the unlimited rate: at any poll of a window, from every state, with an unusable sample. The dead band does not hold it back and a real limiter takes the value (single burst = rate / 10) | T `aStopOrADelayAtAnyPollOfAWindowOpensTheLimiterInThatPoll`; R `aWriteStallOpensTheRealLimiterInThePollThatSeesIt...`, `limiterIsNotRewritten...` |
| The limiter stays unlimited while the stall lasts and for 10 clear polls after it; a stall every other poll, or back within the hold, never closes it; 2 changes of the limiter per stall that ends | T `theLimiterStaysOpenWhileTheStallLasts...`, `aStallThatComesAndGoes...`, `anUnusablePollDoesNotCloseAnOpenLimiterBeforeTheHold`; I `aWriteStallOpensTheLimiterAtOnceAndTeachesNothing` |
| After the hold the limiter is at the budget in force before the stall (from PROBE: before the reduction; from RECOVERY: what it had reached; from a new database: the seed) | T `afterAStallTheLimiterReturnsToTheBudgetInForceBeforeIt` |
| Unlimited windows (60 MB/s under a 16 MB/s budget, reads 20x slower) leave budget, known service, shutdown budget, baseline and *c* unchanged; reads 3x slower in the windows after lower nothing; the budget never exceeds 2 x the demand of limited windows | T `nothingIsLearnedWhileTheLimiterIsUnlimitedNorFromTheReadsRightAfter`; I same (over 2000 stalls) |
| What goes to the limiter is the budget or the unlimited rate: never under the seed, never 0 | T `noTraceTakesTheBudgetUnderTheSeed`; I (every poll) |
| The baseline does not follow a slow degradation, and noise is not degradation | T `aRampOfPointLatencyIsNotFollowedAsHealthy`, `noisyPointMeansAreNotTakenForDegradation` |
| Rising debt raises the floor monotonically; slow reads cannot go under it; falling debt returns it to the seed | T `risingCompactionDebt...` (2), `slowReadsCannotLowerTheBudgetUnderTheDebtFloor`, `fallingCompactionDebt...`, `unsampledOrInvalidDebtIsNoDebt` |
| The sampled debt of a real database is L0 files / trigger (0 when empty) | G `healthyNativeSamplerReadsL0FilesAndPollsWithoutFailures` |
| The limiter is written only for changes >= 5% and is never more than 5% under the budget | R `limiterIsNotRewrittenForBudgetChangesUnderFivePercent` |

## What changed on 2026-11-12

| Commit | Defect | Change |
|---|---|---|
| 57663e5 | Production: budget 40,960 B/s for 12 minutes after a tiny first flush, snapshot flush of 206-229 s. BOOTSTRAP took the first window's demand as the budget; growth was +10% per window (90 windows from the floor to 200 MB/s). | A window that did not saturate the limiter no longer lowers the budget; a saturated healthy one doubles it. |
| 35c392c | After growth, only a trial every 12 windows could lower the budget. | The slow window after a growth step takes the step back. |
| 25d0f4a | A trial ran at 75% of a demand-limited rate, under the flush-safe rate; pressure could lower the budget. | No trial from a window under 80% of the budget; nothing under the seed. |
| 4a40d4a | Introduced by 25d0f4a: slow reads over a background under the seed were never accepted. | They become the baseline again (the limiter cannot go lower). |
| aeede04 | The baseline followed any degradation under 2% per window without end. | It may rise to at most 1.2x its lowest level. |
| 975de53 | After a doubling the budget is 2x the rate in use, no trial starts under 80%: slow reads lowered nothing; the limiter was a fixed cap. | 2 consecutive slow windows: budget = 0.7 x achieved, at any utilisation, verified in PROBE; a kept decrease does not raise the baseline. |
| 6e138cf | Growth doubled straight through a rate just found harmful; one slow window switched the growth law. | Two phases around the remembered harmful rate; 3 windows of hold after a decrease. |
| 88ddd16 | A trial every 12 saturated windows for ever, with healthy reads. | Trials only to calibrate the baseline after BOOTSTRAP (while each helps) or while the read pool is saturated. |
| 36475e9 | A second stall, arriving from BOOTSTRAP, could lower a budget the first stall had doubled. | Entering RECOVERY never lowers the budget. |
| 53f49e3 | Introduced by 6e138cf: growth stalled one byte under 90% of the harmful rate. | Whole-byte levels. Found by the randomized properties. |
| 7021773 | 6e138cf: one noisy window after a doubling was remembered as a harmful rate for 10 minutes. | Remembered only when the retried doubling is taken back too. |
| 64dc4db | (tests) | The invariants above as randomized properties. |
| a80abf9 | The floor was the constant seed; debt acted only as a stall. | Floor proportional to compaction debt; `Sample.compactionDebt`. |
| 6c9e3d0 | The limiter was rewritten every second for any change. | 5% dead band in the controller. |
| 6c25b18 | Review of the native side (reasoned, not observed): during a write stall compaction output is `IO_USER`, a flush that started before it stays `IO_HIGH` and gets no tokens while compaction asks for the budget; RECOVERY doubled the budget only once per saturated window. | Writes stopped or delayed: limiter unlimited in that poll, for the stall and 10 s after it; nothing learned from those polls; the budget from before returns. New state UNLIMITED (4), `Sample.writeDelayed`. |

## Literature (from the survey requested by the owner; the sources were not re-read for this file)

- Latency threshold driving a rate up and down is what IOCost (ASPLOS'22) and PARDA do; RocksDB's own auto-tuned
  limiter follows demand only and has no harm signal. Nothing found supersedes it.
- No production engine uses latency alone: Pebble/CockroachDB admission control (L0 sub-levels/files), TiKV flow
  control (pending compaction bytes), ScyllaDB (backlog -> shares), S-RocksDB (PVLDB 2026: backlog, L0, stall flags,
  hysteresis). SILK (ATC'19): plain limiting postpones compaction into later pile-ups. -> the debt floor (a80abf9).
- SILK changes the limit only for moves over a threshold. -> the dead band (6c9e3d0).
- Already in the design: no learning from demand-limited windows (IOCost's budget-deficiency gate); a baseline that
  does not drift with slow degradation; growth only in a healthy state (S-RocksDB).

Considered, not done:
- **Tail instead of mean** (IOCost uses p90). `pointTailMicros` / `bulkTailMicros` are the *maximum* latency since the
  previous poll (about 1 s), not a percentile: too noisy to decide a decrease, and the code uses them only to switch
  readahead off. A p90/p99 needs a histogram in the point-read completion path.
- Growth driven by debt beyond the last known service; removing PROBE altogether (CompactionBulkFeedbackTest pins the
  bulk and queue lanes through it); `strict_bytes_per_sync` (writeback bursts bypass the limiter: a configuration
  to evaluate); an IOPS cap for cloud volumes (Calcspar); charging user reads at `IO_USER`; BBR-style model-based
  control (no storage validation found); throttling foreground admission (a different actuator; the workload
  scheduler covers it).

## Not verified, and known limits

- No run against a real RocksDB under load, in production or in the benchmark harness. The properties use a model
  in which read latency rises in proportion to the background rate above a capacity.
- Harm with a cliff (a 30% cut that makes reads less than 10% faster) is treated as a workset change: restored, and
  accepted as the baseline after two attempts.
- Effective tolerance up to 1.44x, not 1.2x, while the budget probes slowly upward (the baseline rises under it).
- With adaptive readahead on, slow reads first set readahead to 0 and no byte-rate reduction starts while that
  settles (60-180 s).
- Harm already in the BOOTSTRAP baseline that the calibration trials miss is found only in a window without
  background I/O.
- The debt floor cannot exceed the service once measured: a budget that never left the seed opens only at the stall.
  Debt 1 depends on each column's soft pending compaction limit and L0 slowdown trigger; those values were not reviewed.
- The limiter can be up to 5% under the budget gauge.
- Write stalls: the starvation of a flush behind `IO_USER` compaction was not reproduced, and no test stalls a
  real database to watch the limiter open (the controller test drives a real limiter with scripted samples). The
  stall is seen by a 1 s poll plus up to one 100 ms refill; a stall shorter than a poll can be missed.
- Unlimited means unprotected: for the stall and 10 s after it compaction and flush take what the device gives, and
  reads are as slow as that makes them. The compaction readahead size is not changed by this (a stop still sets it to 0).
- The limiter returns to the budget that preceded the stall. If that budget is what let the debt grow, the stall
  comes back; between stalls only the debt floor and the doubling of saturated windows raise it. A budget learned
  from the burst would end that loop and is deliberately not taken (it is not a rate at which reads were healthy).
- A poll whose sampling failed reports no stall; 10 s of such polls close the limiter at the kept budget (>= seed).

## What to watch when deploying

Gauges (tag `db`): `rockserver.compaction.io.budget`, `.state` (0 BOOTSTRAP, 1 TRACKING, 2 PROBE, 3 RECOVERY, 4 UNLIMITED),
`.floor`, `.debt`, `.harm.ceiling`, `.point.read.micros`, `.baseline.read.micros`, `.adjustment.failures`; plus
pending compaction bytes, L0 file counts and write stalls / delayed write rate.

- Budget at the seed for minutes while debt or pending compaction bytes grow: the 57663e5 symptom. `.debt` over 0.5
  with `.floor` still at the seed means no service above the seed was ever measured.
- `.state` at 2 more than about a fifth of the time, or alternating 1/2 with a budget that returns to the same value:
  reads are slow and do not answer to the rate (see the baseline moving up in steps).
- `.state` = 3 at all: urgent pressure (with 2 write buffers a flush in progress is enough); check `.debt` and `.floor`.
- `.state` = 4: writes were stopped or delayed within the last 10 s and the limiter is open; `.budget` is the rate it
  returns to, not the rate in force. Check what `.debt` and `.floor` did in the minute before. Returning to 4 every
  20-60 s with the same `.budget` in between: that budget is too low for the write load (see the limits above).
- `.point.read.micros` over 1.4 x `.baseline.read.micros` for more than 3 windows without a decrease: a reduction is
  blocked (readahead settling, cooldown) or the budget is at the floor.
- `.harm.ceiling` set and never 0 for hours with a budget well under it: harm keeps recurring at that rate.
