# Scheduler bounded exhaustive state proof

The scheduler has complementary seeded/property/fuzz coverage, but sampling cannot establish that
every reachable small state obeys the pressure and fairness contract. This suite therefore builds a
finite independent transition system and checks a bisimulation against the real
`WorkloadPressureController` after every action.

## Pressure-controller state space

`PressureControllerExhaustiveModel` is a plain semantic specification. It does not reuse production
permit masks, queue structures, allowance methods, or fairness helpers. Its state includes:

- pressure on/off and three pressure-generation transitions;
- READ/WRITE queued and independently dispatchable state;
- READ/WRITE competition membership, competition hold/expiry, and four membership changes;
- READ/WRITE preemption publication;
- one active READ or WRITE permit, its pressure/competition generation, finish, and cancellation;
- the last pressured completion, pressure pacing, competing-WRITE pacing, pending wakeup, direct
  cross-pool wake mask, and fair-turn eligibility.

The bounds are chosen around the production zero-progress failure rather than arbitrary trace length.
A global pressure cap of one is the exact topology that can stop all progress. Three pressure changes
cover `off -> on -> off -> on`, including completion of a permit from a stale pressure generation.
Four competition membership changes cover entry, removal into the hold interval, logical expiry and
generation advance, re-entry, and stale completion. Queue, dispatchability, and preemption states are
binary and therefore need no transition counter. One active permit is sufficient to prove the cap-one
incident, finish/cancel conservation, and fair handoff; higher-cap concurrency remains covered by the
existing pressure-controller tests.

Two logical time observations form a sound quotient for these tests: `EARLY=0` is before every
positive pacing deadline and `LATE=Long.MAX_VALUE` is after every finite deadline. Actual controller
deadlines use one-day durations, so wall-clock execution cannot cross them. Exact deadline values are
irrelevant to every available action once classified as absent, active hold, or finite pacing.

Breadth-first exploration clones every real controller state through a fail-closed list of mutable
fields, applies one action normally to each model/controller branch, and deduplicates the semantic
state. The mutable-field-shape test fails if production adds state without deliberately adding it to
the clone. Predecessor and action links retain the shortest path; any result, state, or invariant
divergence reports the shortest counterexample trace.

At source base `f14693a`, the graph saturates completely at depth 20:

- 176,128 unique reachable states;
- 4,165,120 checked transitions;
- every action kind exercised, including 557,056 starts, 147,456 finishes, 147,456 cancellations,
  567,296 competition actions, and 176,128 deadline-expiry observations.

Every state proves:

- active counter/token conservation and the pressured cap;
- no dead state when either pool is dispatchable at an eligible time;
- continued work when the peer is queued but nondispatchable;
- immediate bounded alternation after a completion when both pools are dispatchable;
- pressure and competition generation isolation;
- exact cancel/abort ownership release;
- deferred and direct wakeup agreement;
- competition and preemption publication agreement.

## One-pool arbitration complement

`OnePoolSchedulerArbitrationExhaustiveTest` uses real worker threads held behind explicit BATCH
barriers and compares dispatch against small independent queue models:

- all 6 submission permutations of three EDF tasks crossed with all 8 cancellation subsets
  (48 queue states), including equal-deadline sequence tie-breaking;
- every nonempty four-profile vector where INGEST, CDC, ANALYTICAL, and BATCH are absent, cost 1, or
  cost 5 (80 DRR queue/cost states);
- nine LATENCY tasks plus INGEST, proving that priority yields exactly after the configured burst;
- already-expired admission for all five data profiles, proving exactly one rejection and no run.

These scenarios end with exact scheduler conservation. There are no correctness sleeps: latches
establish worker/queue ordering, and bounded waits only fail a missing transition.

## Evidence boundary

This is a complete proof only for the explicitly bounded graph and enumerated arbitration spaces. It
does not prove unbounded state, cap-greater-than-one interleavings, all queue lengths/costs, native I/O
progress, deployed Yotsuba behavior, or performance. Those require the existing race, fuzz, mixed
workload, fresh-process performance, and production-observation gates.
