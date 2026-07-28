package it.cavallium.rockserver.core.common;

/**
 * The service guarantee requested for a Rockserver operation.
 *
 * <p>A workload profile describes <em>why</em> work is being performed and the
 * progress/isolation guarantee it needs. It does not describe the physical cost of
 * the operation; Rockserver derives that independently as an {@link OperationFamily}.
 * The canonical selection matrix and decision tree live in
 * {@code docs/workload-profiles.md}.</p>
 *
 * <p>Clients may request only {@link #LATENCY}, {@link #ANALYTICAL},
 * {@link #INGEST}, or {@link #BATCH}. {@link #CONTROL}, {@link #CDC}, and
 * {@link #PHYSICAL_MAINTENANCE} are protected profiles assigned by Rockserver.</p>
 *
 * <table class="striped">
 *   <caption>Canonical workload contract</caption>
 *   <thead><tr><th>Profile</th><th>Selection rule</th><th>Guarantee</th><th>Examples</th><th>Must not contain</th></tr></thead>
 *   <tbody>
 *   <tr><td>CONTROL</td><td>Short server-owned resource-release or liveness work</td><td>Isolated bounded capacity; never blocked by data work</td><td>Iterator close, rollback, cancellation, shutdown leases</td><td>User queries, scans, bulk mutations</td></tr>
 *   <tr><td>LATENCY</td><td>Request-bound work expected inside the normal API latency budget</td><td>Fast admission or immediate overload</td><td>Point lookup, first/last seek, bounded multi-get, metadata estimate</td><td>Full counts, unbounded ranges, backfills</td></tr>
 *   <tr><td>ANALYTICAL</td><td>Caller waits for an intrinsically expensive query</td><td>Capped guaranteed progress; caller cancellation stops it</td><td>Exact count, full aggregation, operator-requested statistics</td><td>Periodic metrics, migrations, unattended jobs</td></tr>
 *   <tr><td>INGEST</td><td>Sustained live pipeline whose lag affects primary freshness or durability</td><td>Reserved progress and bounded backlog</td><td>Crawler writes, transactional updates, live monitor persistence</td><td>Historical replay, cleanup, ad-hoc statistics</td></tr>
 *   <tr><td>CDC</td><td>WAL discovery, publication, polling, or acknowledgement</td><td>Independent lag SLO and reserved progress across every stage</td><td>CDC pages, cursor resume, commit, required WAL flush</td><td>Ordinary scans or generic maintenance</td></tr>
 *   <tr><td>BATCH</td><td>Retryable or deferrable work with no waiting caller</td><td>Uses spare capacity, pauses under pressure, eventually progresses</td><td>Backfill, migration, cleanup, scheduled exact statistics</td><td>Live ingestion or synchronous operator queries</td></tr>
 *   <tr><td>PHYSICAL_MAINTENANCE</td><td>Explicit physical database servicing</td><td>Serialized and admitted only when storage policy permits</td><td>Manual flush and compaction</td><td>CDC-required flush, logical cleanup</td></tr>
 *   </tbody>
 * </table>
 *
 * <p>Decision tree, in order: (1) CDC lifecycle or WAL work is CDC; (2) resource
 * release, rollback, or cancellation is CONTROL; (3) explicit manual flush or
 * compaction is PHYSICAL_MAINTENANCE; (4) a live primary or derived pipeline is
 * INGEST; (5) a bounded request expected within the normal deadline is LATENCY;
 * (6) expensive work with a waiting caller is ANALYTICAL; (7) retryable or unattended
 * work is BATCH. If none applies, its service contract must be defined before merge.</p>
 *
 * <p>An exact heavy count is ANALYTICAL when a caller waits, BATCH when periodic or
 * retryable, and LATENCY only when it uses a bounded metadata estimate such as
 * {@code estimateNumKeys}.</p>
 */
public enum WorkloadProfile {

	/** Short server-owned work required to release resources or preserve liveness. */
	CONTROL(1, false),

	/** Request-bound work expected to complete inside the normal API latency budget. */
	LATENCY(2, true),

	/** An intrinsically expensive query whose caller waits for the result. */
	ANALYTICAL(3, true),

	/** A sustained live pipeline whose lag affects primary freshness or durability. */
	INGEST(4, true),

	/** WAL discovery, publication, polling, acknowledgement, or required WAL flush work. */
	CDC(5, false),

	/** Retryable or deferrable work with no waiting caller. */
	BATCH(6, true),

	/** Explicit manual physical database servicing such as flush or compaction. */
	PHYSICAL_MAINTENANCE(7, false);

	private final int wireValue;
	private final boolean clientSelectable;

	WorkloadProfile(int wireValue, boolean clientSelectable) {
		this.wireValue = wireValue;
		this.clientSelectable = clientSelectable;
	}

	/** Stable numeric value shared by protobuf and Thrift. */
	public int wireValue() {
		return wireValue;
	}

	/** Decode one of the seven stable wire values without relying on enum order. */
	public static WorkloadProfile fromWireValue(int wireValue) {
		return switch (wireValue) {
			case 1 -> CONTROL;
			case 2 -> LATENCY;
			case 3 -> ANALYTICAL;
			case 4 -> INGEST;
			case 5 -> CDC;
			case 6 -> BATCH;
			case 7 -> PHYSICAL_MAINTENANCE;
			default -> throw new IllegalArgumentException("Unknown workload profile: " + wireValue);
		};
	}

	/** Whether an external caller is allowed to put this profile in a request context. */
	public boolean isClientSelectable() {
		return clientSelectable;
	}
}
