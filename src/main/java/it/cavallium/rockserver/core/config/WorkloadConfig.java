package it.cavallium.rockserver.core.config;

import java.time.Duration;
import org.github.gestalt.config.exceptions.GestaltException;

/** Explicit workload scheduler and bounded-operation tuning. */
public interface WorkloadConfig {

	Integer latencyQueueCapacity() throws GestaltException;

	Integer ingestQueueCapacity() throws GestaltException;

	Integer cdcQueueCapacity() throws GestaltException;

	Integer analyticalQueueCapacity() throws GestaltException;

	Integer batchQueueCapacity() throws GestaltException;

	Integer controlQueueCapacity() throws GestaltException;

	Integer physicalMaintenanceQueueCapacity() throws GestaltException;

	Integer readLatencyReservation() throws GestaltException;

	Integer readIngestReservation() throws GestaltException;

	Integer readCdcReservation() throws GestaltException;

	Integer writeLatencyReservation() throws GestaltException;

	Integer writeIngestReservation() throws GestaltException;

	Integer writeCdcReservation() throws GestaltException;

	Integer controlThreads() throws GestaltException;

	Integer physicalConcurrency() throws GestaltException;

	Integer analyticalActiveLimit() throws GestaltException;

	Integer retainedAnalyticalSnapshots() throws GestaltException;

	Duration retainedSnapshotMaximumAge() throws GestaltException;

	Integer latencyBurst() throws GestaltException;

	Integer ingestDrrWeight() throws GestaltException;

	Integer cdcDrrWeight() throws GestaltException;

	Integer analyticalDrrWeight() throws GestaltException;

	Integer batchDrrWeight() throws GestaltException;

	Integer competingBatchReadMaximumActive() throws GestaltException;

	Integer competingBatchWriteMaximumActive() throws GestaltException;

	Duration competingBatchWriteInterval() throws GestaltException;

	Integer pressuredBatchMaximumActive() throws GestaltException;

	Duration pressuredBatchInterval() throws GestaltException;

	Integer rangeQuantumMaxItems() throws GestaltException;

	DataSize rangeQuantumMaxBytes() throws GestaltException;

	Duration rangeQuantumMaxDuration() throws GestaltException;

	Integer rawScanFileConcurrency() throws GestaltException;

	DataSize rawScanReadaheadBytes() throws GestaltException;

	Integer cdcQuantumMaxMutations() throws GestaltException;

	DataSize cdcQuantumMaxBytes() throws GestaltException;

	Duration cdcQuantumMaxDuration() throws GestaltException;

	Integer latencyRangeMaxItems() throws GestaltException;

	DataSize latencyRangeMaxBytes() throws GestaltException;

	Integer latencyFanOutMaxItems() throws GestaltException;

	DataSize latencyFanOutMaxBytes() throws GestaltException;
}
