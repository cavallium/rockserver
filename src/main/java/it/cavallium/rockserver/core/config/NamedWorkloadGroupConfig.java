package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

/** Independent admission pools; omitted settings inherit global parallelism/workload settings.
 * Workload overrides cover queues, workers, reservations, weights and batch admission.
 * Retained native state and bounded-operation settings remain database-wide. */
public interface NamedWorkloadGroupConfig {
	String name() throws GestaltException;
	@Nullable Integer read() throws GestaltException;
	@Nullable Integer write() throws GestaltException;
	@Nullable WorkloadConfig workload() throws GestaltException;
}
