package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface MetricsConfig {

	@Nullable
	String databaseName() throws GestaltException;

	/**
	 * Tags every meter with {@code node} = this value when it is not blank: the members of a replicated cluster share one
	 * config, and their meters would otherwise be indistinguishable.
	 */
	@Nullable
	String nodeName() throws GestaltException;

	InfluxMetricsConfig influx() throws GestaltException;

    /** Expensive SST metadata reads, off by default. */
    boolean tablePropertiesEnabled() throws GestaltException;

    /** Minimum interval in seconds; collection is also limited by the statistics polling cadence. */
    long tablePropertiesIntervalSeconds() throws GestaltException;

	JmxMetricsConfig jmx() throws GestaltException;

}
