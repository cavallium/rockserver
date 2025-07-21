package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface MetricsConfig {

	@Nullable
	String databaseName() throws GestaltException;

	InfluxMetricsConfig influx() throws GestaltException;

	JmxMetricsConfig jmx() throws GestaltException;

}
