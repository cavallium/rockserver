package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

public interface MetricsConfig {

	InfluxMetricsConfig influx() throws GestaltException;

	JmxMetricsConfig jmx() throws GestaltException;

}
