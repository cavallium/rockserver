package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

public interface JmxMetricsConfig {

	boolean enabled() throws GestaltException;

}
