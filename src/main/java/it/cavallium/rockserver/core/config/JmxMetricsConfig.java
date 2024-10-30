package it.cavallium.rockserver.core.config;

import java.time.Duration;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;

public interface JmxMetricsConfig {

	boolean enabled() throws GestaltException;

}
