package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface InfluxMetricsConfig {

	boolean enabled() throws GestaltException;

	@Nullable String url() throws GestaltException;

	@Nullable String bucket() throws GestaltException;

	@Nullable String user() throws GestaltException;

	@Nullable String token() throws GestaltException;

	@Nullable String org() throws GestaltException;

	@Nullable Boolean allowInsecureCertificates() throws GestaltException;
}
