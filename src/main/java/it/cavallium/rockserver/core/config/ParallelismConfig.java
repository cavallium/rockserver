package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface ParallelismConfig {

	@Nullable
	Integer read() throws GestaltException;

	@Nullable
	Integer write() throws GestaltException;
}
