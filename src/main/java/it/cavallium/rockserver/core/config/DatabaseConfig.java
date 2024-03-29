package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

public interface DatabaseConfig {

	GlobalDatabaseConfig global() throws GestaltException;
}
