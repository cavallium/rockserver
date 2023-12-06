package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

public interface NamedColumnConfig extends FallbackColumnConfig {

	String name() throws GestaltException;

}
