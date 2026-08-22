package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

/**
 * A named block-cache budget that column families can select.
 */
public interface BlockCacheConfig {

	String name() throws GestaltException;

	DataSize size() throws GestaltException;
}
