package it.cavallium.rockserver.core.config;

import org.jetbrains.annotations.Nullable;
import org.github.gestalt.config.exceptions.GestaltException;

/**
 * A named block-cache budget that column families can select.
 */
public interface BlockCacheConfig {

	String name() throws GestaltException;

	DataSize size() throws GestaltException;
	@Nullable DataSize metadataSize() throws GestaltException;
	/** Named-cache override; omitted fields inherit the global snapshot policy. */
	@Nullable HotCacheSnapshotConfig snapshot() throws GestaltException;
}
