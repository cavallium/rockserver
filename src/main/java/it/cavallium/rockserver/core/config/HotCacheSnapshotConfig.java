package it.cavallium.rockserver.core.config;

import java.nio.file.Path;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

/** Snapshots use independent DATA/METADATA policies and exclusive cache namespaces. */
public interface HotCacheSnapshotConfig {
    @Nullable Boolean enabled() throws GestaltException;
    @Nullable Path directory() throws GestaltException;
    @Nullable CacheSnapshotPolicyConfig data() throws GestaltException;
    @Nullable CacheSnapshotPolicyConfig metadata() throws GestaltException;
}
