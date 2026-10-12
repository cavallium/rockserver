package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

/** Physical storage domains are explicit; workload names describe feedback cohorts. */
public interface StartupIoConfig {
    boolean enabled() throws GestaltException;
    @Nullable String defaultStorageId() throws GestaltException;
    @Nullable StartupStorageConfig[] storageGroups() throws GestaltException;
}
