package it.cavallium.rockserver.core.config;

import java.nio.file.Path;
import java.time.Duration;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

/** Paths sharing hardware must use the same storage ID, including across DBs. */
public interface StartupStorageConfig {
    String id() throws GestaltException;
    @Nullable Integer maximumOpens() throws GestaltException;
    @Nullable Duration progressInterval() throws GestaltException;
    @Nullable Duration staleAfter() throws GestaltException;
    @Nullable Path[] paths() throws GestaltException;
    @Nullable String[] workloadGroups() throws GestaltException;
}
