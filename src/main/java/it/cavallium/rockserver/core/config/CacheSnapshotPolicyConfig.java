package it.cavallium.rockserver.core.config;

import java.time.Duration;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

/** Explicit limits include index/framing, retired readers and temporary publication. */
public interface CacheSnapshotPolicyConfig {
    @Nullable DataSize diskBudget() throws GestaltException;
    @Nullable DataSize snapshotBudget() throws GestaltException;
    @Nullable Long collectorEntries() throws GestaltException;
    @Nullable DataSize indexRam() throws GestaltException;
    @Nullable DataSize maximumBlock() throws GestaltException;
    @Nullable DataSize lookupBuffer() throws GestaltException;
    @Nullable DataSize minimumFreeDisk() throws GestaltException;
    @Nullable Duration snapshotDeadline() throws GestaltException;
    @Nullable Duration period() throws GestaltException;
    @Nullable Integer sampleEvery() throws GestaltException;
    @Nullable Integer lookupConcurrency() throws GestaltException;
    @Nullable DataSize readCeiling() throws GestaltException;
    @Nullable DataSize writeCeiling() throws GestaltException;
}
