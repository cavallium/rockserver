package org.apache.ratis.server.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;

/** Pinned construction seam: close an allocated proxy when initGroups fails before ownership can be returned. */
public final class RockserverRaftServers {
    private RockserverRaftServers() {}
    public static RaftServer create(RaftPeerId id,RaftGroup seed,RaftStorage.StartupOption option,StateMachine.Registry registry,
            RaftProperties properties,Parameters parameters) throws IOException {
        Objects.requireNonNull(id); Objects.requireNonNull(option); Objects.requireNonNull(registry); Objects.requireNonNull(properties);
        if (seed!=null && !seed.getPeers().isEmpty() && seed.getPeer(id)==null) throw new IllegalArgumentException("Seed group does not contain this node");
        return initialize(new RaftServerProxy(id,registry,properties,parameters,null),server -> server.initGroups(seed,option));
    }
    @FunctionalInterface interface Initializer<T> { void initialize(T resource) throws IOException; }
    static <T extends Closeable> T initialize(T resource,Initializer<T> initialize) throws IOException {
        try { initialize.initialize(resource); return resource; }
        catch (IOException | RuntimeException | Error failure) {
            try { resource.close(); } catch (Throwable cleanup) { if (cleanup!=failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
}
