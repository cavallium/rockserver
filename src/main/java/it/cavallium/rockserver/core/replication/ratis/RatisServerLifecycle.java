package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;

public interface RatisServerLifecycle extends AutoCloseable {

    RatisAdapterBoundary boundary();

    void start() throws IOException;

    @Override
    void close() throws IOException;
}
