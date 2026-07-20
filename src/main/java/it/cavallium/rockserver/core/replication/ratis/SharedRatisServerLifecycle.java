package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;

public final class SharedRatisServerLifecycle implements RatisServerLifecycle {

    private final RatisServerLifecycle delegate;
    private int references;
    private boolean delegateCleanupRequired;

    public SharedRatisServerLifecycle(RatisServerLifecycle delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
    }

    @Override
    public RatisAdapterBoundary boundary() {
        return delegate.boundary();
    }

    @Override
    public synchronized void start() throws IOException {
        if (references == 0) {
            if (delegateCleanupRequired) {
                throw new IllegalStateException("Shared Ratis lifecycle cleanup is pending");
            }
            delegateCleanupRequired = true;
            delegate.start();
        }
        references++;
    }

    @Override
    public synchronized void close() throws IOException {
        if (references > 1) {
            references--;
            return;
        }
        if (!delegateCleanupRequired) {
            return;
        }
        references = 0;
        delegate.close();
        delegateCleanupRequired = false;
    }

    public synchronized int activeReferences() {
        return references;
    }

}
