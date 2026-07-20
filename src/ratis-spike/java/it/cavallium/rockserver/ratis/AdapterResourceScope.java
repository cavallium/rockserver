package it.cavallium.rockserver.ratis;

import java.io.IOException;
import java.util.ArrayList;

final class AdapterResourceScope implements AutoCloseable {

    private final ArrayList<AutoCloseable> resources = new ArrayList<>();
    private boolean closed;

    <T extends AutoCloseable> T add(T resource) {
        if (resource == null) {
            return null;
        }
        if (closed) {
            throw new IllegalStateException("Ratis adapter resource scope is closed");
        }
        resources.add(resource);
        return resource;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        Error fatalFailure = null;
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception e) {
                if (fatalFailure != null) {
                    fatalFailure.addSuppressed(e);
                } else {
                    if (failure == null) {
                        failure = new IOException("Failed to close Ratis adapter resources");
                    }
                    failure.addSuppressed(e);
                }
            } catch (Error e) {
                if (fatalFailure == null) {
                    fatalFailure = e;
                    if (failure != null) {
                        fatalFailure.addSuppressed(failure);
                    }
                } else if (fatalFailure != e) {
                    fatalFailure.addSuppressed(e);
                }
            }
        }
        resources.clear();
        if (fatalFailure != null) {
            throw fatalFailure;
        }
        if (failure != null) {
            throw failure;
        }
    }
}
