package it.cavallium.rockserver.core.cluster.internal;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public final class StrictProperties {

    private StrictProperties() {
    }

    public static Properties load(Reader reader, String description) throws IOException {
        requireArgument(reader, "reader");
        var loaded = new DuplicateRejectingProperties(description);
        loaded.load(reader);

        var properties = new Properties();
        properties.putAll(loaded);
        return properties;
    }

    private static final class DuplicateRejectingProperties extends Properties {

        private final String description;

        private DuplicateRejectingProperties(String description) {
            this.description = requireArgument(description, "description");
        }

        @Override
        public synchronized Object put(Object key, Object value) {
            if (containsKey(key)) {
                throw new IllegalArgumentException("Duplicate " + description + " key: " + key);
            }
            return super.put(key, value);
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
