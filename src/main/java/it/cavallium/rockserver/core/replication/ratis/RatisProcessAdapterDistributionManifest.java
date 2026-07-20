package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

public final class RatisProcessAdapterDistributionManifest {

    private static final String MAIN_CLASS_KEY = "mainClass";
    private static final String CLASSPATH_KEY = "classpath";
    private static final String CLASSPATH_PREFIX = "classpath.";
    private static final String JVM_ARGUMENT_PREFIX = "jvmArgument.";
    private static final String ENVIRONMENT_PREFIX = "environment.";
    public static final int MAX_DISTRIBUTION_MANIFEST_BYTES = 1024 * 1024;

    private RatisProcessAdapterDistributionManifest() {
    }

    public static RatisProcessAdapterDistribution read(Path manifestPath,
                                                       Path adapterHome,
                                                       Path javaExecutable) throws IOException {
        if (manifestPath == null) {
            throw new IllegalArgumentException("manifestPath must not be null");
        }
        return fromProperties(adapterHome, javaExecutable, RatisProcessAdapterProperties.read(
                manifestPath,
                MAX_DISTRIBUTION_MANIFEST_BYTES,
                "Ratis adapter distribution manifest"
        ));
    }

    public static RatisProcessAdapterDistribution fromProperties(Path adapterHome,
                                                                 Path javaExecutable,
                                                                 Properties properties) {
        if (adapterHome == null) {
            throw new IllegalArgumentException("adapterHome must not be null");
        }
        if (javaExecutable == null) {
            throw new IllegalArgumentException("javaExecutable must not be null");
        }
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        rejectUnknownKeys(properties);

        return new RatisProcessAdapterDistribution(
                adapterHome,
                javaExecutable,
                requiredMainClass(properties),
                classpathEntries(properties),
                indexedJvmArguments(properties),
                environment(properties)
        );
    }

    private static void rejectUnknownKeys(Properties properties) {
        for (var keyObject : properties.keySet()) {
            var key = keyObject.toString();
            if (key.equals(MAIN_CLASS_KEY)
                    || key.equals(CLASSPATH_KEY)
                    || key.startsWith(CLASSPATH_PREFIX)
                    || key.startsWith(JVM_ARGUMENT_PREFIX)
                    || key.startsWith(ENVIRONMENT_PREFIX)) {
                continue;
            }
            throw new IllegalArgumentException("Unknown Ratis adapter distribution manifest key: " + key);
        }
    }

    private static String requiredMainClass(Properties properties) {
        var value = properties.getProperty(MAIN_CLASS_KEY);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing Ratis adapter distribution manifest key: " + MAIN_CLASS_KEY);
        }
        return value;
    }

    private static List<Path> classpathEntries(Properties properties) {
        var generatedClasspath = properties.getProperty(CLASSPATH_KEY);
        if (generatedClasspath != null) {
            if (hasIndexedClasspathKeys(properties)) {
                throw new IllegalArgumentException(
                        "Ratis adapter distribution manifest must not mix classpath and indexed classpath keys"
                );
            }
            return splitClasspath(generatedClasspath);
        }
        return indexedKeys(properties, CLASSPATH_PREFIX, true).stream()
                .map(key -> classpathEntry(properties.getProperty(key), key))
                .toList();
    }

    private static boolean hasIndexedClasspathKeys(Properties properties) {
        for (var keyObject : properties.keySet()) {
            if (keyObject.toString().startsWith(CLASSPATH_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    private static List<Path> splitClasspath(String generatedClasspath) {
        if (generatedClasspath.isBlank()) {
            throw new IllegalArgumentException("Ratis adapter distribution manifest classpath must not be blank");
        }
        var entries = new ArrayList<Path>();
        for (var entry : generatedClasspath.split(Pattern.quote(File.pathSeparator), -1)) {
            if (entry.isBlank()) {
                throw new IllegalArgumentException("Ratis adapter distribution manifest classpath contains a blank entry");
            }
            entries.add(classpathEntry(entry, CLASSPATH_KEY));
        }
        return List.copyOf(entries);
    }

    private static Path classpathEntry(String value, String key) {
        if (value == null) {
            throw new IllegalArgumentException("Missing Ratis adapter distribution manifest key: " + key);
        }
        if (RatisProcessAdapterText.containsUnsafeLineCharacter(value)) {
            throw new IllegalArgumentException(
                    "Ratis adapter distribution manifest classpath entry must not contain NUL or line separators: "
                            + key
            );
        }
        if (value.indexOf(File.pathSeparatorChar) >= 0) {
            throw new IllegalArgumentException(
                    "Ratis adapter distribution manifest classpath entry must not contain the path separator: " + key
            );
        }
        return Path.of(value);
    }

    private static List<String> indexedJvmArguments(Properties properties) {
        return indexedKeys(properties, JVM_ARGUMENT_PREFIX, false).stream()
                .map(properties::getProperty)
                .toList();
    }

    private static List<String> indexedKeys(Properties properties, String prefix, boolean required) {
        var keys = new TreeMap<Integer, String>();
        for (var keyObject : properties.keySet()) {
            var key = keyObject.toString();
            if (!key.startsWith(prefix)) {
                continue;
            }
            keys.put(index(prefix, key), key);
        }
        if (required && keys.isEmpty()) {
            throw new IllegalArgumentException("Missing indexed Ratis adapter distribution manifest key: " + prefix + "0");
        }

        var result = new ArrayList<String>();
        for (int expected = 0; expected < keys.size(); expected++) {
            if (!keys.containsKey(expected)) {
                throw new IllegalArgumentException(
                        "Ratis adapter distribution manifest keys must be contiguous for prefix " + prefix
                );
            }
            result.add(keys.get(expected));
        }
        return List.copyOf(result);
    }

    private static int index(String prefix, String key) {
        var index = key.substring(prefix.length());
        if (index.isBlank()) {
            throw new IllegalArgumentException("Missing index for Ratis adapter distribution manifest key: " + key);
        }
        try {
            var parsed = Integer.parseInt(index);
            if (parsed < 0) {
                throw new IllegalArgumentException(
                        "Negative index for Ratis adapter distribution manifest key: " + key
                );
            }
            if (!Integer.toString(parsed).equals(index)) {
                throw new IllegalArgumentException(
                        "Non-canonical index for Ratis adapter distribution manifest key: " + key
                );
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid index for Ratis adapter distribution manifest key: " + key, e);
        }
    }

    private static Map<String, String> environment(Properties properties) {
        var environment = new TreeMap<String, String>();
        for (var keyObject : properties.keySet()) {
            var key = keyObject.toString();
            if (!key.startsWith(ENVIRONMENT_PREFIX)) {
                continue;
            }
            environment.put(key.substring(ENVIRONMENT_PREFIX.length()), properties.getProperty(key));
        }
        return Map.copyOf(environment);
    }
}
