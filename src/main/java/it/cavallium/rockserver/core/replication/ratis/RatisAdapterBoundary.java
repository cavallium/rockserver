package it.cavallium.rockserver.core.replication.ratis;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public record RatisAdapterBoundary(RatisAdapterBoundaryMode mode, String implementationName) {

    private static final Pattern JAVA_BINARY_NAME = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*"
    );
    private static final Pattern ENVIRONMENT_KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Set<String> JAVA_LAUNCHER_OVERRIDE_ENVIRONMENT_KEYS = Set.of(
            "CLASSPATH",
            "JAVA_TOOL_OPTIONS",
            "_JAVA_OPTIONS",
            "JDK_JAVA_OPTIONS"
    );
    private static final Set<String> NATIVE_LOADER_OVERRIDE_ENVIRONMENT_KEYS = Set.of(
            "LD_AUDIT",
            "LD_LIBRARY_PATH",
            "LD_PRELOAD",
            "DYLD_FALLBACK_LIBRARY_PATH",
            "DYLD_INSERT_LIBRARIES",
            "DYLD_LIBRARY_PATH"
    );

    public RatisAdapterBoundary {
        requireNonNull(mode, "mode");
        requireSingleLineNonBlank(implementationName, "implementationName");
    }

    public static RatisAdapterBoundary classpathIsolated(String implementationName) {
        return new RatisAdapterBoundary(RatisAdapterBoundaryMode.CLASSPATH_ISOLATED_ADAPTER, implementationName);
    }

    public static RatisAdapterBoundary externalProcess(String implementationName) {
        return new RatisAdapterBoundary(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, implementationName);
    }

    public boolean keepsRatisOffProductionModulePath() {
        return !mode.usesProductionModulePath();
    }

    static String requireNonBlank(String value, String name) {
        var requiredValue = requireNonNull(value, name);
        if (requiredValue.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return requiredValue;
    }

    static String requireSingleLineNonBlank(String value, String name) {
        var requiredValue = requireNonBlank(value, name);
        if (requiredValue.indexOf('\r') >= 0 || requiredValue.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(name + " must be a single line");
        }
        return requiredValue;
    }

    static Path requireSingleLinePath(Path path, String name) {
        requireNonNull(path, name);
        var value = path.toString();
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (value.indexOf('\0') >= 0 || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(name + " must not contain NUL, CR, or LF");
        }
        return path;
    }

    static String requireEnvironmentKey(String value, String name) {
        var requiredValue = requireSingleLineNonBlank(value, name);
        if (requiredValue.indexOf('=') >= 0 || requiredValue.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must not contain '=' or NUL");
        }
        if (!ENVIRONMENT_KEY.matcher(requiredValue).matches()) {
            throw new IllegalArgumentException(name + " must be a portable environment variable name");
        }
        if (isJavaLauncherOverrideEnvironmentKey(requiredValue)) {
            throw new IllegalArgumentException(
                    name + " must not override the adapter Java launcher environment: " + requiredValue
            );
        }
        if (isNativeLoaderOverrideEnvironmentKey(requiredValue)) {
            throw new IllegalArgumentException(
                    name + " must not override the adapter native loader environment: " + requiredValue
            );
        }
        return requiredValue;
    }

    static String requireEnvironmentValue(String value, String name) {
        var requiredValue = requireNonNull(value, name);
        if (requiredValue.indexOf('\0') >= 0
                || requiredValue.indexOf('\r') >= 0
                || requiredValue.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(name + " must not contain NUL, CR, or LF");
        }
        return requiredValue;
    }

    static String requireJavaMainClassName(String value) {
        var requiredValue = requireSingleLineNonBlank(value, "mainClass");
        if (!JAVA_BINARY_NAME.matcher(requiredValue).matches()) {
            throw new IllegalArgumentException("mainClass must be a Java binary class name");
        }
        return requiredValue;
    }

    static Map<String, String> copyEnvironment(Map<String, String> environment) {
        requireNonNull(environment, "environment");
        var copy = new LinkedHashMap<String, String>();
        for (var entry : environment.entrySet()) {
            var key = requireEnvironmentKey(entry.getKey(), "environment key");
            var value = requireEnvironmentValue(entry.getValue(), "environment value");
            copy.put(key, value);
        }
        return Map.copyOf(copy);
    }

    static void removeJavaLauncherOverrideEnvironment(Map<String, String> environment) {
        requireNonNull(environment, "environment");
        environment.keySet().removeIf(RatisAdapterBoundary::isJavaLauncherOverrideEnvironmentKey);
    }

    static void removeNativeLoaderOverrideEnvironment(Map<String, String> environment) {
        requireNonNull(environment, "environment");
        environment.keySet().removeIf(RatisAdapterBoundary::isNativeLoaderOverrideEnvironmentKey);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static boolean isJavaLauncherOverrideEnvironmentKey(String key) {
        return key != null && JAVA_LAUNCHER_OVERRIDE_ENVIRONMENT_KEYS.contains(key.toUpperCase(Locale.ROOT));
    }

    private static boolean isNativeLoaderOverrideEnvironmentKey(String key) {
        return key != null && NATIVE_LOADER_OVERRIDE_ENVIRONMENT_KEYS.contains(key.toUpperCase(Locale.ROOT));
    }
}
