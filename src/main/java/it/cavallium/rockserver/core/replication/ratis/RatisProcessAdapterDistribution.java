package it.cavallium.rockserver.core.replication.ratis;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public record RatisProcessAdapterDistribution(Path adapterHome,
                                              Path javaExecutable,
                                              String mainClass,
                                              List<Path> runtimeClasspathEntries,
                                              List<String> defaultJvmArguments,
                                              Map<String, String> environment) {

    public RatisProcessAdapterDistribution {
        RatisAdapterBoundary.requireSingleLinePath(adapterHome, "adapterHome");
        RatisAdapterBoundary.requireSingleLinePath(javaExecutable, "javaExecutable");
        RatisAdapterBoundary.requireJavaMainClassName(mainClass);
        runtimeClasspathEntries = copyRelativeClasspath(runtimeClasspathEntries);
        defaultJvmArguments = copyArguments(defaultJvmArguments, "defaultJvmArguments", true);
        environment = RatisAdapterBoundary.copyEnvironment(environment);
    }

    public RatisProcessAdapterCommand toCommand(Path workingDirectory,
                                                List<String> jvmArguments,
                                                List<String> programArguments,
                                                Map<String, String> environmentOverrides) {
        var combinedJvmArguments = new ArrayList<>(defaultJvmArguments);
        combinedJvmArguments.addAll(copyArguments(jvmArguments, "jvmArguments", true));

        var combinedEnvironment = new LinkedHashMap<>(environment);
        combinedEnvironment.putAll(RatisAdapterBoundary.copyEnvironment(environmentOverrides));

        return new RatisProcessAdapterCommand(
                javaExecutable,
                RatisAdapterBoundary.requireSingleLinePath(workingDirectory, "workingDirectory"),
                resolvedClasspathEntries(),
                mainClass,
                combinedJvmArguments,
                copyArguments(programArguments, "programArguments", false),
                combinedEnvironment
        );
    }

    public List<Path> resolvedClasspathEntries() {
        return runtimeClasspathEntries.stream()
                .map(adapterHome::resolve)
                .map(Path::normalize)
                .toList();
    }

    private static List<Path> copyRelativeClasspath(List<Path> entries) {
        if (entries == null) {
            throw new IllegalArgumentException("runtimeClasspathEntries must not be null");
        }
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("runtimeClasspathEntries must not be empty");
        }
        var copy = new ArrayList<Path>();
        var seen = new HashSet<Path>();
        for (var entry : entries) {
            if (entry == null) {
                throw new IllegalArgumentException("runtimeClasspathEntries entry must not be null");
            }
            RatisAdapterBoundary.requireSingleLinePath(entry, "runtimeClasspathEntries entry");
            if (entry.toString().indexOf(File.pathSeparatorChar) >= 0) {
                throw new IllegalArgumentException(
                        "runtimeClasspathEntries entries must not contain the path separator"
                );
            }
            var normalized = entry.normalize();
            if (normalized.isAbsolute() || normalized.startsWith("..") || normalized.toString().isBlank()) {
                throw new IllegalArgumentException(
                        "runtimeClasspathEntries must be relative JAR paths inside the adapter distribution"
                );
            }
            if (!normalized.toString().toLowerCase(Locale.ROOT).endsWith(".jar")) {
                throw new IllegalArgumentException(
                        "runtimeClasspathEntries must be relative JAR paths inside the adapter distribution: "
                                + normalized
                );
            }
            if (!seen.add(normalized)) {
                throw new IllegalArgumentException(
                        "runtimeClasspathEntries must not contain duplicate paths: " + normalized
                );
            }
            copy.add(normalized);
        }
        return List.copyOf(copy);
    }

    private static List<String> copyArguments(List<String> arguments, String name, boolean rejectUnsafeJvmArguments) {
        if (arguments == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        for (var argument : arguments) {
            if (argument == null) {
                throw new IllegalArgumentException(name + " entry must not be null");
            }
            if (argument.isBlank()) {
                throw new IllegalArgumentException(name + " entries must not be blank");
            }
            if (argument.indexOf('\0') >= 0 || argument.indexOf('\r') >= 0 || argument.indexOf('\n') >= 0) {
                throw new IllegalArgumentException(name + " entries must not contain NUL, CR, or LF");
            }
            if (rejectUnsafeJvmArguments) {
                RatisProcessAdapterCommand.requireSafeJvmArgument(argument);
            }
        }
        return List.copyOf(arguments);
    }

}
