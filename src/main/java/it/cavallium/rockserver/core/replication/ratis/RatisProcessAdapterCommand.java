package it.cavallium.rockserver.core.replication.ratis;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public record RatisProcessAdapterCommand(Path javaExecutable,
                                         Path workingDirectory,
                                         List<Path> classpathEntries,
                                         String mainClass,
                                         List<String> jvmArguments,
                                         List<String> programArguments,
                                         Map<String, String> environment) {

    public RatisProcessAdapterCommand {
        RatisAdapterBoundary.requireSingleLinePath(javaExecutable, "javaExecutable");
        RatisAdapterBoundary.requireSingleLinePath(workingDirectory, "workingDirectory");
        classpathEntries = copyClasspath(classpathEntries);
        RatisAdapterBoundary.requireJavaMainClassName(mainClass);
        jvmArguments = copyArguments(jvmArguments, "jvmArguments", true);
        programArguments = copyArguments(programArguments, "programArguments", false);
        environment = RatisAdapterBoundary.copyEnvironment(environment);
    }

    public String classpath() {
        var classpath = new StringBuilder();
        for (var entry : classpathEntries) {
            if (!classpath.isEmpty()) {
                classpath.append(File.pathSeparator);
            }
            classpath.append(entry);
        }
        return classpath.toString();
    }

    public List<String> toCommandLine() {
        var command = new ArrayList<String>();
        command.add(javaExecutable.toString());
        command.addAll(jvmArguments);
        command.add("-cp");
        command.add(classpath());
        command.add(mainClass);
        command.addAll(programArguments);
        return List.copyOf(command);
    }

    private static List<Path> copyClasspath(List<Path> entries) {
        if (entries == null) {
            throw new IllegalArgumentException("classpathEntries must not be null");
        }
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("classpathEntries must not be empty");
        }
        var seen = new HashSet<Path>();
        for (var entry : entries) {
            if (entry == null) {
                throw new IllegalArgumentException("classpathEntries entry must not be null");
            }
            RatisAdapterBoundary.requireSingleLinePath(entry, "classpathEntries entry");
            if (entry.toString().indexOf(File.pathSeparatorChar) >= 0) {
                throw new IllegalArgumentException("classpathEntries entries must not contain the path separator");
            }
            var normalized = entry.toAbsolutePath().normalize();
            if (!normalized.toString().toLowerCase(Locale.ROOT).endsWith(".jar")) {
                throw new IllegalArgumentException("classpathEntries must name JAR files: " + entry);
            }
            if (!seen.add(normalized)) {
                throw new IllegalArgumentException("classpathEntries must not contain duplicate paths: " + entry);
            }
        }
        return List.copyOf(entries);
    }

    private static List<String> copyArguments(List<String> arguments, String name, boolean rejectClasspathOrModulePath) {
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
            if (rejectClasspathOrModulePath) {
                requireSafeJvmArgument(argument);
            }
        }
        return List.copyOf(arguments);
    }

    static void requireSafeJvmArgument(String argument) {
        if (overridesAdapterLaunchBoundary(argument)) {
            throw new IllegalArgumentException(
                    "jvmArguments must not override the adapter classpath, module path, launch target, "
                            + "or instrumentation boundary"
            );
        }
        if (exitsBeforeAdapterMain(argument)) {
            throw new IllegalArgumentException(
                    "jvmArguments must not use Java launcher inspection, validation, or dry-run modes"
            );
        }
        if (argument.startsWith("@")) {
            throw new IllegalArgumentException("jvmArguments must not use Java launcher argument files");
        }
    }

    private static boolean overridesAdapterLaunchBoundary(String argument) {
        return argument.equals("-cp")
                || argument.equals("-classpath")
                || argument.equals("--class-path")
                || argument.startsWith("--class-path=")
                || argument.equals("-p")
                || argument.equals("--module-path")
                || argument.startsWith("--module-path=")
                || argument.equals("-m")
                || argument.equals("--module")
                || argument.startsWith("--module=")
                || argument.equals("-jar")
                || argument.equals("--patch-module")
                || argument.startsWith("--patch-module=")
                || argument.equals("--upgrade-module-path")
                || argument.startsWith("--upgrade-module-path=")
                || argument.startsWith("-javaagent")
                || argument.startsWith("-agentlib")
                || argument.startsWith("-agentpath")
                || argument.startsWith("-Xbootclasspath");
    }

    private static boolean exitsBeforeAdapterMain(String argument) {
        return argument.equals("--dry-run")
                || argument.equals("--validate-modules")
                || argument.equals("--list-modules")
                || argument.equals("--describe-module")
                || argument.startsWith("--describe-module=")
                || argument.equals("-version")
                || argument.equals("--version")
                || argument.equals("-fullversion")
                || argument.equals("--full-version")
                || argument.equals("-help")
                || argument.equals("--help")
                || argument.equals("--help-extra")
                || argument.equals("-h")
                || argument.equals("-?")
                || argument.equals("-X");
    }

}
