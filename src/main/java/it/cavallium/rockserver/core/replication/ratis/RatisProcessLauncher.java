package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
public interface RatisProcessLauncher {

    Process start(RatisProcessAdapterCommand command) throws IOException;

    static RatisProcessLauncher defaultLauncher() {
        return command -> {
            if (command == null) {
                throw new IllegalArgumentException("command must not be null");
            }
            var processBuilder = new ProcessBuilder(command.toCommandLine());
            processBuilder.directory(command.workingDirectory().toFile());
            applyEnvironment(processBuilder, command.environment());
            processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            return processBuilder.start();
        };
    }

    static void applyEnvironment(ProcessBuilder processBuilder, Map<String, String> commandEnvironment) {
        if (processBuilder == null) {
            throw new IllegalArgumentException("processBuilder must not be null");
        }
        if (commandEnvironment == null) {
            throw new IllegalArgumentException("commandEnvironment must not be null");
        }
        var environment = processBuilder.environment();
        RatisAdapterBoundary.removeJavaLauncherOverrideEnvironment(environment);
        RatisAdapterBoundary.removeNativeLoaderOverrideEnvironment(environment);
        environment.putAll(RatisAdapterBoundary.copyEnvironment(commandEnvironment));
    }
}
