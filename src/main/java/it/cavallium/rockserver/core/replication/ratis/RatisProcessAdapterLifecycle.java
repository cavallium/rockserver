package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class RatisProcessAdapterLifecycle implements RatisServerLifecycle {

    private static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final long MAX_FORCEFUL_TERMINATION_GRACE_MILLIS = 100;

    private final String implementationName;
    private final RatisProcessAdapterCommand command;
    private final long stopTimeoutMillis;
    private final RatisProcessLauncher launcher;
    private final RatisProcessAdapterHealthProbe healthProbe;
    private Process process;

    public RatisProcessAdapterLifecycle(String implementationName, RatisProcessAdapterCommand command) {
        this(implementationName, command, DEFAULT_STOP_TIMEOUT, RatisProcessLauncher.defaultLauncher());
    }

    public RatisProcessAdapterLifecycle(String implementationName,
                                        RatisProcessAdapterCommand command,
                                        Duration stopTimeout,
                                        RatisProcessLauncher launcher) {
        this(implementationName, command, stopTimeout, launcher, RatisProcessAdapterHealthProbe.alwaysReady());
    }

    public RatisProcessAdapterLifecycle(String implementationName,
                                        RatisProcessAdapterCommand command,
                                        Duration stopTimeout,
                                        RatisProcessLauncher launcher,
                                        RatisProcessAdapterHealthProbe healthProbe) {
        this.implementationName = RatisAdapterBoundary.requireSingleLineNonBlank(
                implementationName,
                "implementationName"
        );
        this.command = requireNonNull(command, "command");
        this.stopTimeoutMillis = requireStopTimeoutMillis(stopTimeout);
        this.launcher = requireNonNull(launcher, "launcher");
        this.healthProbe = requireNonNull(healthProbe, "healthProbe");
    }

    @Override
    public RatisAdapterBoundary boundary() {
        return RatisAdapterBoundary.externalProcess(implementationName);
    }

    @Override
    public synchronized void start() throws IOException {
        if (process != null && process.isAlive()) {
            throw new IllegalStateException("Ratis adapter process is already running");
        }
        process = null;
        RatisProcessAdapterFileWriter.createDirectoriesDurably(command.workingDirectory());
        var started = requireStartedProcess(launcher.start(command));
        if (!started.isAlive()) {
            throw exitedDuringStartup(started);
        }
        process = started;
        verifyHealthy(started);
        if (!started.isAlive()) {
            process = null;
            throw exitedDuringStartup(started);
        }
    }

    public synchronized boolean isRunning() {
        return process != null && process.isAlive();
    }

    @Override
    public synchronized void close() throws IOException {
        var runningProcess = process;
        if (runningProcess == null || !runningProcess.isAlive()) {
            process = null;
            return;
        }

        var startNanos = System.nanoTime();
        runningProcess.destroy();
        if (awaitExit(runningProcess, startNanos, stopTimeoutMillis)) {
            process = null;
            return;
        }

        runningProcess.destroyForcibly();
        if (!awaitExit(runningProcess, startNanos, forcefulTerminationBudgetMillis())) {
            throw new IOException("Ratis adapter process did not exit after forceful termination");
        }
        process = null;
    }

    private void verifyHealthy(Process started) throws IOException {
        try {
            var health = requireHealth(healthProbe.check(command));
            if (health.isReady()) {
                return;
            }
            throw new IOException("Ratis adapter process failed health check: "
                    + health.status() + " " + health.message());
        } catch (IOException | RuntimeException | Error e) {
            stopStartedProcess(started, e);
            if (!started.isAlive()) {
                process = null;
            }
            throw e;
        }
    }

    private void stopStartedProcess(Process started, Throwable failure) {
        var startNanos = System.nanoTime();
        try {
            started.destroy();
            if (!awaitExit(started, startNanos, stopTimeoutMillis)) {
                started.destroyForcibly();
                if (!awaitExit(started, startNanos, forcefulTerminationBudgetMillis())) {
                    suppress(failure, new IOException(
                            "Ratis adapter process did not exit after failed startup health check"
                    ));
                }
            }
        } catch (IOException | RuntimeException | Error e) {
            suppress(failure, e);
        }
    }

    private static void suppress(Throwable failure, Throwable addition) {
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
    }

    private boolean awaitExit(Process runningProcess, long startNanos, long timeoutBudgetMillis) throws IOException {
        var interruption = new CloseInterruption();
        try {
            while (true) {
                try {
                    return runningProcess.waitFor(
                            remainingTimeoutMillis(startNanos, timeoutBudgetMillis),
                            TimeUnit.MILLISECONDS
                    );
                } catch (InterruptedException _) {
                    interruption.record();
                    if (!runningProcess.isAlive()) {
                        return true;
                    }
                    if (remainingTimeoutMillis(startNanos, timeoutBudgetMillis) <= 0) {
                        return false;
                    }
                }
            }
        } finally {
            interruption.restore();
        }
    }

    private long forcefulTerminationBudgetMillis() {
        try {
            return Math.addExact(stopTimeoutMillis, MAX_FORCEFUL_TERMINATION_GRACE_MILLIS);
        } catch (ArithmeticException _) {
            return Long.MAX_VALUE;
        }
    }

    private static long remainingTimeoutMillis(long startNanos, long timeoutBudgetMillis) {
        var elapsedNanos = System.nanoTime() - startNanos;
        var timeoutBudgetNanos = TimeUnit.MILLISECONDS.toNanos(timeoutBudgetMillis);
        if (elapsedNanos >= timeoutBudgetNanos) {
            return 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(timeoutBudgetNanos - elapsedNanos);
    }

    private static int exitCode(Process process) {
        try {
            return process.exitValue();
        } catch (IllegalThreadStateException _) {
            return Integer.MIN_VALUE;
        }
    }

    private static IOException exitedDuringStartup(Process process) {
        return new IOException("Ratis adapter process exited during startup with exit code "
                + exitCode(process));
    }

    private static long requireStopTimeoutMillis(Duration duration) {
        requireNonNull(duration, "stopTimeout");
        if (duration.isNegative()) {
            throw new IllegalArgumentException("stopTimeout must not be negative");
        }
        long timeoutMillis;
        try {
            timeoutMillis = duration.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("stopTimeout must fit milliseconds", e);
        }
        if (!duration.isZero() && timeoutMillis == 0) {
            throw new IllegalArgumentException("stopTimeout must be zero or at least one millisecond");
        }
        return timeoutMillis;
    }

    private static Process requireStartedProcess(Process process) {
        if (process == null) {
            throw new IllegalStateException("launcher returned null process");
        }
        return process;
    }

    private static RatisProcessAdapterHealth requireHealth(RatisProcessAdapterHealth health) {
        if (health == null) {
            throw new IllegalStateException("healthProbe returned null health");
        }
        return health;
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static final class CloseInterruption {

        private boolean interrupted;

        void record() {
            interrupted = true;
        }

        void restore() {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
