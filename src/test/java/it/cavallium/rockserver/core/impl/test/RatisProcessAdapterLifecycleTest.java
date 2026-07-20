package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundaryMode;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealth;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealthCodec;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealthStatus;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterCommand;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDistribution;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDistributionManifest;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealthProbe;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterLifecycle;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketHealthProbe;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketHealthServer;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessLauncher;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterLifecycleTest {

    @Test
    void commandLineUsesClasspathAndMainClass() {
        var command = command(
                List.of(Path.of("lib/rockserver.jar"), Path.of("lib/ratis.jar")),
                List.of("-Xmx128m"),
                List.of("--node-id=n1")
        );

        assertEquals(List.of(
                "bin/java",
                "-Xmx128m",
                "-cp",
                "lib/rockserver.jar" + File.pathSeparator + "lib/ratis.jar",
                "it.cavallium.rockserver.ratis.AdapterMain",
                "--node-id=n1"
        ), command.toCommandLine());
    }

    @Test
    void commandRejectsModulePathOrCallerClasspathOverrides() {
        assertThrows(IllegalArgumentException.class, () -> command(
                List.of(Path.of("lib/rockserver.jar")),
                List.of("--module-path", "mods"),
                List.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> command(
                List.of(Path.of("lib/rockserver.jar")),
                List.of("-cp", "other.jar"),
                List.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> command(
                List.of(Path.of("lib/rockserver.jar")),
                List.of("@adapter.args"),
                List.of()
        ));
    }

    @Test
    void commandAndDistributionRejectNulArgumentsBeforeProcessLaunch() {
        assertThrows(IllegalArgumentException.class, () -> command(
                List.of(Path.of("lib/rockserver.jar")),
                List.of("-Dunsafe=bad\0value"),
                List.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> command(
                List.of(Path.of("lib/rockserver.jar")),
                List.of(),
                List.of("--unsafe=bad\0value")
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Dunsafe=bad\0value"),
                Map.of()
        ));
    }

    @Test
    void commandAndDistributionRejectUnsafeMainClassNames() {
        assertInvalidCommandMainClass("-jar");
        assertInvalidCommandMainClass("it.cavallium.rockserver.ratis.AdapterMain\n-Xmx1g");
        assertInvalidCommandMainClass("it.cavallium.rockserver.ratis.Adapter-Main");
        assertInvalidCommandMainClass("it.cavallium.rockserver.ratis.Adapter Main");

        assertInvalidDistributionMainClass("-jar");
        assertInvalidDistributionMainClass("it.cavallium.rockserver.ratis.AdapterMain\n-Xmx1g");
        assertInvalidDistributionMainClass("it.cavallium.rockserver.ratis.Adapter-Main");
        assertInvalidDistributionMainClass("it.cavallium.rockserver.ratis.Adapter Main");
    }

    @Test
    void distributionBuildsCommandFromRuntimeClasspathMetadata() {
        var distribution = new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(
                        Path.of("adapter/rockserver-ratis-adapter.jar"),
                        Path.of("lib/ratis-server.jar"),
                        Path.of("lib/ratis-grpc.jar")
                ),
                List.of("-Xmx512m"),
                Map.of("ROCKSERVER_RATIS_HOME", "/opt/rockserver-ratis-adapter")
        );

        var command = distribution.toCommand(
                Path.of("/var/lib/rockserver"),
                List.of("-Dlogging.level=ERROR"),
                List.of("--node-id=n1", "--config=/etc/rockserver/ratis-adapter.conf"),
                Map.of("ROCKSERVER_NODE_ID", "n1")
        );

        assertEquals(List.of(
                Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar"),
                Path.of("/opt/rockserver-ratis-adapter/lib/ratis-server.jar"),
                Path.of("/opt/rockserver-ratis-adapter/lib/ratis-grpc.jar")
        ), command.classpathEntries());
        assertEquals(List.of(
                "/usr/lib/jvm/default/bin/java",
                "-Xmx512m",
                "-Dlogging.level=ERROR",
                "-cp",
                "/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar"
                        + File.pathSeparator
                        + "/opt/rockserver-ratis-adapter/lib/ratis-server.jar"
                        + File.pathSeparator
                        + "/opt/rockserver-ratis-adapter/lib/ratis-grpc.jar",
                "it.cavallium.rockserver.ratis.AdapterMain",
                "--node-id=n1",
                "--config=/etc/rockserver/ratis-adapter.conf"
        ), command.toCommandLine());
        assertEquals(Map.of(
                "ROCKSERVER_RATIS_HOME", "/opt/rockserver-ratis-adapter",
                "ROCKSERVER_NODE_ID", "n1"
        ), command.environment());
    }

    @Test
    void distributionRejectsRuntimeClasspathOutsideAdapterHome() {
        assertInvalidDistributionClasspath(Path.of("/tmp/ratis.jar"));
        assertInvalidDistributionClasspath(Path.of("../ratis.jar"));
    }

    @Test
    void commandAndDistributionRejectClasspathSeparatorInjection() {
        assertThrows(IllegalArgumentException.class, () -> command(
                List.of(Path.of("lib/rockserver.jar" + File.pathSeparator + "/tmp/injected.jar")),
                List.of(),
                List.of()
        ));
        assertInvalidDistributionClasspath(
                Path.of("adapter/rockserver-ratis-adapter.jar" + File.pathSeparator + "/tmp/injected.jar")
        );
    }

    @Test
    void distributionManifestLoadsIndexedClasspathAndLaunchMetadata(@TempDir Path tempDir) throws Exception {
        var properties = manifestProperties();
        var manifestPath = tempDir.resolve("ratis-adapter.properties");
        try (var writer = Files.newBufferedWriter(manifestPath)) {
            properties.store(writer, "test manifest");
        }

        var distribution = RatisProcessAdapterDistributionManifest.read(
                manifestPath,
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java")
        );
        var command = distribution.toCommand(
                Path.of("/var/lib/rockserver"),
                List.of("-Dlogging.level=ERROR"),
                List.of("--node-id=n1"),
                Map.of("ROCKSERVER_NODE_ID", "n1")
        );

        assertEquals("it.cavallium.rockserver.ratis.AdapterMain", command.mainClass());
        assertEquals(List.of(
                Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar"),
                Path.of("/opt/rockserver-ratis-adapter/lib/ratis-server.jar")
        ), command.classpathEntries());
        assertEquals(List.of("-Xmx512m", "-Dlogging.level=ERROR"), command.jvmArguments());
        assertEquals(Map.of(
                "ROCKSERVER_RATIS_HOME", "/opt/rockserver-ratis-adapter",
                "ROCKSERVER_NODE_ID", "n1"
        ), command.environment());
    }

    @Test
    void distributionManifestRejectsNullInputsWithExplicitDiagnostics() {
        var properties = manifestProperties();
        var nullManifestPath = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDistributionManifest.read(
                        null,
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java")
                ));
        var nullAdapterHome = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDistributionManifest.fromProperties(
                        null,
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        properties
                ));
        var nullJavaExecutable = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDistributionManifest.fromProperties(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        null,
                        properties
                ));
        var nullProperties = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDistributionManifest.fromProperties(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        null
                ));

        assertEquals("manifestPath must not be null", nullManifestPath.getMessage());
        assertEquals("adapterHome must not be null", nullAdapterHome.getMessage());
        assertEquals("javaExecutable must not be null", nullJavaExecutable.getMessage());
        assertEquals("properties must not be null", nullProperties.getMessage());
    }

    @Test
    void distributionManifestRejectsUnknownAndNonContiguousKeys() {
        var unknown = manifestProperties();
        unknown.setProperty("modulePath.0", "mods");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unknown
        ));

        var gap = manifestProperties();
        gap.remove("classpath.1");
        gap.setProperty("classpath.2", "lib/ratis-grpc.jar");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                gap
        ));

        var leadingZeroIndex = manifestProperties();
        leadingZeroIndex.setProperty("classpath.01", "lib/ratis-grpc.jar");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                leadingZeroIndex
        ));

        var plusIndex = manifestProperties();
        plusIndex.setProperty("jvmArgument.+1", "-Xms128m");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                plusIndex
        ));

        var unsafeEnvironmentKey = manifestProperties();
        unsafeEnvironmentKey.setProperty("environment.BAD\nKEY", "value");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeEnvironmentKey
        ));

        var unsafeEnvironmentNulKey = manifestProperties();
        unsafeEnvironmentNulKey.setProperty("environment.BAD\0KEY", "value");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeEnvironmentNulKey
        ));

        var unsafeEnvironmentValue = manifestProperties();
        unsafeEnvironmentValue.setProperty("environment.BAD_VALUE", "bad\nvalue");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeEnvironmentValue
        ));

        var nonPortableEnvironmentKey = manifestProperties();
        nonPortableEnvironmentKey.setProperty("environment.BAD KEY", "value");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                nonPortableEnvironmentKey
        ));

        var unsafeJavaLauncherEnvironment = manifestProperties();
        unsafeJavaLauncherEnvironment.setProperty("environment.JAVA_TOOL_OPTIONS", "-javaagent:/tmp/agent.jar");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeJavaLauncherEnvironment
        ));

        var unsafeNativeLoaderEnvironment = manifestProperties();
        unsafeNativeLoaderEnvironment.setProperty("environment.LD_PRELOAD", "/tmp/injected.so");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeNativeLoaderEnvironment
        ));

        var unsafeJvmArgument = manifestProperties();
        unsafeJvmArgument.setProperty("jvmArgument.0", "-Dunsafe=bad\0value");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeJvmArgument
        ));

        var lineUnsafeJvmArgument = manifestProperties();
        lineUnsafeJvmArgument.setProperty("jvmArgument.0", "-Dunsafe=bad\nvalue");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                lineUnsafeJvmArgument
        ));

        var unsafeLauncherJvmArgument = manifestProperties();
        unsafeLauncherJvmArgument.setProperty("jvmArgument.0", "-javaagent:/tmp/agent.jar");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeLauncherJvmArgument
        ));

        var unsafeIndexedClasspath = manifestProperties();
        unsafeIndexedClasspath.setProperty(
                "classpath.0",
                "adapter/rockserver-ratis-adapter.jar" + File.pathSeparator + "/tmp/injected.jar"
        );
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                unsafeIndexedClasspath
        ));

        var lineUnsafeIndexedClasspath = manifestProperties();
        lineUnsafeIndexedClasspath.setProperty("classpath.0", "adapter/rockserver-ratis-adapter\n.jar");
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                lineUnsafeIndexedClasspath
        ));
    }

    @Test
    void distributionManifestRejectsUnsafeClasspathWithManifestDiagnostic() {
        var indexedNul = manifestProperties();
        indexedNul.setProperty("classpath.0", "adapter/rockserver-ratis-adapter\0.jar");
        var indexedFailure = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDistributionManifest.fromProperties(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        indexedNul
                ));

        var generatedNul = new Properties();
        generatedNul.setProperty("mainClass", "it.cavallium.rockserver.ratis.AdapterMain");
        generatedNul.setProperty("classpath", "adapter/rockserver-ratis-adapter\0.jar");
        var generatedFailure = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDistributionManifest.fromProperties(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        generatedNul
                ));

        assertEquals(
                "Ratis adapter distribution manifest classpath entry must not contain NUL or line separators: classpath.0",
                indexedFailure.getMessage()
        );
        assertEquals(
                "Ratis adapter distribution manifest classpath entry must not contain NUL or line separators: classpath",
                generatedFailure.getMessage()
        );
    }

    @Test
    void distributionManifestLoadsGeneratedPathSeparatedClasspath() {
        var properties = new Properties();
        properties.setProperty("mainClass", "it.cavallium.rockserver.ratis.AdapterMain");
        properties.setProperty("classpath", "adapter/rockserver-ratis-adapter.jar"
                + File.pathSeparator
                + "lib/ratis-server.jar");

        var distribution = RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                properties
        );

        assertEquals(List.of(
                Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar"),
                Path.of("/opt/rockserver-ratis-adapter/lib/ratis-server.jar")
        ), distribution.resolvedClasspathEntries());
    }

    @Test
    void distributionManifestRejectsMixedClasspathFormats() {
        var properties = manifestProperties();
        properties.setProperty("classpath", "lib/ratis-server.jar");

        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                properties
        ));
    }

    @Test
    void distributionManifestRejectsBlankGeneratedClasspathEntries() {
        assertInvalidGeneratedClasspath("");
        assertInvalidGeneratedClasspath(" ");
        assertInvalidGeneratedClasspath(File.pathSeparator + "lib/ratis-server.jar");
        assertInvalidGeneratedClasspath("adapter/rockserver-ratis-adapter.jar" + File.pathSeparator);
        assertInvalidGeneratedClasspath("adapter/rockserver-ratis-adapter.jar"
                + File.pathSeparator
                + File.pathSeparator
                + "lib/ratis-server.jar");
    }

    @Test
    void distributionManifestRejectsGeneratedClasspathEntriesThatDoNotNameJars() {
        assertInvalidGeneratedClasspath(".");
        assertInvalidGeneratedClasspath("lib/..");
        assertInvalidGeneratedClasspath("lib");
        assertInvalidGeneratedClasspath("lib/ratis-server.zip");
    }

    @Test
    void healthCodecRoundTripsReadyResponse() {
        var health = new RatisProcessAdapterHealth(
                RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                RatisProcessAdapterHealthStatus.READY,
                "ready"
        );

        var decoded = RatisProcessAdapterHealthCodec.decode(RatisProcessAdapterHealthCodec.encode(health));

        assertEquals(health, decoded);
        assertTrue(decoded.isReady());
    }

    @Test
    void healthCodecRejectsNullArguments() {
        var encodeFailure = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterHealthCodec.encode(null)
        );
        var decodeFailure = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterHealthCodec.decode(null)
        );

        assertEquals("health must not be null", encodeFailure.getMessage());
        assertEquals("payload must not be null", decodeFailure.getMessage());
    }

    @Test
    void healthCodecRejectsUnknownKeysAndFutureVersions() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "version=1\nstatus=READY\nmessage=ready\nextra=value\n"
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "version=2\nstatus=READY\nmessage=ready\n"
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "version=01\nstatus=READY\nmessage=ready\n"
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "version=+1\nstatus=READY\nmessage=ready\n"
        ));
    }

    @Test
    void healthCodecRejectsUnknownKeyBeforeRetainingLargeTail() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "extra=value\n" + "ignored=\n".repeat(10_000)
        ));

        assertEquals("Unknown Ratis adapter health key: extra", failure.getMessage());
    }

    @Test
    void healthCodecRejectsMalformedLineBeforeScanningLargeTail() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "malformed\n" + "ignored=\n".repeat(10_000)
        ));

        assertEquals("Malformed Ratis adapter health line: malformed", failure.getMessage());
    }

    @Test
    void healthCodecRejectsInvalidStatusesWithAdapterDiagnostic() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "version=1\nstatus=BROKEN\nmessage=ready\n"
        ));

        assertEquals("Invalid Ratis adapter health status: BROKEN", failure.getMessage());
    }

    @Test
    void healthCodecBoundsKnownFieldsAndUnknownKeyDiagnosticsBeforeRetention() {
        var oversizedValue = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "message=" + "x".repeat(RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS + 1)
        ));
        var oversizedUnknownKey = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "x".repeat(10_000) + "=value"
        ));

        assertEquals("Ratis adapter health value for key message must not exceed "
                + RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS + " characters", oversizedValue.getMessage());
        assertTrue(oversizedUnknownKey.getMessage().length() < 256);
    }

    @Test
    void healthCodecRejectsDuplicateKeysBeforeRetainingReplacementValues() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "message=ready\nmessage=" + "x".repeat(RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS + 1)
        ));

        assertEquals("Duplicate Ratis adapter health key: message", failure.getMessage());
    }

    @Test
    void healthCodecRejectsNonCanonicalEmptyLines() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealthCodec.decode(
                "\nversion=1"
        ));

        assertEquals("Ratis adapter codec payload contains an empty line", failure.getMessage());
    }

    @Test
    void healthRejectsNullFieldsExplicitly() {
        var nullStatus = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterHealth(
                RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                null,
                "ready"
        ));
        var nullMessage = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterHealth(
                RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                RatisProcessAdapterHealthStatus.DEGRADED,
                null
        ));
        var nullDegradedMessage = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterHealth.degraded(null)
        );

        assertEquals("status must not be null", nullStatus.getMessage());
        assertEquals("message must not be null", nullMessage.getMessage());
        assertEquals("message must not be null", nullDegradedMessage.getMessage());
    }

    @Test
    void healthRejectsCodecLineSeparators() {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterHealth(
                RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                RatisProcessAdapterHealthStatus.DEGRADED,
                "split\u2028message"
        ));
    }

    @Test
    void healthRejectsOversizedMessages() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterHealth.degraded(
                "x".repeat(RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS + 1)
        ));
    }

    @Test
    void socketHealthProbeUsesHealthRequestAndDecodesResponse() throws Exception {
        try (var server = new ServerSocket(0)) {
            var served = CompletableFuture.runAsync(() -> serveOneHealthRequest(server));
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5)
            );

            var health = probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()));

            assertTrue(health.isReady());
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketHealthProbeRetriesStartingResponseUntilReady() throws Exception {
        try (var server = new ServerSocket(0)) {
            var served = CompletableFuture.runAsync(() -> serveHealthResponses(
                    server,
                    List.of(
                            new RatisProcessAdapterHealth(
                                    RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                                    RatisProcessAdapterHealthStatus.STARTING,
                                    "still starting"
                            ),
                            RatisProcessAdapterHealth.ready()
                    )
            ));
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5)
            );

            var health = probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()));

            assertTrue(health.isReady());
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketHealthProbeRejectsOversizedResponseWithoutRetrying() throws Exception {
        try (var server = new ServerSocket(0)) {
            var served = CompletableFuture.runAsync(() -> serveOneOversizedHealthResponse(server));
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5),
                    128
            );

            var failure = assertThrows(IOException.class,
                    () -> probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of())));

            assertTrue(failure.getMessage().contains("exceeds max size"));
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketHealthProbeRejectsMalformedResponseWithoutRetrying() throws Exception {
        try (var server = new ServerSocket(0)) {
            var served = CompletableFuture.runAsync(() -> serveOneRawHealthResponse(
                    server,
                    "malformed".getBytes(StandardCharsets.UTF_8)
            ));
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5)
            );

            var failure = assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertThrows(
                    IOException.class,
                    () -> probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()))
            ));

            assertTrue(failure.getMessage().contains("Invalid Ratis adapter process health response"));
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketHealthProbeRejectsMalformedUtf8ResponseWithoutRetrying() throws Exception {
        try (var server = new ServerSocket(0)) {
            var served = CompletableFuture.runAsync(() -> serveOneRawHealthResponse(
                    server,
                    new byte[]{(byte) 0xC3}
            ));
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5)
            );

            var failure = assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertThrows(
                    IOException.class,
                    () -> probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()))
            ));

            assertTrue(failure.getMessage().contains("not valid UTF-8"));
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketHealthProbeDoesNotSleepPastTimeoutAfterFailure() {
        var probe = new RatisProcessAdapterSocketHealthProbe(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofMillis(10)
        );

        assertTimeoutPreemptively(Duration.ofMillis(75), () ->
                assertThrows(IOException.class,
                        () -> probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()))));
    }

    @Test
    void socketHealthProbeRetainsRetryFailureWhenSleepIsInterrupted() {
        var probe = new RatisProcessAdapterSocketHealthProbe(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(1)
        );
        try {
            Thread.currentThread().interrupt();

            var failure = assertThrows(IOException.class,
                    () -> probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of())));

            assertEquals("Interrupted while waiting for Ratis adapter process health", failure.getMessage());
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertEquals(1, failure.getSuppressed().length);
            assertInstanceOf(IOException.class, failure.getSuppressed()[0]);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void socketHealthEndpointsRejectTimeoutsWithoutMillisecondRepresentation() {
        var address = new InetSocketAddress("127.0.0.1", 1);
        for (var timeout : List.of(
                Duration.ofMillis(-1),
                Duration.ZERO,
                Duration.ofNanos(1),
                Duration.ofMillis((long) Integer.MAX_VALUE + 1),
                Duration.ofSeconds(Long.MAX_VALUE)
        )) {
            assertThrows(IllegalArgumentException.class,
                    () -> new RatisProcessAdapterSocketHealthProbe(address, timeout));
            assertThrows(IllegalArgumentException.class,
                    () -> {
                        try (var ignored = new RatisProcessAdapterSocketHealthServer(address, timeout)) {
                            throw new AssertionError("expected constructor rejection");
                        }
            });
        }
    }

    @Test
    void socketHealthEndpointsRejectNullInputsWithExplicitDiagnostics() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", 1);
        var timeout = Duration.ofMillis(1);

        assertRejectsNull("address must not be null",
                () -> new RatisProcessAdapterSocketHealthProbe(null, timeout));
        assertRejectsNull("timeout must not be null",
                () -> new RatisProcessAdapterSocketHealthProbe(address, null));
        assertRejectsNull("command must not be null",
                () -> new RatisProcessAdapterSocketHealthProbe(address, timeout).check(null));
        assertRejectsNullCloseable("bindAddress must not be null",
                () -> new RatisProcessAdapterSocketHealthServer(null, timeout));
        assertRejectsNullCloseable("stopTimeout must not be null",
                () -> new RatisProcessAdapterSocketHealthServer(address, null));
        assertRejectsNullCloseable("healthSupplier must not be null",
                () -> new RatisProcessAdapterSocketHealthServer(
                        address,
                        timeout,
                        null
                ));
        try (var server = new RatisProcessAdapterSocketHealthServer(address, timeout)) {
            var failure = invokeHealthStartListener(server, null);

            assertInstanceOf(IllegalArgumentException.class, failure);
            assertEquals("listener must not be null", failure.getMessage());
        }
    }

    @Test
    void socketHealthServerServesReadyResponse() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.boundAddress().getPort()),
                    Duration.ofSeconds(5)
            );

            var health = probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()));

            assertTrue(health.isReady());
        }
    }

    @Test
    void socketHealthServerCloseDisconnectsAcceptedIdleRequestPromptly() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            try (var socket = new Socket()) {
                socket.connect(server.boundAddress(), 1_000);
                socket.setSoTimeout(1_000);
                socket.getOutputStream().write('H');
                socket.getOutputStream().flush();
                awaitAcceptedHealthSocket(server);

                assertTimeoutPreemptively(Duration.ofSeconds(1), server::close);

                assertPeerDisconnected(socket);
            }
        }
    }

    @Test
    void socketHealthServerServesSuppliedDegradedResponse() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                () -> RatisProcessAdapterHealth.degraded("RPC server is not running")
        )) {
            server.start();
            var probe = new RatisProcessAdapterSocketHealthProbe(
                    new InetSocketAddress("127.0.0.1", server.boundAddress().getPort()),
                    Duration.ofSeconds(5)
            );

            var health = probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()));

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, health.status());
            assertEquals("RPC server is not running", health.message());
        }
    }

    @Test
    void socketHealthServerReportsNullSupplierHealth() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                () -> null
        )) {
            var health = invokeHealthSuppliedHealth(server, System.nanoTime());

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, health.status());
            assertEquals("Ratis process adapter health supplier returned null", health.message());
        }
    }

    @Test
    void socketHealthServerDoesNotSubmitExpiredSupplier() throws Exception {
        var supplierCalled = new AtomicBoolean();
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofMillis(1),
                () -> {
                    supplierCalled.set(true);
                    return RatisProcessAdapterHealth.ready();
                }
        )) {
            var health = invokeHealthSuppliedHealth(server, System.nanoTime() - Duration.ofSeconds(1).toNanos());

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, health.status());
            assertTrue(health.message().contains("supplier timed out"));
            assertFalse(supplierCalled.get());
        }
    }

    @Test
    void socketHealthServerNormalizesSupplierFailureMessagesToCodecLineGrammar() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                () -> {
                    throw new IllegalArgumentException("supplier\u2028failure");
                }
        )) {
            server.start();
            var probe = new RatisProcessAdapterSocketHealthProbe(server.boundAddress(), Duration.ofSeconds(5));

            var health = probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()));

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, health.status());
            assertEquals("supplier failure", health.message());
        }
    }

    @Test
    void socketHealthServerPropagatesWrappedFatalSupplierFailures() throws Exception {
        var supplierFailure = new AssertionError("fatal wrapped health supplier failure");
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                () -> {
                    throw new CompletionException(supplierFailure);
                }
        )) {
            var failure = invokeHealthSuppliedHealthFailure(server, System.nanoTime());

            assertSame(supplierFailure, failure);
        }
    }

    @Test
    void socketHealthServerPropagatesNestedWrappedFatalSupplierFailures() throws Exception {
        var supplierFailure = new AssertionError("fatal nested wrapped health supplier failure");
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                () -> {
                    throw new CompletionException(new CompletionException(supplierFailure));
                }
        )) {
            var failure = invokeHealthSuppliedHealthFailure(server, System.nanoTime());

            assertSame(supplierFailure, failure);
        }
    }

    @Test
    void socketHealthServerTimesOutAndReportsSaturatedSupplierExecutor() throws Exception {
        var supplierEntered = new CountDownLatch(1);
        var releaseSupplier = new CountDownLatch(1);
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofMillis(25),
                256,
                1,
                () -> {
                    supplierEntered.countDown();
                    awaitUninterruptibly(releaseSupplier);
                    return RatisProcessAdapterHealth.ready();
                }
        )) {
            server.start();
            var probe = new RatisProcessAdapterSocketHealthProbe(server.boundAddress(), Duration.ofSeconds(1));
            var classpath = List.of(Path.of("lib/rockserver.jar"));
            try {
                var timedOut = probe.check(command(classpath, List.of(), List.of()));
                assertTrue(supplierEntered.await(1, TimeUnit.SECONDS));
                assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, timedOut.status());
                assertTrue(timedOut.message().contains("supplier timed out"));

                var saturated = probe.check(command(classpath, List.of(), List.of()));
                assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, saturated.status());
                assertTrue(saturated.message().contains("supplier executor is saturated"));
            } finally {
                releaseSupplier.countDown();
            }
        }
    }

    @Test
    void socketHealthServerReportsFatalSupplierFailureAndStopsListener() throws Exception {
        var supplierFailure = new AssertionError("fatal health supplier failure");
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                () -> {
                    throw supplierFailure;
                }
        )) {
            server.start();

            assertSame(supplierFailure, invokeHealthServe(server, new HealthRequestSocket()));

            var failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, server::awaitClosed)
            );
            assertTrue(failure.getMessage().contains("health request worker failed"));
            assertSame(supplierFailure, failure.getCause());
            assertThrows(IllegalStateException.class, server::start);
        }
    }

    @Test
    void socketHealthServerCanRetryAfterBindFailure() throws Exception {
        try (var blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = new InetSocketAddress("127.0.0.1", blocker.getLocalPort());
            try (var server = new RatisProcessAdapterSocketHealthServer(address, Duration.ofSeconds(5))) {
                assertThrows(IOException.class, server::start);
                assertThrows(IllegalStateException.class, server::boundAddress);

                blocker.close();
                server.start();

                assertEquals(address, server.boundAddress());
            }
        }
    }

    @Test
    void socketHealthServerCanRetryAfterAcceptExecutorThrowsError() throws Exception {
        try (var blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = new InetSocketAddress("127.0.0.1", blocker.getLocalPort());
            blocker.close();
            try (var server = new RatisProcessAdapterSocketHealthServer(address, Duration.ofSeconds(5))) {
                var handoffFailure = new AssertionError("accept executor failed");
                var acceptExecutor = replaceHealthAcceptExecutor(server, new ErroringExecutorService(handoffFailure));
                try {
                    assertSame(handoffFailure, assertThrows(AssertionError.class, server::start));
                    assertThrows(IllegalStateException.class, server::boundAddress);
                } finally {
                    replaceHealthAcceptExecutor(server, acceptExecutor);
                }

                server.start();

                assertEquals(address, server.boundAddress());
            }
        }
    }

    @Test
    void socketHealthServerRetainsStartupListenerWhenRollbackCloseThrowsIOException() throws Exception {
        var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        );
        var handoffFailure = new AssertionError("accept executor failed");
        var closeFailure = "startup listener rollback close failed";
        var listener = new CloseOnceIOExceptionServerSocket(closeFailure);
        var acceptExecutor = replaceHealthAcceptExecutor(server, new ErroringExecutorService(handoffFailure));
        try {
            assertSame(handoffFailure, invokeHealthStartListener(server, listener));
            assertEquals(List.of(closeFailure), List.of(handoffFailure.getSuppressed()[0].getMessage()));
            assertSame(listener, healthListener(server));
            assertThrows(IllegalStateException.class, server::start);

            replaceHealthAcceptExecutor(server, acceptExecutor);
            server.close();
            assertEquals(2, listener.closeCalls());
            assertNull(healthListener(server));
        } finally {
            replaceHealthAcceptExecutor(server, acceptExecutor);
            clearHealthListener(server);
            server.close();
        }
    }

    @Test
    void socketHealthServerReportsUnexpectedAcceptFailureAndRejectsRestart() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();

            healthListener(server).close();

            assertThrows(IOException.class, server::awaitClosed);
            assertThrows(IllegalStateException.class, server::start);
        }
    }

    @Test
    void socketHealthServerClosesAcceptedSocketAndReportsFailureWhenRequestExecutorThrowsError() throws Exception {
        try (var blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = new InetSocketAddress("127.0.0.1", blocker.getLocalPort());
            blocker.close();
            try (var server = new RatisProcessAdapterSocketHealthServer(address, Duration.ofSeconds(5))) {
                server.start();
                var handoffFailure = new AssertionError("request executor failed");
                var requestExecutor = replaceHealthRequestExecutor(server, new ErroringExecutorService(handoffFailure));
                try (var socket = new Socket()) {
                    socket.connect(address, 5_000);
                    socket.setSoTimeout(1_000);

                    assertPeerDisconnected(socket);
                    var failure = assertThrows(IOException.class, server::awaitClosed);

                    assertTrue(failure.getMessage().contains("health listener handoff failed"));
                    assertSame(handoffFailure, failure.getCause());
                    assertThrows(IllegalStateException.class, server::start);
                    try (var rebound = new ServerSocket()) {
                        rebound.bind(address);
                    }
                } finally {
                    replaceHealthRequestExecutor(server, requestExecutor);
                }
            }
        }
    }

    @Test
    void socketHealthServerReportsRequestWorkerErrorAndStopsListener() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var requestFailure = new AssertionError("injected health request worker failure");

            assertSame(requestFailure, invokeHealthServe(server, new ErroringSocket(requestFailure)));

            var failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, server::awaitClosed)
            );
            assertTrue(failure.getMessage().contains("health request worker failed"));
            assertSame(requestFailure, failure.getCause());
            assertThrows(IllegalStateException.class, server::start);
        }
    }

    @Test
    void socketHealthServerReportsRequestSocketCloseErrorAndStopsListener() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var closeFailure = new AssertionError("injected health request socket close failure");
            var socket = new ErroringCloseSocket(closeFailure);
            acceptedHealthSocketSet(server).add(socket);
            try {
                assertSame(closeFailure, invokeHealthServe(server, socket));

                var failure = assertTimeoutPreemptively(
                        Duration.ofSeconds(1),
                        () -> assertThrows(IOException.class, server::awaitClosed)
                );
                assertTrue(failure.getMessage().contains("health request worker failed"));
                assertSame(closeFailure, failure.getCause());
                assertTrue(acceptedHealthSocketSet(server).contains(socket));
                assertThrows(IllegalStateException.class, server::start);
            } finally {
                acceptedHealthSocketSet(server).remove(socket);
            }
        }
    }

    @Test
    void socketHealthServerRetainsRejectedSocketWhenCloseThrowsError() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            var closeFailure = new AssertionError("injected rejected health socket close failure");
            var socket = new ErroringCloseSocket(closeFailure);
            acceptedHealthSocketSet(server).add(socket);
            try {
                assertSame(closeFailure, invokeHealthCloseRejected(server, socket));

                assertTrue(acceptedHealthSocketSet(server).contains(socket));
            } finally {
                acceptedHealthSocketSet(server).remove(socket);
            }
        }
    }

    @Test
    void socketHealthServerRetainsHandoffSocketWhenCloseThrowsError() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            var handoffFailure = new AssertionError("injected health handoff failure");
            var closeFailure = new AssertionError("injected health handoff socket close failure");
            var socket = new ErroringCloseSocket(closeFailure);
            acceptedHealthSocketSet(server).add(socket);
            try {
                invokeHealthCloseAfterFailedHandoff(server, socket, handoffFailure);

                assertEquals(List.of(closeFailure), List.of(handoffFailure.getSuppressed()));
                assertTrue(acceptedHealthSocketSet(server).contains(socket));
            } finally {
                acceptedHealthSocketSet(server).remove(socket);
            }
        }
    }

    @Test
    void socketHealthServerPreservesRequestWorkerErrorWhenSocketCloseAlsoFails() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var requestFailure = new AssertionError("injected health request worker failure");
            var closeFailure = new AssertionError("injected health request socket close failure");

            assertSame(requestFailure, invokeHealthServe(server, new ErroringSocket(requestFailure, closeFailure)));

            var failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, server::awaitClosed)
            );
            assertTrue(failure.getMessage().contains("health request worker failed"));
            assertSame(requestFailure, failure.getCause());
            assertEquals(List.of(closeFailure), List.of(requestFailure.getSuppressed()));
            assertThrows(IllegalStateException.class, server::start);
        }
    }

    @Test
    void socketHealthServerPreservesRequestWorkerFailureWhenListenerCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        );
        var workerFailure = new IOException("injected health request worker failure");
        var closeFailure = new AssertionError("injected health listener close failure");
        try {
            invokeHealthFailRequestWorker(server, workerFailure, new ErroringCloseServerSocket(closeFailure));

            assertEquals(List.of(closeFailure), List.of(workerFailure.getSuppressed()));
            assertThrows(IllegalStateException.class, server::start);
        } finally {
            clearHealthListener(server);
            server.close();
        }
    }

    @Test
    void socketHealthServerPreservesAcceptLoopFailureWhenListenerCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        );
        var acceptFailure = new IOException("injected health accept-loop failure");
        var closeFailure = new AssertionError("injected health listener close failure");
        try {
            invokeHealthFailAcceptLoop(server, acceptFailure, new ErroringCloseServerSocket(closeFailure));

            assertEquals(List.of(closeFailure), List.of(acceptFailure.getSuppressed()));
            assertThrows(IllegalStateException.class, server::start);
        } finally {
            clearHealthListener(server);
            server.close();
        }
    }

    @Test
    void socketHealthServerCloseBeforeStartReleasesAwaitClosed() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.close();

            assertTimeoutPreemptively(Duration.ofSeconds(1), server::awaitClosed);
        }
    }

    @Test
    void socketHealthServerCloseRetriesListenerAfterCloseThrowsIOException() throws Exception {
        var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        );
        var listener = new CloseOnceIOExceptionServerSocket("first listener close failed");
        try {
            installHealthListener(server, listener);

            assertThrows(IOException.class, server::close);

            assertSame(listener, healthListener(server));
            server.close();
            assertEquals(2, listener.closeCalls());
            assertNull(healthListener(server));
        } finally {
            clearHealthListener(server);
            server.close();
        }
    }

    @Test
    void socketHealthServerCloseWaitsAcrossInterruptionAndRestoresInterrupt() throws Exception {
        var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        );
        var workerEntered = new CountDownLatch(1);
        var releaseWorker = new CountDownLatch(1);
        var closeEntered = new CountDownLatch(1);
        var closeFailure = new AtomicReference<Throwable>();
        var interruptRestored = new AtomicBoolean();
        try {
            healthOperationExecutor(server).execute(() -> {
                workerEntered.countDown();
                awaitUninterruptibly(releaseWorker);
            });
            assertTrue(workerEntered.await(1, TimeUnit.SECONDS));

            var closeThread = new Thread(() -> {
                closeEntered.countDown();
                try {
                    server.close();
                } catch (Throwable e) {
                    closeFailure.set(e);
                }
                if (Thread.currentThread().isInterrupted()) {
                    interruptRestored.set(true);
                }
            }, "ratis-health-server-close-test");
            closeThread.start();
            try {
                assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
                TimeUnit.MILLISECONDS.sleep(50);
                assertTrue(closeThread.isAlive());
                closeThread.interrupt();
                TimeUnit.MILLISECONDS.sleep(50);
                assertTrue(closeThread.isAlive(), "close returned before interrupted worker shutdown completed");
            } finally {
                releaseWorker.countDown();
            }
            closeThread.join(1_000);

            assertFalse(closeThread.isAlive());
            assertNull(closeFailure.get());
            assertTrue(interruptRestored.get());
            assertTrue(healthOperationExecutor(server).isTerminated());
        } finally {
            releaseWorker.countDown();
            clearInterruptedStatus();
            server.close();
        }
    }

    @Test
    void socketHealthServerRejectsOversizedRequestAndKeepsServing() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5),
                256
        )) {
            server.start();
            var address = new InetSocketAddress("127.0.0.1", server.boundAddress().getPort());

            var rejected = sendHealthRequest(address, "x".repeat(512) + "\n");

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, rejected.status());
            assertTrue(rejected.message().contains("exceeds max size"));
            var probe = new RatisProcessAdapterSocketHealthProbe(address, Duration.ofSeconds(5));
            assertTrue(probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of())).isReady());
        }
    }

    @Test
    void socketHealthServerRejectsMalformedUtf8RequestAndKeepsServing() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var address = new InetSocketAddress("127.0.0.1", server.boundAddress().getPort());

            var rejected = sendHealthRequest(address, new byte[]{(byte) 0xC3, '\n'});

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, rejected.status());
            assertTrue(rejected.message().contains("not valid UTF-8"));
            var probe = new RatisProcessAdapterSocketHealthProbe(address, Duration.ofSeconds(5));
            assertTrue(probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of())).isReady());
        }
    }

    @Test
    void socketHealthServerRejectsTruncatedRequestAndKeepsServing() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var address = new InetSocketAddress("127.0.0.1", server.boundAddress().getPort());

            var rejected = sendTruncatedHealthRequest(address);

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, rejected.status());
            assertTrue(rejected.message().contains("ended before line terminator"));
            var probe = new RatisProcessAdapterSocketHealthProbe(address, Duration.ofSeconds(5));
            assertTrue(probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of())).isReady());
        }
    }

    @Test
    void socketHealthServerBoundsUnsupportedRequestMessagesAndKeepsServing() throws Exception {
        try (var server = new RatisProcessAdapterSocketHealthServer(
                new InetSocketAddress("127.0.0.1", 0),
                Duration.ofSeconds(5)
        )) {
            server.start();
            var address = new InetSocketAddress("127.0.0.1", server.boundAddress().getPort());

            var rejected = sendHealthRequest(address, "x".repeat(512) + "\n");

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, rejected.status());
            assertEquals(RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS, rejected.message().length());
            var probe = new RatisProcessAdapterSocketHealthProbe(address, Duration.ofSeconds(5));
            assertTrue(probe.check(command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of())).isReady());
        }
    }

    @Test
    void lifecycleUsesExternalProcessBoundaryAndClosesStartedProcess() throws Exception {
        var command = command(List.of(Path.of("lib/rockserver.jar")), List.of("-Xmx128m"), List.of());
        var process = new FakeProcess(true, 0, true);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command,
                Duration.ofMillis(1),
                launchedCommand -> {
                    assertSame(command, launchedCommand);
                    return process;
                }
        );

        lifecycle.start();

        assertTrue(lifecycle.isRunning());
        assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, lifecycle.boundary().mode());
        assertTrue(lifecycle.boundary().keepsRatisOffProductionModulePath());

        lifecycle.close();

        assertFalse(lifecycle.isRunning());
        assertTrue(process.destroyCalled);
        assertFalse(process.destroyForciblyCalled);
    }

    @SuppressWarnings("resource")
    @Test
    void lifecycleRejectsUnrepresentableStopTimeoutsDuringConstruction() throws IOException {
        var command = command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of());
        RatisProcessLauncher launcher = _ -> new FakeProcess(true, 0, true);

        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test", command, Duration.ofNanos(-1), launcher
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test", command, Duration.ofNanos(1), launcher
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test", command, Duration.ofSeconds(Long.MAX_VALUE), launcher
        ));

        try (var immediate = new RatisProcessAdapterLifecycle(
                "ratis-process-test", command, Duration.ZERO, launcher
        )) {
            assertFalse(immediate.isRunning());
        }
    }

    @Test
    void lifecycleRejectsLineBrokenImplementationName() {
        var command = command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of());

        assertThrows(IllegalArgumentException.class, () -> {
            try (var ignored = new RatisProcessAdapterLifecycle(
                    "ratis-process\ntest",
                    command,
                    Duration.ofMillis(1),
                    _ -> new FakeProcess(true, 0, true)
            )) {
                fail("lifecycle construction unexpectedly succeeded");
            }
        });
    }

    @Test
    @SuppressWarnings("resource")
    void lifecycleRejectsNullInputsWithExplicitDiagnostics() {
        var command = command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of());
        RatisProcessLauncher launcher = _ -> new FakeProcess(true, 0, true);
        RatisProcessAdapterHealthProbe healthProbe = _ -> RatisProcessAdapterHealth.ready();

        assertRejectsNull("implementationName must not be null", () -> new RatisProcessAdapterLifecycle(
                null,
                command,
                Duration.ofMillis(1),
                launcher,
                healthProbe
        ));
        assertRejectsNull("command must not be null", () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                null,
                Duration.ofMillis(1),
                launcher,
                healthProbe
        ));
        assertRejectsNull("stopTimeout must not be null", () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command,
                null,
                launcher,
                healthProbe
        ));
        assertRejectsNull("launcher must not be null", () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command,
                Duration.ofMillis(1),
                null,
                healthProbe
        ));
        assertRejectsNull("healthProbe must not be null", () -> new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command,
                Duration.ofMillis(1),
                launcher,
                null
        ));
    }

    @Test
    void lifecycleCreatesWorkingDirectoryBeforeLaunchingProcess(@TempDir Path tempDir) throws Exception {
        var workingDirectory = tempDir.resolve("missing").resolve("adapter-work");
        var command = command(workingDirectory, List.of(Path.of("lib/rockserver.jar")), List.of(), List.of());
        var process = new FakeProcess(true, 0, true);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command,
                Duration.ofMillis(1),
                launchedCommand -> {
                    assertSame(command, launchedCommand);
                    assertTrue(Files.isDirectory(workingDirectory));
                    return process;
                }
        );

        lifecycle.start();
        lifecycle.close();
    }

    @Test
    void lifecycleForceKillsProcessThatIgnoresGracefulDestroy() throws Exception {
        var process = new FakeProcess(true, 0, false);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process
        );

        lifecycle.start();
        lifecycle.close();

        assertTrue(process.destroyCalled);
        assertTrue(process.destroyForciblyCalled);
        assertFalse(lifecycle.isRunning());
    }

    @Test
    void lifecycleAllowsForcefulTerminationGraceWithImmediateStopTimeout() throws Exception {
        var process = new FakeProcess(true, 0, false);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ZERO,
                _ -> process
        );

        lifecycle.start();
        lifecycle.close();

        assertTrue(process.destroyCalled);
        assertTrue(process.destroyForciblyCalled);
        assertEquals(2, process.waitTimeoutMillis.size());
        assertEquals(0, process.waitTimeoutMillis.getFirst());
        assertTrue(process.waitTimeoutMillis.get(1) > 0);
        assertTrue(process.waitTimeoutMillis.get(1) <= 100);
        assertFalse(lifecycle.isRunning());
    }

    @Test
    @SuppressWarnings("resource")
    void lifecycleCapsForcefulTerminationGraceAfterGracefulTimeout() throws Exception {
        var process = new FakeProcess(true, 0, false, false, true);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(200),
                _ -> process
        );
        lifecycle.start();

        var error = assertThrows(IOException.class, lifecycle::close);

        assertTrue(error.getMessage().contains("forceful termination"));
        assertTrue(process.destroyCalled);
        assertTrue(process.destroyForciblyCalled);
        assertEquals(2, process.waitTimeoutMillis.size());
        assertTrue(process.waitTimeoutMillis.getFirst() <= 200);
        assertTrue(process.waitTimeoutMillis.get(1) <= 100);
    }

    @Test
    void lifecycleCloseRetriesProcessWaitAfterInterruptionAndRestoresInterrupt() throws Exception {
        var process = new FakeProcess(true, 0, false);
        process.interruptNextWait = true;
        process.exitAfterInterruptedWait = true;
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofSeconds(1),
                _ -> process
        );
        lifecycle.start();

        try {
            lifecycle.close();

            assertTrue(Thread.interrupted());
            assertFalse(lifecycle.isRunning());
            assertTrue(process.destroyCalled);
            assertFalse(process.destroyForciblyCalled);
            assertEquals(2, process.waitTimeoutMillis.size());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void lifecycleRejectsUnhealthyProcessDuringStartupAndTerminatesIt() {
        var process = new FakeProcess(true, 0, true);
        try (var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> new RatisProcessAdapterHealth(
                        RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                        RatisProcessAdapterHealthStatus.STARTING,
                        "still starting"
                )
        )) {
            var error = assertThrows(IOException.class, lifecycle::start);

            assertTrue(error.getMessage().contains("STARTING"));
            assertTrue(process.destroyCalled);
            assertFalse(lifecycle.isRunning());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void lifecycleRejectsNullLauncherProcessDuringStartupWithExplicitDiagnostic() throws Exception {
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> null
        );

        try (lifecycle) {
            var failure = assertThrows(IllegalStateException.class, lifecycle::start);

            assertEquals("launcher returned null process", failure.getMessage());
            assertFalse(lifecycle.isRunning());
        }
    }

    @Test
    void lifecycleTerminatesStartedProcessWhenHealthProbeReturnsNull() throws Exception {
        var process = new FakeProcess(true, 0, true);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> null
        );

        try (lifecycle) {
            var failure = assertThrows(IllegalStateException.class, lifecycle::start);

            assertEquals("healthProbe returned null health", failure.getMessage());
            assertTrue(process.destroyCalled);
            assertFalse(lifecycle.isRunning());
        }
    }

    @Test
    void lifecycleTerminatesStartedProcessWhenHealthProbeThrowsError() throws Exception {
        var process = new FakeProcess(true, 0, true);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> {
                    throw new AssertionError("health probe failed");
                }
        );
        try (lifecycle) {
            var error = assertThrows(AssertionError.class, lifecycle::start);

            assertEquals("health probe failed", error.getMessage());
            assertTrue(process.destroyCalled);
            assertFalse(lifecycle.isRunning());
        }
    }

    @Test
    void lifecycleRetainsUnhealthyProcessWhenStartupCleanupFailsSoCloseCanRetry() throws Exception {
        var process = new FakeProcess(true, 0, false, false, false);
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> new RatisProcessAdapterHealth(
                        RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                        RatisProcessAdapterHealthStatus.STARTING,
                        "still starting"
                )
        );

        var error = assertThrows(IOException.class, lifecycle::start);

        assertTrue(error.getMessage().contains("STARTING"));
        assertEquals(1, error.getSuppressed().length);
        assertTrue(error.getSuppressed()[0].getMessage().contains("failed startup"));
        assertTrue(lifecycle.isRunning());

        process.exitOnForcefulDestroy = true;
        lifecycle.close();
        assertFalse(lifecycle.isRunning());
    }

    @Test
    void lifecycleRetainsPrimaryHealthFailureWhenStartupGracefulDestroyThrowsError() throws Exception {
        var destroyFailure = new AssertionError("destroy failed");
        var process = new FakeProcess(true, 0, false);
        process.destroyFailure = destroyFailure;
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> new RatisProcessAdapterHealth(
                        RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                        RatisProcessAdapterHealthStatus.STARTING,
                        "still starting"
                )
        );

        var error = assertThrows(IOException.class, lifecycle::start);

        assertTrue(error.getMessage().contains("STARTING"));
        assertEquals(1, error.getSuppressed().length);
        assertSame(destroyFailure, error.getSuppressed()[0]);
        assertTrue(lifecycle.isRunning());

        process.destroyFailure = null;
        lifecycle.close();
        assertFalse(lifecycle.isRunning());
    }

    @Test
    void lifecycleRetainsPrimaryHealthFailureWhenStartupForcefulDestroyThrowsRuntimeException() throws Exception {
        var destroyFailure = new IllegalStateException("forceful destroy failed");
        var process = new FakeProcess(true, 0, false);
        process.destroyForciblyFailure = destroyFailure;
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> new RatisProcessAdapterHealth(
                        RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                        RatisProcessAdapterHealthStatus.STARTING,
                        "still starting"
                )
        );

        var error = assertThrows(IOException.class, lifecycle::start);

        assertTrue(error.getMessage().contains("STARTING"));
        assertEquals(1, error.getSuppressed().length);
        assertSame(destroyFailure, error.getSuppressed()[0]);
        assertTrue(lifecycle.isRunning());

        process.destroyForciblyFailure = null;
        lifecycle.close();
        assertFalse(lifecycle.isRunning());
    }

    @Test
    void lifecycleRejectsExitedProcessDuringStartup() {
        var process = new FakeProcess(false, 17, true);
        try (var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process
        )) {
            var error = assertThrows(IOException.class, lifecycle::start);

            assertTrue(error.getMessage().contains("17"));
            assertFalse(lifecycle.isRunning());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void lifecycleRejectsProcessThatExitsAfterReadyHealthCheck() {
        var process = new FakeProcess(true, 17, true);
        try (var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> process,
                _ -> {
                    process.alive = false;
                    return RatisProcessAdapterHealth.ready();
                }
        )) {
            var error = assertThrows(IOException.class, lifecycle::start);

            assertTrue(error.getMessage().contains("17"));
            assertFalse(lifecycle.isRunning());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void lifecycleRejectsDoubleStartWhileProcessIsRunning() throws Exception {
        var lifecycle = new RatisProcessAdapterLifecycle(
                "ratis-process-test",
                command(List.of(Path.of("lib/rockserver.jar")), List.of(), List.of()),
                Duration.ofMillis(1),
                _ -> new FakeProcess(true, 0, true)
        );

        lifecycle.start();

        assertThrows(IllegalStateException.class, lifecycle::start);
        lifecycle.close();
    }

    private static RatisProcessAdapterCommand command(List<Path> classpathEntries,
                                                      List<String> jvmArguments,
                                                      List<String> programArguments) {
        return command(Path.of("."), classpathEntries, jvmArguments, programArguments);
    }

    private static RatisProcessAdapterCommand command(Path workingDirectory,
                                                      List<Path> classpathEntries,
                                                      List<String> jvmArguments,
                                                      List<String> programArguments) {
        return new RatisProcessAdapterCommand(
                Path.of("bin/java"),
                workingDirectory,
                classpathEntries,
                "it.cavallium.rockserver.ratis.AdapterMain",
                jvmArguments,
                programArguments,
                Map.of("ROCKSERVER_NODE_ID", "n1")
        );
    }

    private static void assertInvalidDistributionClasspath(Path runtimeClasspathEntry) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(runtimeClasspathEntry),
                List.of(),
                Map.of()
        ));
    }

    private static void assertInvalidCommandMainClass(String mainClass) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("bin/java"),
                Path.of("."),
                List.of(Path.of("lib/rockserver.jar")),
                mainClass,
                List.of(),
                List.of(),
                Map.of()
        ));
    }

    private static void assertInvalidDistributionMainClass(String mainClass) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                mainClass,
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of(),
                Map.of()
        ));
    }

    private static void assertInvalidGeneratedClasspath(String classpath) {
        var properties = new Properties();
        properties.setProperty("mainClass", "it.cavallium.rockserver.ratis.AdapterMain");
        properties.setProperty("classpath", classpath);

        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterDistributionManifest.fromProperties(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                properties
        ));
    }

    private static void assertRejectsNull(String expectedMessage, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(expectedMessage, error.getMessage());
    }

    private static void assertRejectsNullCloseable(String expectedMessage,
                                                   CloseableFactory<? extends AutoCloseable> factory) {
        assertRejectsNull(expectedMessage, () -> {
            try (var ignored = factory.create()) {
                fail("closeable construction unexpectedly succeeded");
            }
        });
    }

    @FunctionalInterface
    private interface CloseableFactory<T extends AutoCloseable> {

        T create() throws Exception;
    }

    private static Properties manifestProperties() {
        var properties = new Properties();
        properties.setProperty("mainClass", "it.cavallium.rockserver.ratis.AdapterMain");
        properties.setProperty("classpath.0", "adapter/rockserver-ratis-adapter.jar");
        properties.setProperty("classpath.1", "lib/ratis-server.jar");
        properties.setProperty("jvmArgument.0", "-Xmx512m");
        properties.setProperty("environment.ROCKSERVER_RATIS_HOME", "/opt/rockserver-ratis-adapter");
        return properties;
    }

    private static void serveOneHealthRequest(ServerSocket server) {
        try (var socket = server.accept()) {
            var request = socket.getInputStream().readNBytes(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST.length());
            assertEquals(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST, new String(request, StandardCharsets.UTF_8));
            socket.getOutputStream()
                    .write(RatisProcessAdapterHealthCodec.encode(RatisProcessAdapterHealth.ready())
                            .getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void serveHealthResponses(ServerSocket server, List<RatisProcessAdapterHealth> responses) {
        for (var response : responses) {
            try (var socket = server.accept()) {
                var request = socket.getInputStream()
                        .readNBytes(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST.length());
                assertEquals(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST,
                        new String(request, StandardCharsets.UTF_8));
                socket.getOutputStream()
                        .write(RatisProcessAdapterHealthCodec.encode(response).getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static void serveOneOversizedHealthResponse(ServerSocket server) {
        try (var socket = server.accept()) {
            var request = socket.getInputStream().readNBytes(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST.length());
            assertEquals(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST, new String(request, StandardCharsets.UTF_8));
            socket.getOutputStream()
                    .write(RatisProcessAdapterHealthCodec.encode(new RatisProcessAdapterHealth(
                            RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION,
                            RatisProcessAdapterHealthStatus.READY,
                            "x".repeat(256)
                    )).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void serveOneRawHealthResponse(ServerSocket server, byte[] response) {
        try (var socket = server.accept()) {
            var request = socket.getInputStream().readNBytes(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST.length());
            assertEquals(RatisProcessAdapterSocketHealthProbe.HEALTH_REQUEST, new String(request, StandardCharsets.UTF_8));
            socket.getOutputStream().write(response);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static RatisProcessAdapterHealth sendHealthRequest(InetSocketAddress address, String request)
            throws IOException {
        return sendHealthRequest(address, request.getBytes(StandardCharsets.UTF_8));
    }

    private static RatisProcessAdapterHealth sendHealthRequest(InetSocketAddress address, byte[] request)
            throws IOException {
        return sendHealthRequest(address, request, false);
    }

    private static RatisProcessAdapterHealth sendTruncatedHealthRequest(InetSocketAddress address)
            throws IOException {
        return sendHealthRequest(address, "HEALTH".getBytes(StandardCharsets.UTF_8), true);
    }

    private static RatisProcessAdapterHealth sendHealthRequest(InetSocketAddress address,
                                                               byte[] request,
                                                               boolean shutdownOutput) throws IOException {
        try (var socket = new Socket()) {
            socket.connect(address, 5_000);
            socket.setSoTimeout(5_000);
            socket.getOutputStream().write(request);
            socket.getOutputStream().flush();
            if (shutdownOutput) {
                socket.shutdownOutput();
            }
            return RatisProcessAdapterHealthCodec.decode(new String(
                    socket.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8
            ));
        }
    }

    private static ServerSocket healthListener(RatisProcessAdapterSocketHealthServer server) throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("serverSocket");
        field.setAccessible(true);
        return (ServerSocket) field.get(server);
    }

    private static Throwable invokeHealthServe(RatisProcessAdapterSocketHealthServer server, Socket socket)
            throws Exception {
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod("serve", Socket.class, long.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, socket, System.nanoTime())
        ).getCause();
    }

    private static RatisProcessAdapterHealth invokeHealthSuppliedHealth(RatisProcessAdapterSocketHealthServer server,
                                                                       long startNanos) throws Exception {
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod("suppliedHealth", long.class);
        method.setAccessible(true);
        return (RatisProcessAdapterHealth) method.invoke(server, startNanos);
    }

    private static Throwable invokeHealthSuppliedHealthFailure(RatisProcessAdapterSocketHealthServer server,
                                                               long startNanos) throws Exception {
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod("suppliedHealth", long.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, startNanos)
        ).getCause();
    }

    private static Throwable invokeHealthCloseRejected(RatisProcessAdapterSocketHealthServer server, Socket socket)
            throws Exception {
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod("closeRejected", Socket.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, socket)
        ).getCause();
    }

    private static void invokeHealthCloseAfterFailedHandoff(RatisProcessAdapterSocketHealthServer server,
                                                            Socket socket,
                                                            Throwable failure) throws Exception {
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod(
                "closeAfterFailedHandoff",
                Socket.class,
                Throwable.class
        );
        method.setAccessible(true);
        method.invoke(server, socket, failure);
    }

    private static void invokeHealthFailRequestWorker(RatisProcessAdapterSocketHealthServer server,
                                                      IOException failure,
                                                      ServerSocket listener) throws Exception {
        installHealthListener(server, listener);
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod("failRequestWorker", IOException.class);
        method.setAccessible(true);
        method.invoke(server, failure);
    }

    private static void invokeHealthFailAcceptLoop(RatisProcessAdapterSocketHealthServer server,
                                                   IOException failure,
                                                   ServerSocket listener) throws Exception {
        installHealthListener(server, listener);
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod(
                "failAcceptLoop",
                ServerSocket.class,
                IOException.class
        );
        method.setAccessible(true);
        method.invoke(server, listener, failure);
    }

    private static Throwable invokeHealthStartListener(RatisProcessAdapterSocketHealthServer server,
                                                       ServerSocket listener) throws Exception {
        var method = RatisProcessAdapterSocketHealthServer.class.getDeclaredMethod("startListener", ServerSocket.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, listener)
        ).getCause();
    }

    private static void installHealthListener(RatisProcessAdapterSocketHealthServer server, ServerSocket listener)
            throws Exception {
        var listenerField = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("serverSocket");
        listenerField.setAccessible(true);
        listenerField.set(server, listener);
        var runningField = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.setBoolean(server, true);
    }

    private static void clearHealthListener(RatisProcessAdapterSocketHealthServer server) throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("serverSocket");
        field.setAccessible(true);
        field.set(server, null);
    }

    private static ThreadPoolExecutor healthOperationExecutor(RatisProcessAdapterSocketHealthServer server)
            throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("operationExecutor");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(server);
    }

    private static ExecutorService replaceHealthRequestExecutor(RatisProcessAdapterSocketHealthServer server,
                                                                ExecutorService replacement) throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("requestExecutor");
        field.setAccessible(true);
        var previous = (ExecutorService) field.get(server);
        field.set(server, replacement);
        return previous;
    }

    private static ExecutorService replaceHealthAcceptExecutor(RatisProcessAdapterSocketHealthServer server,
                                                               ExecutorService replacement) throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("acceptExecutor");
        field.setAccessible(true);
        var previous = (ExecutorService) field.get(server);
        field.set(server, replacement);
        return previous;
    }

    @SuppressWarnings("unchecked")
    private static Set<Socket> acceptedHealthSocketSet(RatisProcessAdapterSocketHealthServer server) throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("acceptedSockets");
        field.setAccessible(true);
        var acceptedSockets = field.get(server);
        var sockets = acceptedSockets.getClass().getDeclaredField("sockets");
        sockets.setAccessible(true);
        return (Set<Socket>) sockets.get(acceptedSockets);
    }

    @SuppressWarnings("BusyWait")
    private static void awaitAcceptedHealthSocket(RatisProcessAdapterSocketHealthServer server) throws Exception {
        var field = RatisProcessAdapterSocketHealthServer.class.getDeclaredField("acceptedSockets");
        field.setAccessible(true);
        var acceptedSockets = field.get(server);
        var size = acceptedSockets.getClass().getDeclaredMethod("size");
        size.setAccessible(true);
        var timeoutNanos = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < timeoutNanos) {
            if ((int) size.invoke(acceptedSockets) > 0) {
                return;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("Health server did not track the accepted socket");
    }

    private static void assertPeerDisconnected(Socket socket) {
        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            try {
                assertEquals(-1, socket.getInputStream().read());
            } catch (SocketTimeoutException e) {
                throw e;
            } catch (IOException ignored) {
                // A reset is also a prompt peer disconnect.
            }
        });
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        var interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    private static final class ErroringSocket extends Socket {

        private final AssertionError failure;
        private final AssertionError closeFailure;

        private ErroringSocket(AssertionError failure) {
            this(failure, null);
        }

        private ErroringSocket(AssertionError failure, AssertionError closeFailure) {
            this.failure = failure;
            this.closeFailure = closeFailure;
        }

        @Override
        public InputStream getInputStream() {
            throw failure;
        }

        @Override
        public void close() {
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class HealthRequestSocket extends Socket {

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream("HEALTH\n".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }
    }

    private static final class ErroringCloseSocket extends Socket {

        private final AssertionError failure;

        private ErroringCloseSocket(AssertionError failure) {
            this.failure = failure;
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream("HEALTH\n".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public void close() {
            throw failure;
        }
    }

    private static final class ErroringCloseServerSocket extends ServerSocket {

        private final AssertionError closeFailure;

        private ErroringCloseServerSocket(AssertionError closeFailure) throws IOException {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            throw closeFailure;
        }
    }

    private static final class CloseOnceIOExceptionServerSocket extends ServerSocket {

        private final String message;
        private int closeCalls;

        private CloseOnceIOExceptionServerSocket(String message) throws IOException {
            this.message = message;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (closeCalls == 1) {
                throw new IOException(message);
            }
            super.close();
        }

        private int closeCalls() {
            return closeCalls;
        }
    }

    private static final class FakeProcess extends Process {

        private final int exitCode;
        private final boolean exitOnDestroy;
        private boolean exitOnForcefulDestroy;
        private final boolean waitFullTimeout;
        private final List<Long> waitTimeoutMillis = new java.util.ArrayList<>();
        private boolean alive;
        private boolean destroyCalled;
        private boolean destroyForciblyCalled;
        private boolean interruptNextWait;
        private boolean interruptedWaitObserved;
        private boolean exitAfterInterruptedWait;
        private Throwable destroyFailure;
        private Throwable destroyForciblyFailure;

        private FakeProcess(boolean alive, int exitCode, boolean exitOnDestroy) {
            this(alive, exitCode, exitOnDestroy, true, false);
        }

        private FakeProcess(boolean alive,
                            int exitCode,
                            boolean exitOnDestroy,
                            boolean exitOnForcefulDestroy,
                            boolean waitFullTimeout) {
            this.alive = alive;
            this.exitCode = exitCode;
            this.exitOnDestroy = exitOnDestroy;
            this.exitOnForcefulDestroy = exitOnForcefulDestroy;
            this.waitFullTimeout = waitFullTimeout;
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream getErrorStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public int waitFor() {
            alive = false;
            return exitCode;
        }

        @Override
        public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            var timeoutMillis = unit.toMillis(timeout);
            waitTimeoutMillis.add(timeoutMillis);
            if (interruptNextWait) {
                interruptNextWait = false;
                interruptedWaitObserved = true;
                throw new InterruptedException("wait interrupted");
            }
            if (exitAfterInterruptedWait && interruptedWaitObserved) {
                alive = false;
            }
            if (waitFullTimeout && alive) {
                Thread.sleep(timeoutMillis);
            }
            if (destroyCalled && exitOnDestroy) {
                alive = false;
            }
            return !alive;
        }

        @Override
        public int exitValue() {
            if (alive) {
                throw new IllegalThreadStateException("still running");
            }
            return exitCode;
        }

        @Override
        public void destroy() {
            destroyCalled = true;
            throwUnchecked(destroyFailure);
            if (exitOnDestroy) {
                alive = false;
            }
        }

        @Override
        public Process destroyForcibly() {
            destroyForciblyCalled = true;
            throwUnchecked(destroyForciblyFailure);
            if (exitOnForcefulDestroy) {
                alive = false;
            }
            return this;
        }

        @Override
        public boolean isAlive() {
            return alive;
        }

        private static void throwUnchecked(Throwable failure) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure != null) {
                throw new AssertionError("Unsupported fake process failure", failure);
            }
        }
    }

    private static final class ErroringExecutorService extends AbstractExecutorService {

        private final Error failure;

        private ErroringExecutorService(Error failure) {
            this.failure = failure;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public @NotNull List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
            return false;
        }

        @Override
        public void execute(@NotNull Runnable command) {
            throw failure;
        }
    }
}
