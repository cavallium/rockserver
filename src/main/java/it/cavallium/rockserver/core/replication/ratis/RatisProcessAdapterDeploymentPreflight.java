package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarFile;

public final class RatisProcessAdapterDeploymentPreflight {

    public static final int MAX_PREFLIGHT_REPORT_BYTES = 1024 * 1024;
    public static final int MAX_DEPLOYMENT_EVIDENCE_BYTES = 4 * 1024 * 1024;
    public static final long MAX_LAUNCH_FILE_BYTES = 1024L * 1024 * 1024;

    private RatisProcessAdapterDeploymentPreflight() {
    }

    public static Report check(RatisProcessAdapterLaunchPlan plan) {
        requireNonNull(plan, "plan");
        var problems = new ArrayList<String>();

        checkJavaExecutable(plan.command().javaExecutable(), problems);
        checkReadableClasspath(plan.adapterHome(), plan.command().classpathEntries(), problems);
        checkWritableDirectory(plan.command().workingDirectory(), "workingDirectory", problems);
        checkWritableDirectory(plan.adapterDataRoot(), "adapterDataRoot", problems);
        checkWritableFileParent(plan.configPath(), "configPath", problems);
        checkWritableFileParent(plan.metadataPath(), "metadataPath", problems);
        checkConfigAndMetadataPaths(plan, problems);
        checkPartitionLayout(plan.adapterConfig().partitions(), plan, problems);
        checkListeners(plan.healthAddress(), plan.rpcAddress(), problems);
        checkCommandMetadata(plan, problems);

        return new Report(problems);
    }

    public static void verify(RatisProcessAdapterLaunchPlan plan) throws IOException {
        check(plan).throwIfFailed();
    }

    public static Report writeReport(RatisProcessAdapterLaunchPlan plan, Path reportPath) throws IOException {
        requireNonNull(reportPath, "reportPath");
        var report = check(plan);
        report.write(reportPath);
        return report;
    }

    public static Evidence evidence(RatisProcessAdapterLaunchPlan plan) throws IOException {
        var resolvedPlan = requireNonNull(plan, "plan");
        verify(resolvedPlan);
        return RatisProcessAdapterLaunchPlan.withConfigPairLock(resolvedPlan.configPath(), () -> {
            var plannedConfigProperties = resolvedPlan.adapterConfigProperties();
            var plannedMetadataProperties = resolvedPlan.metadataConfig().toProperties();
            return new Evidence(
                    resolvedPlan.command().toCommandLine(),
                    resolvedPlan.command().environment(),
                    resolvedPlan.command().workingDirectory(),
                    resolvedPlan.configPath(),
                    resolvedPlan.metadataPath(),
                    resolvedPlan.healthAddress(),
                    resolvedPlan.rpcAddress(),
                    launchFiles(resolvedPlan),
                    PlannedPropertiesEvidence.from("plannedConfig", plannedConfigProperties),
                    PlannedPropertiesEvidence.from("plannedMetadata", plannedMetadataProperties),
                    matchingPublishedProperties(
                            "publishedConfig",
                            resolvedPlan.configPath(),
                            plannedConfigProperties,
                            RatisProcessAdapterConfig.readProperties(resolvedPlan.configPath())
                    ),
                    matchingPublishedProperties(
                            "publishedMetadata",
                            resolvedPlan.metadataPath(),
                            plannedMetadataProperties,
                            RatisProcessAdapterMetadataConfig.readProperties(resolvedPlan.metadataPath())
                    ),
                    resolvedPlan.adapterConfig().partitions().stream()
                            .map(PartitionEvidence::from)
                            .toList()
            );
        });
    }

    public static void writeEvidence(RatisProcessAdapterLaunchPlan plan, Path evidencePath) throws IOException {
        requireNonNull(evidencePath, "evidencePath");
        evidence(plan).write(evidencePath);
    }

    private static List<FileEvidence> launchFiles(RatisProcessAdapterLaunchPlan plan) throws IOException {
        var files = new ArrayList<FileEvidence>();
        files.add(FileEvidence.from("javaExecutable", plan.command().javaExecutable()));
        for (var classpathEntry : plan.command().classpathEntries()) {
            files.add(FileEvidence.from("classpath", classpathEntry));
        }
        return List.copyOf(files);
    }

    private static PlannedPropertiesEvidence matchingPublishedProperties(String role,
                                                                         Path path,
                                                                         Properties planned,
                                                                         Properties published)
            throws IOException {
        requireNonNull(planned, "planned");
        requireNonNull(published, "published");
        var plannedEvidence = PlannedPropertiesEvidence.from("planned " + role, planned);
        var publishedEvidence = PlannedPropertiesEvidence.from(role, published);
        if (!planned.equals(published)) {
            throw new IOException(role + " at " + path + " does not match planned properties: expected propertyCount="
                    + plannedEvidence.propertyCount() + " sha256=" + plannedEvidence.sha256()
                    + " but found propertyCount=" + publishedEvidence.propertyCount()
                    + " sha256=" + publishedEvidence.sha256());
        }
        return publishedEvidence;
    }

    private static void checkJavaExecutable(Path path, List<String> problems) {
        var name = "javaExecutable";
        checkAbsolute(path, name, problems);
        if (!Files.exists(path)) {
            problems.add(name + " does not exist: " + path);
            return;
        }
        if (!Files.isRegularFile(path)) {
            problems.add(name + " must be a regular file: " + path);
        } else {
            checkNonEmptyFile(path, name, problems);
        }
        if (!Files.isExecutable(path)) {
            problems.add(name + " must be executable: " + path);
        }
    }

    private static void checkReadableClasspath(Path adapterHome,
                                               List<Path> classpathEntries,
                                               List<String> problems) {
        Path realAdapterHome = null;
        try {
            realAdapterHome = adapterHome.toRealPath();
            if (!Files.isDirectory(realAdapterHome)) {
                problems.add("adapterHome must resolve to a directory: " + adapterHome + " -> " + realAdapterHome);
            }
        } catch (IOException e) {
            problems.add("adapterHome cannot be resolved: " + adapterHome + " (" + e.getMessage() + ")");
        }
        var normalizedClasspathEntries = new HashSet<Path>();
        var realClasspathEntries = new HashSet<Path>();
        for (var entry : classpathEntries) {
            checkAbsolute(entry, "classpath entry", problems);
            var normalizedEntry = entry.toAbsolutePath().normalize();
            if (!normalizedClasspathEntries.add(normalizedEntry)) {
                problems.add("classpath entry must not duplicate another planned classpath entry: " + entry);
            }
            if (!Files.exists(entry)) {
                problems.add("classpath entry does not exist: " + entry);
                continue;
            }
            if (!Files.isRegularFile(entry)) {
                problems.add("classpath entry must be a regular file: " + entry);
            } else {
                checkNonEmptyFile(entry, "classpath entry", problems);
                checkReadableJar(entry, problems);
            }
            if (!Files.isReadable(entry)) {
                problems.add("classpath entry must be readable: " + entry);
            }
            if (realAdapterHome != null) {
                var realEntry = checkClasspathContained(realAdapterHome, entry, problems);
                if (realEntry != null && !realClasspathEntries.add(realEntry)) {
                    problems.add("classpath entry must not resolve to duplicate classpath file: "
                            + entry + " -> " + realEntry);
                }
            }
        }
    }

    private static Path checkClasspathContained(Path realAdapterHome, Path entry, List<String> problems) {
        try {
            var realEntry = entry.toRealPath();
            if (!realEntry.startsWith(realAdapterHome)) {
                problems.add("classpath entry must resolve inside adapterHome: " + entry + " -> " + realEntry);
            }
            return realEntry;
        } catch (IOException e) {
            problems.add("classpath entry cannot be resolved: " + entry + " (" + e.getMessage() + ")");
            return null;
        }
    }

    private static void checkNonEmptyFile(Path path, String name, List<String> problems) {
        try {
            if (Files.size(path) == 0L) {
                problems.add(name + " must not be empty: " + path);
            }
        } catch (IOException e) {
            problems.add(name + " size cannot be read: " + path + " (" + e.getMessage() + ")");
        }
    }

    private static void checkReadableJar(Path path, List<String> problems) {
        try (var jar = new JarFile(path.toFile(), false)) {
            jar.size();
        } catch (IOException e) {
            problems.add("classpath entry must be a readable JAR file: " + path + " (" + e.getMessage() + ")");
        }
    }

    private static void checkWritableDirectory(Path path, String name, List<String> problems) {
        checkAbsolute(path, name, problems);
        try {
            RatisProcessAdapterFileWriter.createDirectoriesDurably(path);
            if (!Files.isDirectory(path)) {
                problems.add(name + " must be a directory: " + path);
                return;
            }
            checkCanCreateTempFile(path, name, problems);
        } catch (IOException e) {
            problems.add(name + " cannot be prepared: " + path + " (" + e.getMessage() + ")");
        }
    }

    private static void checkWritableFileParent(Path path, String name, List<String> problems) {
        checkAbsolute(path, name, problems);
        if (Files.isDirectory(path)) {
            problems.add(name + " must be a file path, not a directory: " + path);
        }
        var parent = path.getParent();
        if (parent == null) {
            problems.add(name + " must have a parent directory: " + path);
            return;
        }
        checkWritableDirectory(parent, name + " parent", problems);
    }

    private static void checkCanCreateTempFile(Path directory, String name, List<String> problems) {
        Path tempPath = null;
        try {
            tempPath = Files.createTempFile(directory, ".rockserver-ratis-preflight-", ".tmp");
        } catch (IOException e) {
            problems.add(name + " must be writable: " + directory + " (" + e.getMessage() + ")");
        } finally {
            if (tempPath != null) {
                try {
                    Files.deleteIfExists(tempPath);
                } catch (IOException e) {
                    problems.add(name + " preflight temp file could not be removed: " + tempPath
                            + " (" + e.getMessage() + ")");
                }
            }
        }
    }

    private static void checkConfigAndMetadataPaths(RatisProcessAdapterLaunchPlan plan, List<String> problems) {
        checkDistinctConfigAndMetadataPaths(plan, problems);
        var protectedDirectories = new ArrayList<NamedPath>();
        if (Files.isDirectory(plan.adapterHome())) {
            protectedDirectories.add(new NamedPath("adapterHome", plan.adapterHome()));
        }
        protectedDirectories.add(new NamedPath("workingDirectory", plan.command().workingDirectory()));
        protectedDirectories.add(new NamedPath("adapterDataRoot", plan.adapterDataRoot()));
        for (var partition : plan.adapterConfig().partitions()) {
            protectedDirectories.add(new NamedPath("partition " + partition.partitionId() + " dataPath",
                    partition.dataPath()));
        }
        for (var directory : protectedDirectories) {
            checkNotContained("configPath", plan.configPath(), directory, problems);
            checkNotContained("metadataPath", plan.metadataPath(), directory, problems);
        }
    }

    private static void checkDistinctConfigAndMetadataPaths(RatisProcessAdapterLaunchPlan plan, List<String> problems) {
        try {
            if (RatisProcessAdapterPaths.resolveExistingAncestor(plan.configPath())
                    .equals(RatisProcessAdapterPaths.resolveExistingAncestor(plan.metadataPath()))) {
                problems.add("configPath and metadataPath must be distinct: " + plan.configPath());
            }
        } catch (IOException e) {
            problems.add("configPath and metadataPath could not be resolved safely: " + plan.configPath() + " / "
                    + plan.metadataPath() + " (" + e.getMessage() + ")");
        }
    }

    private static void checkNotContained(String pathName,
                                          Path path,
                                          NamedPath directory,
                                          List<String> problems) {
        try {
            if (containsPath(directory.path(), path)) {
                problems.add(pathName + " must not be inside " + directory.name() + ": " + path);
            }
        } catch (IOException e) {
            problems.add(pathName + " and " + directory.name() + " could not be resolved safely: " + path + " / "
                    + directory.path() + " (" + e.getMessage() + ")");
        }
    }

    private static void checkPartitionLayout(List<RatisPartitionDescriptor> partitions,
                                             RatisProcessAdapterLaunchPlan plan,
                                             List<String> problems) {
        if (partitions.isEmpty()) {
            problems.add("adapterConfig must contain at least one partition descriptor");
            return;
        }

        var localPeerIds = new HashSet<String>();
        var dataPaths = new ArrayList<NamedPath>();
        for (var partition : partitions) {
            localPeerIds.add(partition.localPeerId());
            checkCanonicalPartitionDataPath(partition, plan.adapterDataRoot(), problems);
            checkWritableDirectory(partition.dataPath(), "partition " + partition.partitionId() + " dataPath",
                    problems);
            dataPaths.add(new NamedPath("partition " + partition.partitionId() + " dataPath", partition.dataPath()));
            checkPartitionPeers(partition, problems);
            checkLocalPeerListenerCollisions(partition, plan, problems);
        }
        if (localPeerIds.size() != 1) {
            problems.add("all adapter partitions must use the same local peer id: " + localPeerIds);
        }
        checkCommandNodeId(localPeerIds, plan, problems);
        var workingDirectory = new NamedPath("workingDirectory", plan.command().workingDirectory());
        checkDirectoryOverlap(
                workingDirectory,
                List.of(new NamedPath("adapterDataRoot", plan.adapterDataRoot())),
                problems
        );
        checkDirectoryOverlap(workingDirectory, dataPaths, problems);
        checkPairwiseDirectoryOverlap(dataPaths, problems);
    }

    private static void checkCanonicalPartitionDataPath(RatisPartitionDescriptor partition,
                                                        Path adapterDataRoot,
                                                        List<String> problems) {
        final Path expectedPath;
        try {
            expectedPath = normalize(RatisProcessAdapterConfig.canonicalDataPath(
                    adapterDataRoot,
                    partition.partitionId()
            ));
        } catch (IllegalArgumentException e) {
            problems.add("partition " + partition.partitionId() + " dataPath cannot be derived safely: "
                    + e.getMessage());
            return;
        }
        var actualPath = normalize(partition.dataPath());
        if (!expectedPath.equals(actualPath)) {
            problems.add("partition " + partition.partitionId() + " dataPath must equal canonical adapterDataRoot "
                    + "child " + expectedPath + ": " + actualPath);
        }
        try {
            if (!RatisProcessAdapterPaths.resolvesInside(adapterDataRoot, actualPath)) {
                problems.add("partition " + partition.partitionId()
                        + " dataPath must resolve inside adapterDataRoot: " + actualPath + " / " + adapterDataRoot);
            }
        } catch (IOException e) {
            problems.add("partition " + partition.partitionId() + " dataPath could not be resolved safely: "
                    + actualPath + " / " + adapterDataRoot + " (" + e.getMessage() + ")");
        }
    }

    private static void checkCommandNodeId(Set<String> localPeerIds,
                                           RatisProcessAdapterLaunchPlan plan,
                                           List<String> problems) {
        var nodeId = plan.command().environment().get(RatisProcessAdapterLaunchPlan.NODE_ID_ENV);
        if (nodeId == null || nodeId.isBlank()) {
            problems.add(RatisProcessAdapterLaunchPlan.NODE_ID_ENV + " must be present in the adapter environment");
            return;
        }
        if (localPeerIds.size() == 1) {
            var localPeerId = localPeerIds.iterator().next();
            if (!localPeerId.equals(nodeId)) {
                problems.add(RatisProcessAdapterLaunchPlan.NODE_ID_ENV + " must match localPeerId "
                        + localPeerId + ": " + nodeId);
            }
        }
    }

    private static void checkPartitionPeers(RatisPartitionDescriptor partition, List<String> problems) {
        var peerIds = new HashSet<String>();
        var endpoints = new HashSet<String>();
        for (var peer : partition.voters()) {
            if (!peerIds.add(peer.peerId())) {
                problems.add("partition " + partition.partitionId() + " has duplicate peer id: " + peer.peerId());
            }
            var endpointKey = normalizedEndpoint(peer.endpoint(), "partition " + partition.partitionId()
                    + " peer " + peer.peerId(), problems);
            if (endpointKey != null && !endpoints.add(endpointKey)) {
                problems.add("partition " + partition.partitionId() + " has duplicate peer endpoint: "
                        + peer.endpoint());
            }
        }
    }

    private static void checkLocalPeerListenerCollisions(RatisPartitionDescriptor partition,
                                                         RatisProcessAdapterLaunchPlan plan,
                                                         List<String> problems) {
        var localPeer = partition.voters().stream()
                .filter(peer -> peer.peerId().equals(partition.localPeerId()))
                .findFirst()
                .orElse(null);
        if (localPeer == null) {
            problems.add("partition " + partition.partitionId() + " localPeerId is not present in voters: "
                    + partition.localPeerId());
            return;
        }
        var localEndpoint = parsedEndpoint(
                localPeer.endpoint(),
                "partition " + partition.partitionId() + " local peer " + partition.localPeerId(),
                problems
        );
        if (localEndpoint == null) {
            return;
        }
        checkLocalPeerListenerCollision(plan.healthAddress(), "healthAddress", partition, localEndpoint, problems);
        checkLocalPeerListenerCollision(plan.rpcAddress(), "rpcAddress", partition, localEndpoint, problems);
    }

    private static void checkLocalPeerListenerCollision(InetSocketAddress listener,
                                                        String listenerName,
                                                        RatisPartitionDescriptor partition,
                                                        Endpoint localEndpoint,
                                                        List<String> problems) {
        if (listener.getPort() != localEndpoint.port()) {
            return;
        }
        if (sameHost(listener.getHostString(), localEndpoint.host())
                || isAnyLocalAddress(listener)
                || isAnyLocalHost(localEndpoint.host())) {
            problems.add(listenerName + " must not share the local Ratis listener for partition "
                    + partition.partitionId() + " peer " + partition.localPeerId() + ": "
                    + listener + " / " + localEndpoint.source());
        }
    }

    private static String normalizedEndpoint(String endpoint, String name, List<String> problems) {
        var parsed = parsedEndpoint(endpoint, name, problems);
        if (parsed == null) {
            return null;
        }
        var scheme = parsed.scheme() == null ? "" : parsed.scheme().toLowerCase(Locale.ROOT);
        return scheme + "://" + parsed.host().toLowerCase(Locale.ROOT) + ":" + parsed.port();
    }

    private static Endpoint parsedEndpoint(String endpoint, String name, List<String> problems) {
        URI uri;
        try {
            uri = URI.create(endpoint);
        } catch (IllegalArgumentException e) {
            problems.add(name + " endpoint must be a URI: " + endpoint);
            return null;
        }
        if (uri.getScheme() == null || uri.getScheme().isBlank()) {
            problems.add(name + " endpoint must include a scheme: " + endpoint);
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            problems.add(name + " endpoint must include a host: " + endpoint);
        }
        if (uri.getPort() <= 0 || uri.getPort() > 65535) {
            problems.add(name + " endpoint port must be between 1 and 65535: " + endpoint);
        }
        if (uri.getHost() == null || uri.getPort() <= 0 || uri.getPort() > 65535) {
            return null;
        }
        return new Endpoint(uri.getScheme(), uri.getHost(), uri.getPort(), endpoint);
    }

    private static void checkListeners(InetSocketAddress healthAddress,
                                       InetSocketAddress rpcAddress,
                                       List<String> problems) {
        checkListener(healthAddress, "healthAddress", problems);
        checkListener(rpcAddress, "rpcAddress", problems);
        if (healthAddress.getPort() == rpcAddress.getPort()
                && (sameHost(healthAddress, rpcAddress)
                || isAnyLocalAddress(healthAddress)
                || isAnyLocalAddress(rpcAddress))) {
            problems.add("healthAddress and rpcAddress must not share the same listener port: "
                    + healthAddress + " / " + rpcAddress);
        }
    }

    private static void checkListener(InetSocketAddress address, String name, List<String> problems) {
        if (address.isUnresolved()) {
            problems.add(name + " host must resolve before starting the adapter process: " + address.getHostString());
        }
    }

    private static boolean sameHost(InetSocketAddress first, InetSocketAddress second) {
        return sameHost(first.getHostString(), second.getHostString());
    }

    private static boolean sameHost(String first, String second) {
        if (first.equalsIgnoreCase(second)) {
            return true;
        }
        if (isLoopbackHost(first) && isLoopbackHost(second)) {
            return true;
        }
        return resolvedHostsOverlap(first, second);
    }

    private static boolean isAnyLocalAddress(InetSocketAddress address) {
        InetAddress resolved = address.getAddress();
        return resolved != null && resolved.isAnyLocalAddress();
    }

    private static boolean isAnyLocalHost(String host) {
        return "0.0.0.0".equals(host) || "::".equals(host) || "0:0:0:0:0:0:0:0".equals(host);
    }

    private static boolean isLoopbackHost(String host) {
        if ("localhost".equalsIgnoreCase(host)) {
            return true;
        }
        return Arrays.stream(resolveAll(host))
                .anyMatch(InetAddress::isLoopbackAddress);
    }

    private static boolean resolvedHostsOverlap(String first, String second) {
        var firstAddresses = resolveAll(first);
        var secondAddresses = resolveAll(second);
        if (firstAddresses.length == 0 || secondAddresses.length == 0) {
            return false;
        }
        for (var firstAddress : firstAddresses) {
            for (var secondAddress : secondAddresses) {
                if (firstAddress.equals(secondAddress)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static InetAddress[] resolveAll(String host) {
        try {
            return InetAddress.getAllByName(host);
        } catch (UnknownHostException _) {
            return new InetAddress[0];
        }
    }

    private static void checkCommandMetadata(RatisProcessAdapterLaunchPlan plan, List<String> problems) {
        checkCommandEnvironmentPath(plan, RatisProcessAdapterLaunchPlan.CONFIG_PATH_ENV, plan.configPath(), problems);
        checkCommandEnvironmentPath(plan, RatisProcessAdapterLaunchPlan.METADATA_PATH_ENV, plan.metadataPath(),
                problems);
        checkCommandEnvironmentPath(plan, RatisProcessAdapterLaunchPlan.ADAPTER_DATA_ROOT_ENV, plan.adapterDataRoot(),
                problems);
        checkCommandMetadataGroupTopology(
                plan,
                plan.adapterConfig().metadataGroupTopologySha256(),
                problems
        );

        var expectedProgramArguments = List.of(
                "--config=" + processVisiblePath(plan.configPath()),
                "--metadata=" + processVisiblePath(plan.metadataPath()),
                "--health-host=" + plan.healthAddress().getHostString(),
                "--health-port=" + plan.healthAddress().getPort(),
                "--rpc-host=" + plan.rpcAddress().getHostString(),
                "--rpc-port=" + plan.rpcAddress().getPort()
        );
        if (!plan.command().programArguments().equals(expectedProgramArguments)) {
            problems.add("programArguments must match generated adapter launch arguments: expected "
                    + expectedProgramArguments + " but found " + plan.command().programArguments());
        }
    }

    private static void checkCommandEnvironmentPath(RatisProcessAdapterLaunchPlan plan,
                                                    String key,
                                                    Path expectedPath,
                                                    List<String> problems) {
        var value = plan.command().environment().get(key);
        if (value == null || value.isBlank()) {
            problems.add(key + " must be present in the adapter environment");
            return;
        }
        var expected = processVisiblePath(expectedPath).toString();
        if (!expected.equals(value)) {
            problems.add(key + " must point at " + expected + ": " + value);
        }
    }

    private static void checkCommandMetadataGroupTopology(RatisProcessAdapterLaunchPlan plan,
                                                          String expectedValue,
                                                          List<String> problems) {
        var key = RatisProcessAdapterLaunchPlan.METADATA_GROUP_TOPOLOGY_SHA256_ENV;
        var value = plan.command().environment().get(key);
        if (value == null || value.isBlank()) {
            problems.add(key + " must be present in the adapter environment");
            return;
        }
        if (!expectedValue.equals(value)) {
            problems.add(key + " must equal " + expectedValue + ": " + value);
        }
    }

    private static void checkDirectoryOverlap(NamedPath directory,
                                              List<NamedPath> otherDirectories,
                                              List<String> problems) {
        for (var other : otherDirectories) {
            checkNoOverlap(directory, other, problems);
        }
    }

    private static void checkPairwiseDirectoryOverlap(List<NamedPath> directories, List<String> problems) {
        for (int i = 0; i < directories.size(); i++) {
            for (int j = i + 1; j < directories.size(); j++) {
                var first = directories.get(i);
                var second = directories.get(j);
                checkNoOverlap(first, second, problems);
            }
        }
    }

    private static void checkNoOverlap(NamedPath first, NamedPath second, List<String> problems) {
        try {
            if (overlaps(first.path(), second.path())) {
                problems.add(first.name() + " must not overlap " + second.name()
                        + ": " + first.path() + " / " + second.path());
            }
        } catch (IOException e) {
            problems.add(first.name() + " and " + second.name() + " could not be resolved safely: "
                    + first.path() + " / " + second.path() + " (" + e.getMessage() + ")");
        }
    }

    private static boolean overlaps(Path first, Path second) throws IOException {
        return containsPath(first, second) || containsPath(second, first);
    }

    private static boolean containsPath(Path directory, Path path) throws IOException {
        return RatisProcessAdapterPaths.resolvesInside(directory, path);
    }

    private static Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }

    private static Path processVisiblePath(Path path) {
        return path.toAbsolutePath().normalize();
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static <T> List<T> copyList(List<T> values, String name, String entryName) {
        requireNonNull(values, name);
        for (var value : values) {
            if (value == null) {
                throw new IllegalArgumentException(entryName + " must not be null");
            }
        }
        return List.copyOf(values);
    }

    private static Map<String, String> copyEnvironment(Map<String, String> values) {
        var name = "environment";
        requireNonNull(values, name);
        var copy = new java.util.LinkedHashMap<String, String>();
        for (var entry : values.entrySet()) {
            var key = requireNonNull(entry.getKey(), name + " key");
            var value = requireNonNull(entry.getValue(), name + " value");
            copy.put(key, value);
        }
        return Map.copyOf(copy);
    }

    private static void checkAbsolute(Path path, String name, List<String> problems) {
        if (!path.isAbsolute()) {
            problems.add(name + " must be absolute: " + path);
        }
    }

    public record Report(List<String> problems) {

        public Report {
            problems = copyList(problems, "problems", "problems entry");
        }

        public boolean accepted() {
            return problems.isEmpty();
        }

        public void throwIfFailed() throws IOException {
            if (!accepted()) {
                throw new IOException(message());
            }
        }

        public String message() {
            if (accepted()) {
                return "Ratis adapter deployment preflight accepted";
            }
            return "Ratis adapter deployment preflight failed: " + String.join("; ", problems);
        }

        public List<String> asLines() {
            var lines = new ArrayList<String>();
            lines.add(evidenceLine("accepted", accepted()));
            lines.add(evidenceLine("problem.count", problems.size()));
            for (int problemIndex = 0; problemIndex < problems.size(); problemIndex++) {
                lines.add(evidenceLine("problem." + problemIndex, problems.get(problemIndex)));
            }
            lines.add(evidenceLine("message", message()));
            return List.copyOf(lines);
        }

        public String asText() {
            return String.join("\n", asLines()) + "\n";
        }

        public void write(Path reportPath) throws IOException {
            requireNonNull(reportPath, "reportPath");
            RatisProcessAdapterFileWriter.writeString(
                    reportPath,
                    asText(),
                    MAX_PREFLIGHT_REPORT_BYTES,
                    "Ratis adapter deployment preflight report"
            );
        }
    }

    public record Evidence(List<String> commandLine,
                           Map<String, String> environment,
                           Path workingDirectory,
                           Path configPath,
                           Path metadataPath,
                           InetSocketAddress healthAddress,
                           InetSocketAddress rpcAddress,
                           List<FileEvidence> launchFiles,
                           PlannedPropertiesEvidence plannedConfig,
                           PlannedPropertiesEvidence plannedMetadata,
                           PlannedPropertiesEvidence publishedConfig,
                           PlannedPropertiesEvidence publishedMetadata,
                           List<PartitionEvidence> partitions) {

        public Evidence {
            commandLine = copyList(commandLine, "commandLine", "commandLine entry");
            environment = copyEnvironment(environment);
            requireNonNull(workingDirectory, "workingDirectory");
            requireNonNull(configPath, "configPath");
            requireNonNull(metadataPath, "metadataPath");
            requireNonNull(healthAddress, "healthAddress");
            requireNonNull(rpcAddress, "rpcAddress");
            launchFiles = copyList(launchFiles, "launchFiles", "launchFiles entry");
            requireNonNull(plannedConfig, "plannedConfig");
            requireNonNull(plannedMetadata, "plannedMetadata");
            requireNonNull(publishedConfig, "publishedConfig");
            requireNonNull(publishedMetadata, "publishedMetadata");
            partitions = copyList(partitions, "partitions", "partitions entry");
        }

        public List<String> asLines() {
            var lines = new ArrayList<String>();
            lines.add(evidenceLine("command.count", commandLine.size()));
            for (int i = 0; i < commandLine.size(); i++) {
                lines.add(evidenceLine("command." + i, commandLine.get(i)));
            }
            lines.add(evidenceLine("environment.count", environment.size()));
            environment.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> lines.add(evidenceLine("environment." + entry.getKey(), entry.getValue())));
            lines.add(evidenceLine("workingDirectory", workingDirectory));
            lines.add(evidenceLine("configPath", configPath));
            lines.add(evidenceLine("metadataPath", metadataPath));
            lines.add(evidenceLine("healthAddress", addressValue(healthAddress)));
            lines.add(evidenceLine("rpcAddress", addressValue(rpcAddress)));
            lines.add(evidenceLine("launchFile.count", launchFiles.size()));
            for (int fileIndex = 0; fileIndex < launchFiles.size(); fileIndex++) {
                var file = launchFiles.get(fileIndex);
                var prefix = "launchFile." + fileIndex + ".";
                lines.add(evidenceLine(prefix + "role", file.role()));
                lines.add(evidenceLine(prefix + "path", file.path()));
                lines.add(evidenceLine(prefix + "size", file.size()));
                lines.add(evidenceLine(prefix + "sha256", file.sha256()));
            }
            plannedConfig.addLines(lines);
            plannedMetadata.addLines(lines);
            publishedConfig.addLines(lines);
            publishedMetadata.addLines(lines);

            var orderedPartitions = partitions.stream()
                    .sorted(Comparator.comparing(PartitionEvidence::partitionId))
                    .toList();
            lines.add(evidenceLine("partition.count", orderedPartitions.size()));
            for (int partitionIndex = 0; partitionIndex < orderedPartitions.size(); partitionIndex++) {
                var partition = orderedPartitions.get(partitionIndex);
                var prefix = "partition." + partitionIndex + ".";
                lines.add(evidenceLine(prefix + "partitionId", partition.partitionId()));
                lines.add(evidenceLine(prefix + "raftGroupId", partition.raftGroupId()));
                lines.add(evidenceLine(prefix + "localPeerId", partition.localPeerId()));
                lines.add(evidenceLine(prefix + "dataPath", partition.dataPath()));
                var voterEndpoints = partition.voterEndpoints().stream()
                        .sorted()
                        .toList();
                lines.add(evidenceLine(prefix + "voter.count", voterEndpoints.size()));
                for (int voterIndex = 0; voterIndex < voterEndpoints.size(); voterIndex++) {
                    lines.add(evidenceLine(prefix + "voter." + voterIndex, voterEndpoints.get(voterIndex)));
                }
            }
            return List.copyOf(lines);
        }

        public String asText() {
            return String.join("\n", asLines()) + "\n";
        }

        public void write(Path evidencePath) throws IOException {
            requireNonNull(evidencePath, "evidencePath");
            RatisProcessAdapterFileWriter.writeString(
                    evidencePath,
                    asText(),
                    MAX_DEPLOYMENT_EVIDENCE_BYTES,
                    "Ratis adapter deployment evidence"
            );
        }
    }

    public record FileEvidence(String role, Path path, long size, String sha256) {

        public FileEvidence {
            RatisAdapterBoundary.requireNonBlank(role, "role");
            requireNonNull(path, "path");
            if (size < 0) {
                throw new IllegalArgumentException("size must be >= 0");
            }
            RatisAdapterBoundary.requireNonBlank(sha256, "sha256");
        }

        private static FileEvidence from(String role, Path path) throws IOException {
            var digest = digestFile(path);
            return new FileEvidence(role, path, digest.size(), digest.sha256());
        }
    }

    public record PlannedPropertiesEvidence(String role, int propertyCount, String sha256) {

        public PlannedPropertiesEvidence {
            RatisAdapterBoundary.requireNonBlank(role, "role");
            if (propertyCount < 0) {
                throw new IllegalArgumentException("propertyCount must be >= 0");
            }
            RatisAdapterBoundary.requireNonBlank(sha256, "sha256");
        }

        private static PlannedPropertiesEvidence from(String role, Properties properties) {
            requireNonNull(properties, "properties");
            return new PlannedPropertiesEvidence(
                    role,
                    properties.size(),
                    RatisProcessAdapterDeploymentPreflight.sha256CanonicalProperties(properties)
            );
        }

        private void addLines(List<String> lines) {
            lines.add(evidenceLine(role + ".property.count", propertyCount));
            lines.add(evidenceLine(role + ".sha256", sha256));
        }
    }

    public record PartitionEvidence(String partitionId,
                                    String raftGroupId,
                                    String localPeerId,
                                    Path dataPath,
                                    List<String> voterEndpoints) {

        public PartitionEvidence {
            RatisAdapterBoundary.requireNonBlank(partitionId, "partitionId");
            RatisAdapterBoundary.requireNonBlank(raftGroupId, "raftGroupId");
            RatisAdapterBoundary.requireNonBlank(localPeerId, "localPeerId");
            requireNonNull(dataPath, "dataPath");
            voterEndpoints = copyList(voterEndpoints, "voterEndpoints", "voterEndpoints entry");
        }

        private static PartitionEvidence from(RatisPartitionDescriptor partition) {
            return new PartitionEvidence(
                    partition.partitionId(),
                    partition.raftGroupId(),
                    partition.localPeerId(),
                    partition.dataPath(),
                    partition.voters().stream()
                            .map(peer -> peer.peerId() + "=" + peer.endpoint())
                            .toList()
            );
        }
    }

    private record NamedPath(String name, Path path) {

        private NamedPath {
            RatisAdapterBoundary.requireNonBlank(name, "name");
            requireNonNull(path, "path");
        }
    }

    private record Endpoint(String scheme, String host, int port, String source) {

        private Endpoint {
            requireNonNull(host, "host");
            RatisAdapterBoundary.requireNonBlank(source, "source");
        }
    }

    private record FileDigest(long size, String sha256) {

        private FileDigest {
            if (size < 0) {
                throw new IllegalArgumentException("size must be >= 0");
            }
            RatisAdapterBoundary.requireNonBlank(sha256, "sha256");
        }
    }

    private static String evidenceLine(String key, Object value) {
        return key + "=" + escapeEvidenceValue(String.valueOf(value));
    }

    private static String escapeEvidenceValue(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }

    private static String addressValue(InetSocketAddress address) {
        return address.getHostString() + ":" + address.getPort();
    }

    private static String sha256CanonicalProperties(Properties properties) {
        var digest = sha256Digest();
        properties.stringPropertyNames().stream()
                .sorted()
                .forEach(key -> {
                    updateUtf8(digest, key);
                    updateUtf8(digest, "=");
                    updateUtf8(digest, properties.getProperty(key));
                    updateUtf8(digest, "\n");
                });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void updateUtf8(MessageDigest digest, String value) {
        digest.update(value.getBytes(StandardCharsets.UTF_8));
    }

    private static FileDigest digestFile(Path path) throws IOException {
        var admittedRealPath = path.toRealPath();
        var admittedAttributes = launchFileAttributes(admittedRealPath);
        var admittedSize = admittedAttributes.size();
        if (admittedSize > MAX_LAUNCH_FILE_BYTES) {
            throw new IOException("launch file exceeds " + MAX_LAUNCH_FILE_BYTES + " bytes: " + path);
        }
        var digest = sha256Digest();
        long size = 0;
        try (var input = Files.newInputStream(admittedRealPath, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = new byte[16 * 1024];
            while (size < admittedSize) {
                var read = input.read(buffer, 0, (int) Math.min(buffer.length, admittedSize - size));
                if (read == -1) {
                    throw new IOException("launch file was truncated while hashing: " + path);
                }
                size += read;
                digest.update(buffer, 0, read);
            }
            if (input.read() != -1) {
                throw new IOException("launch file grew while hashing: " + path);
            }
        }
        var observedRealPath = path.toRealPath();
        var observedAttributes = launchFileAttributes(observedRealPath);
        if (!admittedRealPath.equals(observedRealPath)
                || admittedAttributes.size() != observedAttributes.size()
                || !admittedAttributes.lastModifiedTime().equals(observedAttributes.lastModifiedTime())
                || !Objects.equals(admittedAttributes.fileKey(), observedAttributes.fileKey())) {
            throw new IOException("launch file changed while hashing: " + path);
        }
        return new FileDigest(size, HexFormat.of().formatHex(digest.digest()));
    }

    private static BasicFileAttributes launchFileAttributes(Path path) throws IOException {
        var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw new IOException("launch file target must be a regular file: " + path);
        }
        return attributes;
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
    }
}
