package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarFile;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

class RatisPackagingBoundaryTest {

    @Test
    void productionModuleDescriptorsDoNotRequireRatisAutomaticModules() throws IOException {
        for (var descriptor : List.of(
                Path.of("src/library/java/module-info.java"),
                Path.of("src/fatjar/java/module-info.java"),
                Path.of("src/native/java/module-info.java"))) {
            var moduleInfo = Files.readString(descriptor);

            assertFalse(moduleInfo.contains("requires ratis."),
                    descriptor + " must not place Ratis automatic modules on the production module path");
            assertFalse(moduleInfo.contains("requires org.apache.ratis"),
                    descriptor + " must not require Apache Ratis modules directly");
        }
    }

    @Test
    void legacyRaftPackagesAreRemovedFromProductionModules() throws IOException {
        for (var descriptor : List.of(
                Path.of("src/library/java/module-info.java"),
                Path.of("src/fatjar/java/module-info.java"),
                Path.of("src/native/java/module-info.java"))) {
            var moduleInfo = Files.readString(descriptor);

            assertFalse(moduleInfo.contains("requires microraft;"),
                    descriptor + " must not require the retired MicroRaft spike dependency");
            assertFalse(moduleInfo.contains("it.cavallium.rockserver.core.cluster.raft"),
                    descriptor + " must not reference retired custom Raft packages");
            assertFalse(moduleInfo.contains("it.cavallium.rockserver.core.cluster.microraft"),
                    descriptor + " must not reference retired MicroRaft packages");
        }

        assertFalse(Files.readString(Path.of("pom.xml")).contains("io.microraft"),
                "pom.xml must not retain the retired MicroRaft spike dependency");
        assertFalse(Files.exists(Path.of("src/main/java/it/cavallium/rockserver/core/cluster/raft")),
                "custom Raft implementation package should not remain in main sources");
        assertFalse(Files.exists(Path.of("src/main/java/it/cavallium/rockserver/core/cluster/microraft")),
                "MicroRaft spike package should not remain in main sources");
        assertNoRetiredCustomRaftRpc(Path.of("src/main/proto/rocksdb.proto"));
        assertNoRetiredCustomRaftRpc(Path.of("rust-client/proto/rocksdb.proto"));
    }

    @Test
    void obsoleteRawMutationWriteFacadeIsNotAProductionSurface() throws IOException {
        var offendingSources = new ArrayList<Path>();
        for (var root : List.of(
                Path.of("src/main/java"),
                Path.of("src/library/java"),
                Path.of("src/fatjar/java"),
                Path.of("src/native/java"))) {
            if (!Files.exists(root)) {
                continue;
            }
            try (var files = Files.walk(root)) {
                for (var source : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                    if (Files.readString(source).contains("ReplicatedWriteSyncAPI")) {
                        offendingSources.add(source);
                    }
                }
            }
        }

        assertEquals(List.of(), offendingSources,
                "The retired raw-mutation ReplicatedWriteSyncAPI facade must remain test-only");
    }

    @Test
    void apacheRatisArtifactsStayOutOfProjectDependenciesAndInsideInactiveSpikeCopy() throws Exception {
        var document = readPom();
        var ratisSpikeProfile = directProfile(document, "ratis-spike");
        assertFalse(profileActiveByDefault(ratisSpikeProfile),
                "ratis-spike must stay opt-in and inactive by default");
        assertTrue(directChildren(ratisSpikeProfile, "dependencies").isEmpty(),
                "ratis-spike must not add Ratis artifacts to the project dependency graph");

        var projectRatisDependencies = new ArrayList<String>();
        walk(document.getDocumentElement(), element -> {
            if ("dependency".equals(element.getTagName())) {
                if ("org.apache.ratis".equals(childText(element, "groupId"))) {
                    projectRatisDependencies.add(childText(element, "artifactId"));
                }
            }
        });
        assertEquals(List.of(), projectRatisDependencies,
                "Apache Ratis must not be a Maven project dependency");

        var copyExecution = executionById(ratisSpikeProfile, "copy-ratis-adapter-isolated-dependencies");
        var artifactItems = directChild(directChild(copyExecution, "configuration"), "artifactItems");
        var copiedRatisArtifacts = new ArrayList<String>();
        var copiedNoticeArtifacts = new ArrayList<String>();

        for (var artifactItem : directChildren(artifactItems, "artifactItem")) {
            var groupId = childText(artifactItem, "groupId");
            var artifactId = childText(artifactItem, "artifactId");
            if (noticePreservedAdapterArtifact(artifactItem)) {
                copiedNoticeArtifacts.add(coordinate(artifactItem));
                if ("io.jstach.rainbowgum".equals(groupId)) {
                    assertEquals("${rainbowgum.version}", childText(artifactItem, "version"),
                            "RainbowGum artifact " + artifactId + " must use the central version pin");
                }
            }
            if (!"org.apache.ratis".equals(groupId)) {
                continue;
            }

            copiedRatisArtifacts.add(artifactId);
            var expectedVersion = "ratis-thirdparty-misc".equals(artifactId)
                    ? "${ratis.thirdparty.version}"
                    : "${ratis.version}";
            assertEquals(expectedVersion, childText(artifactItem, "version"),
                    "Ratis artifact " + artifactId + " must use the central version pin");
            assertTrue(isInsideProfile(artifactItem, "ratis-spike"),
                    "Ratis artifact " + artifactId + " must stay inside the inactive ratis-spike profile");
        }

        assertEquals(List.of(
                "ratis-server",
                "ratis-thirdparty-misc",
                "ratis-proto",
                "ratis-common",
                "ratis-client",
                "ratis-server-api",
                "ratis-metrics-api",
                "ratis-grpc",
                "ratis-metrics-default"), copiedRatisArtifacts);
        assertEquals(List.of(
                "org.apache.ratis:ratis-server",
                "org.apache.ratis:ratis-thirdparty-misc",
                "org.apache.ratis:ratis-proto",
                "org.apache.ratis:ratis-common",
                "org.apache.ratis:ratis-client",
                "org.apache.ratis:ratis-server-api",
                "org.apache.ratis:ratis-metrics-api",
                "org.apache.ratis:ratis-grpc",
                "org.apache.ratis:ratis-metrics-default",
                "io.jstach.rainbowgum:rainbowgum-core",
                "io.jstach.rainbowgum:rainbowgum-annotation",
                "io.jstach.rainbowgum:rainbowgum-pattern",
                "io.jstach.rainbowgum:rainbowgum-slf4j"), copiedNoticeArtifacts);

        var noticeExecution = executionById(ratisSpikeProfile, "unpack-ratis-adapter-notice-metadata");
        var noticeConfiguration = directChild(noticeExecution, "configuration");
        assertEquals("${project.build.directory}/ratis-adapter-notice-markers",
                childText(noticeConfiguration, "markersDirectory"),
                "Classpath adapter notice extraction must not share dependency-plugin markers with other profiles");
        var noticeArtifactItems = directChild(noticeConfiguration, "artifactItems");
        var noticeArtifacts = new ArrayList<String>();
        for (var artifactItem : directChildren(noticeArtifactItems, "artifactItem")) {
            if (!noticePreservedAdapterArtifact(artifactItem)) {
                continue;
            }

            var artifactId = childText(artifactItem, "artifactId");
            noticeArtifacts.add(coordinate(artifactItem));
            assertEquals("${ratis.adapter.notice.includes}", childText(artifactItem, "includes"),
                    "Adapter artifact " + artifactId + " must unpack the shared notice metadata set");
            assertTrue(childText(artifactItem, "outputDirectory")
                            .contains("${project.build.directory}/ratis-adapter-distribution/notices/" + artifactId),
                    "Adapter artifact " + artifactId + " notice metadata must stay inside the adapter distribution");
        }
        assertEquals(copiedNoticeArtifacts, noticeArtifacts,
                "Every copied Ratis/RainbowGum adapter artifact must have matching notice metadata unpacking");
        assertRainbowGumParentNoticeMetadataCopy(ratisSpikeProfile,
                "copy-ratis-adapter-rainbowgum-parent-notice-metadata",
                "${project.build.directory}/ratis-adapter-distribution/notices/");
    }

    @Test
    void grpcNettyUnixCommonMediationIsExplicit() throws Exception {
        var document = readPom();
        var grpcNetty = directDependency(document, "io.grpc", "grpc-netty");
        var unixCommon = directDependency(document, "io.netty", "netty-transport-native-unix-common");

        assertTrue(hasExclusion(grpcNetty, "io.netty", "netty-transport-native-unix-common"),
                "grpc-netty must exclude its older netty-transport-native-unix-common edge");
        assertEquals("${netty.version}", childText(unixCommon, "version"));
    }

    @Test
    void productionCoreDoesNotImportRawApacheRatisPackages() throws IOException {
        var offendingImports = new ArrayList<String>();
        for (var root : List.of(
                Path.of("src/main/java"),
                Path.of("src/library/java"),
                Path.of("src/fatjar/java"),
                Path.of("src/native/java"))) {
            if (!Files.exists(root)) {
                continue;
            }
            try (var files = Files.walk(root)) {
                for (var source : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                    var lines = Files.readAllLines(source);
                    for (int i = 0; i < lines.size(); i++) {
                        if (lines.get(i).trim().startsWith("import org.apache.ratis.")) {
                            offendingImports.add(source + ":" + (i + 1));
                        }
                    }
                }
            }
        }

        assertEquals(List.of(), offendingImports,
                "Production core sources must not import raw org.apache.ratis.*; only the shaded/classpath adapter may do that");
    }

    @Test
    void shadedRatisAdapterProfileRelocatesRatisAndPreservesResources() throws Exception {
        var document = readPom();
        var shadedProfile = directProfile(document, "ratis-shaded-adapter");
        assertFalse(profileActiveByDefault(shadedProfile),
                "ratis-shaded-adapter must stay opt-in and inactive by default");
        assertTrue(directChildren(shadedProfile, "dependencies").isEmpty(),
                "ratis-shaded-adapter must not add Ratis artifacts as project dependencies");
        var jarManifest = directChild(directChild(directChild(pluginByArtifactId(shadedProfile, "maven-jar-plugin"),
                "configuration"), "archive"), "manifest");
        assertEquals("false", childText(jarManifest, "addClasspath"),
                "The project jar used as shaded input must not contribute external Class-Path entries");
        assertEquals(List.of(
                        "org.ow2.asm:asm:${shade.asm.version}",
                        "org.ow2.asm:asm-commons:${shade.asm.version}",
                        "org.ow2.asm:asm-tree:${shade.asm.version}"),
                pluginDependencyCoordinates(pluginByArtifactId(shadedProfile, "maven-shade-plugin")),
                "The shaded adapter relocates Java 25 classes, so Shade must use a Java 25-capable ASM override");

        var shadeExecution = executionById(shadedProfile, "shade-ratis-adapter-artifact");
        var shadeConfiguration = directChild(shadeExecution, "configuration");
        assertEquals("${project.build.directory}/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar",
                childText(shadeConfiguration, "outputFile"));
        assertEquals("false", childText(shadeConfiguration, "createDependencyReducedPom"));

        assertEquals(List.of("${project.build.directory}/ratis-shaded-adapter-staging/adapter/rockserver-ratis-adapter-classes.jar"),
                childTexts(directChild(shadeConfiguration, "extraJars"), "extraJar"));
        assertEquals(List.of(
                        "org.apache.ratis:ratis-server:${ratis.version}",
                        "org.apache.ratis:ratis-thirdparty-misc:${ratis.thirdparty.version}",
                        "org.apache.ratis:ratis-proto:${ratis.version}",
                        "org.apache.ratis:ratis-common:${ratis.version}",
                        "org.apache.ratis:ratis-client:${ratis.version}",
                        "org.apache.ratis:ratis-server-api:${ratis.version}",
                        "org.apache.ratis:ratis-metrics-api:${ratis.version}",
                        "org.apache.ratis:ratis-grpc:${ratis.version}",
                        "org.apache.ratis:ratis-metrics-default:${ratis.version}",
                        "io.jstach.rainbowgum:rainbowgum-core:${rainbowgum.version}",
                        "io.jstach.rainbowgum:rainbowgum-annotation:${rainbowgum.version}",
                        "io.jstach.rainbowgum:rainbowgum-pattern:${rainbowgum.version}",
                        "io.jstach.rainbowgum:rainbowgum-slf4j:${rainbowgum.version}"),
                childTexts(directChild(shadeConfiguration, "extraArtifacts"), "extraArtifact"));

        var relocation = directChild(directChild(shadeConfiguration, "relocations"), "relocation");
        assertEquals("org.apache.ratis", childText(relocation, "pattern"));
        assertEquals("it.cavallium.rockserver.shaded.ratis.org.apache.ratis",
                childText(relocation, "shadedPattern"));

        var excludes = childTexts(directChild(directChild(directChild(shadeConfiguration, "filters"), "filter"), "excludes"),
                "exclude");
        assertTrue(excludes.contains("META-INF/MANIFEST.MF"),
                "The shaded adapter must not inherit dependency manifests or Class-Path entries");
        assertTrue(excludes.contains("module-info.class"),
                "The shaded adapter must not expose dependency module descriptors");
        assertTrue(excludes.contains("META-INF/versions/*/module-info.class"),
                "The shaded adapter must not expose multi-release dependency module descriptors");
        assertTrue(excludes.contains("META-INF/*.SF"));
        assertTrue(excludes.contains("META-INF/*.DSA"));
        assertTrue(excludes.contains("META-INF/*.RSA"));

        var transformers = directChildren(directChild(shadeConfiguration, "transformers"), "transformer");
        var transformerImplementations = transformers.stream()
                .map(transformer -> transformer.getAttribute("implementation"))
                .toList();
        assertTrue(transformerImplementations.contains(
                "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"));
        assertTrue(transformerImplementations.contains(
                "org.apache.maven.plugins.shade.resource.ApacheLicenseResourceTransformer"));
        assertTrue(transformerImplementations.contains(
                "org.apache.maven.plugins.shade.resource.ApacheNoticeResourceTransformer"));

        var dependenciesTransformer = transformerByImplementation(transformers,
                "org.apache.maven.plugins.shade.resource.AppendingTransformer");
        assertEquals("META-INF/DEPENDENCIES", childText(dependenciesTransformer, "resource"));
        var manifestTransformer = transformerByImplementation(transformers,
                "org.apache.maven.plugins.shade.resource.ManifestResourceTransformer");
        assertEquals("it.cavallium.rockserver.ratis.AdapterMain", childText(manifestTransformer, "mainClass"));

        assertCopiedAdapterArtifacts(shadedProfile,
                "copy-ratis-shaded-adapter-isolated-dependencies",
                "${project.build.directory}/ratis-shaded-adapter-staging/lib");
        assertNoticeMetadataExtraction(shadedProfile,
                "unpack-ratis-shaded-adapter-notice-metadata",
                "${project.build.directory}/ratis-shaded-adapter-distribution/notices/",
                "${project.build.directory}/ratis-shaded-adapter-notice-markers");
        assertRainbowGumParentNoticeMetadataCopy(shadedProfile,
                "copy-ratis-shaded-adapter-rainbowgum-parent-notice-metadata",
                "${project.build.directory}/ratis-shaded-adapter-distribution/notices/");
        var shadedRehearsalExecution = executionById(shadedProfile, "run-ratis-shaded-adapter-process-rehearsal");
        var shadedRehearsalJava = directChild(
                directChild(directChild(shadedRehearsalExecution, "configuration"), "target"),
                "java");
        assertEquals("it.cavallium.rockserver.core.cluster.ratis.RatisClasspathProbe",
                shadedRehearsalJava.getAttribute("classname"));
        assertEquals("true", shadedRehearsalJava.getAttribute("fork"));
        assertEquals("true", shadedRehearsalJava.getAttribute("failonerror"));
        assertEquals(List.of(
                        "--external-process-only",
                        "--external-process-adapter-home=${project.build.directory}/ratis-shaded-adapter-distribution",
                        "--external-process-storage-root=${project.build.directory}/ratis-shaded-process-adapter-rehearsal"),
                directChildren(shadedRehearsalJava, "arg").stream()
                        .map(arg -> arg.getAttribute("value"))
                        .toList());
        var shadedRehearsalClasspath = directChild(shadedRehearsalJava, "classpath");
        assertEquals(List.of(
                        "${project.build.directory}/ratis-shaded-adapter-classes",
                        "${project.build.directory}/classes"),
                directChildren(shadedRehearsalClasspath, "pathelement").stream()
                        .map(pathelement -> pathelement.getAttribute("location"))
                        .toList());
        var shadedRehearsalFileset = directChild(shadedRehearsalClasspath, "fileset");
        assertEquals("${project.build.directory}/ratis-shaded-adapter-staging/lib",
                shadedRehearsalFileset.getAttribute("dir"));
        assertEquals("*.jar", shadedRehearsalFileset.getAttribute("includes"));
    }

    @Test
    void generatedShadedAdapterJarDoesNotExposeUnrelocatedRatisPackagesWhenPresent() throws IOException {
        var shadedAdapter = Path.of(
                "target/ratis-shaded-adapter-distribution/adapter/rockserver-ratis-adapter-shaded.jar");
        if (!Files.exists(shadedAdapter)) {
            return;
        }

        var unrelocatedRatisEntries = new ArrayList<String>();
        var relocatedRatisEntries = new ArrayList<String>();
        try (var jar = new JarFile(shadedAdapter.toFile())) {
            assertEquals("it.cavallium.rockserver.ratis.AdapterMain",
                    jar.getManifest().getMainAttributes().getValue("Main-Class"));
            assertNull(jar.getManifest().getMainAttributes().getValue("Class-Path"),
                    "The shaded adapter jar must be self-contained and not declare external classpath entries");
            requireNonEmptyJarEntry(jar, "LICENSE");
            requireNonEmptyJarEntry(jar, "META-INF/NOTICE");
            requireNonEmptyJarEntry(jar, "META-INF/DEPENDENCIES");
            requireNonEmptyJarEntry(jar, "META-INF/license/LICENSE.boringssl.txt");
            requireNonEmptyJarEntry(jar, "META-INF/license/LICENSE.tomcat-native.txt");
            for (var coordinate : expectedNoticeCoordinates()) {
                var separator = coordinate.indexOf(':');
                var groupId = coordinate.substring(0, separator);
                var artifactId = coordinate.substring(separator + 1);
                var metadataRoot = "META-INF/maven/" + groupId + "/" + artifactId + "/";
                requireNonEmptyJarEntry(jar, metadataRoot + "pom.xml");
                requireNonEmptyJarEntry(jar, metadataRoot + "pom.properties");
            }
            jar.stream().forEach(entry -> {
                var name = entry.getName();
                if (name.startsWith("org/apache/ratis/")
                        || name.startsWith("META-INF/services/org.apache.ratis")) {
                    unrelocatedRatisEntries.add(name);
                }
                if (name.startsWith("it/cavallium/rockserver/shaded/ratis/org/apache/ratis/")) {
                    relocatedRatisEntries.add(name);
                }
            });
        }

        assertEquals(List.of(), unrelocatedRatisEntries,
                "Generated shaded adapter jar must not expose unrelocated org.apache.ratis packages or services");
        assertFalse(relocatedRatisEntries.isEmpty(),
                "Generated shaded adapter jar must contain relocated Ratis classes");
    }

    @Test
    void generatedAdapterDistributionsContainNoticeMetadataWhenPresent() {
        assertGeneratedNoticeMetadata(Path.of("target/ratis-adapter-distribution/notices"));
        assertGeneratedNoticeMetadata(Path.of("target/ratis-shaded-adapter-distribution/notices"));
    }

    private static Document readPom() throws IOException, ParserConfigurationException, SAXException {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setExpandEntityReferences(false);
        factory.setIgnoringComments(true);
        factory.setNamespaceAware(false);
        factory.setXIncludeAware(false);
        return factory.newDocumentBuilder().parse(Path.of("pom.xml").toFile());
    }

    private static void assertNoRetiredCustomRaftRpc(Path protoPath) throws IOException {
        var proto = Files.readString(protoPath);
        for (var retiredRpc : List.of(
                "raftRequestVote",
                "raftAppendEntries",
                "raftPropose",
                "RaftVoteRpc",
                "RaftAppendRpc",
                "RaftProposeRpc")) {
            assertFalse(proto.contains(retiredRpc),
                    protoPath + " must not expose retired custom Raft RPCs or messages");
        }
    }

    private static Element directDependency(Document document, String groupId, String artifactId) {
        var dependencies = directChildren(document.getDocumentElement(), "dependencies");
        for (var dependenciesElement : dependencies) {
            for (var dependency : directChildren(dependenciesElement, "dependency")) {
                if (groupId.equals(childText(dependency, "groupId"))
                        && artifactId.equals(childText(dependency, "artifactId"))) {
                    return dependency;
                }
            }
        }
        throw new AssertionError("Missing direct dependency " + groupId + ":" + artifactId);
    }

    private static Element directProfile(Document document, String profileId) {
        var profiles = directChildren(document.getDocumentElement(), "profiles");
        for (var profilesElement : profiles) {
            for (var profile : directChildren(profilesElement, "profile")) {
                if (profileId.equals(childText(profile, "id"))) {
                    return profile;
                }
            }
        }
        throw new AssertionError("Missing profile " + profileId);
    }

    private static Element executionById(Element parent, String executionId) {
        var result = new ArrayList<Element>();
        walk(parent, element -> {
            if ("execution".equals(element.getTagName()) && executionId.equals(childText(element, "id"))) {
                result.add(element);
            }
        });
        assertEquals(1, result.size(), "Expected exactly one execution with id " + executionId);
        return result.getFirst();
    }

    private static boolean profileActiveByDefault(Element profile) {
        for (var activation : directChildren(profile, "activation")) {
            for (var activeByDefault : directChildren(activation, "activeByDefault")) {
                return Boolean.parseBoolean(activeByDefault.getTextContent().trim());
            }
        }
        return false;
    }

    private static boolean hasExclusion(Element dependency, String groupId, String artifactId) {
        for (var exclusions : directChildren(dependency, "exclusions")) {
            for (var exclusion : directChildren(exclusions, "exclusion")) {
                if (groupId.equals(childText(exclusion, "groupId"))
                        && artifactId.equals(childText(exclusion, "artifactId"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean noticePreservedAdapterArtifact(Element artifactItem) {
        var groupId = childText(artifactItem, "groupId");
        return "org.apache.ratis".equals(groupId) || "io.jstach.rainbowgum".equals(groupId);
    }

    private static String coordinate(Element artifactItem) {
        return childText(artifactItem, "groupId") + ":" + childText(artifactItem, "artifactId");
    }

    private static void assertCopiedAdapterArtifacts(Element profile,
                                                     String executionId,
                                                     String expectedOutputDirectory) {
        var copyExecution = executionById(profile, executionId);
        var configuration = directChild(copyExecution, "configuration");
        assertEquals(expectedOutputDirectory, childText(configuration, "outputDirectory"));
        var artifactItems = directChild(configuration, "artifactItems");
        var copiedArtifacts = new ArrayList<String>();

        for (var artifactItem : directChildren(artifactItems, "artifactItem")) {
            copiedArtifacts.add(coordinate(artifactItem));
            var groupId = childText(artifactItem, "groupId");
            var artifactId = childText(artifactItem, "artifactId");
            if ("org.apache.ratis".equals(groupId)) {
                var expectedVersion = "ratis-thirdparty-misc".equals(artifactId)
                        ? "${ratis.thirdparty.version}"
                        : "${ratis.version}";
                assertEquals(expectedVersion, childText(artifactItem, "version"),
                        "Ratis artifact " + artifactId + " must use the central version pin");
            } else if ("io.jstach.rainbowgum".equals(groupId)) {
                assertEquals("${rainbowgum.version}", childText(artifactItem, "version"),
                        "RainbowGum artifact " + artifactId + " must use the central version pin");
            }
            assertTrue(isInsideProfile(artifactItem, childText(profile, "id")),
                    "Adapter artifact " + artifactId + " must stay inside profile " + childText(profile, "id"));
        }

        assertEquals(expectedNoticeCoordinates(), copiedArtifacts);
    }

    private static void assertNoticeMetadataExtraction(Element profile,
                                                       String executionId,
                                                       String expectedOutputDirectoryPrefix,
                                                       String expectedMarkersDirectory) {
        var noticeExecution = executionById(profile, executionId);
        var noticeConfiguration = directChild(noticeExecution, "configuration");
        assertEquals(expectedMarkersDirectory, childText(noticeConfiguration, "markersDirectory"),
                "Adapter notice extraction must use profile-specific dependency-plugin markers");
        var noticeArtifactItems = directChild(noticeConfiguration, "artifactItems");
        var noticeArtifacts = new ArrayList<String>();
        for (var artifactItem : directChildren(noticeArtifactItems, "artifactItem")) {
            if (!noticePreservedAdapterArtifact(artifactItem)) {
                continue;
            }

            var artifactId = childText(artifactItem, "artifactId");
            noticeArtifacts.add(coordinate(artifactItem));
            assertEquals("${ratis.adapter.notice.includes}", childText(artifactItem, "includes"),
                    "Adapter artifact " + artifactId + " must unpack the shared notice metadata set");
            assertTrue(childText(artifactItem, "outputDirectory")
                            .contains(expectedOutputDirectoryPrefix + artifactId),
                    "Adapter artifact " + artifactId + " notice metadata must stay inside the adapter distribution");
        }
        assertEquals(expectedNoticeCoordinates(), noticeArtifacts);
    }

    private static void assertRainbowGumParentNoticeMetadataCopy(Element profile,
                                                                 String executionId,
                                                                 String expectedOutputDirectoryPrefix) {
        var copyExecution = executionById(profile, executionId);
        assertEquals(List.of("copy"), childTexts(directChild(copyExecution, "goals"), "goal"));
        var copyConfiguration = directChild(copyExecution, "configuration");
        var artifactItems = directChildren(directChild(copyConfiguration, "artifactItems"), "artifactItem");
        assertEquals(1, artifactItems.size());
        var artifactItem = artifactItems.getFirst();
        assertEquals("io.jstach.rainbowgum", childText(artifactItem, "groupId"));
        assertEquals("rainbowgum-maven-parent", childText(artifactItem, "artifactId"));
        assertEquals("${rainbowgum.version}", childText(artifactItem, "version"));
        assertEquals("pom", childText(artifactItem, "type"));
        assertEquals(expectedOutputDirectoryPrefix
                        + "rainbowgum-maven-parent/META-INF/maven/io.jstach.rainbowgum/rainbowgum-maven-parent",
                childText(artifactItem, "outputDirectory"));
        assertEquals("pom.xml", childText(artifactItem, "destFileName"));
        assertTrue(isInsideProfile(artifactItem, childText(profile, "id")),
                "RainbowGum parent notice metadata copy must stay inside profile " + childText(profile, "id"));
    }

    private static List<String> expectedNoticeCoordinates() {
        return List.of(
                "org.apache.ratis:ratis-server",
                "org.apache.ratis:ratis-thirdparty-misc",
                "org.apache.ratis:ratis-proto",
                "org.apache.ratis:ratis-common",
                "org.apache.ratis:ratis-client",
                "org.apache.ratis:ratis-server-api",
                "org.apache.ratis:ratis-metrics-api",
                "org.apache.ratis:ratis-grpc",
                "org.apache.ratis:ratis-metrics-default",
                "io.jstach.rainbowgum:rainbowgum-core",
                "io.jstach.rainbowgum:rainbowgum-annotation",
                "io.jstach.rainbowgum:rainbowgum-pattern",
                "io.jstach.rainbowgum:rainbowgum-slf4j");
    }

    private static List<String> expectedNoticeArtifactIds() {
        return expectedNoticeCoordinates().stream()
                .map(coordinate -> coordinate.substring(coordinate.indexOf(':') + 1))
                .toList();
    }

    private static void assertGeneratedNoticeMetadata(Path noticesRoot) {
        if (!Files.exists(noticesRoot)) {
            return;
        }

        var missing = new ArrayList<String>();
        for (var artifactId : expectedNoticeArtifactIds()) {
            var artifactRoot = noticesRoot.resolve(artifactId);
            if (!Files.isDirectory(artifactRoot)) {
                missing.add(artifactRoot.toString());
                continue;
            }

            if (artifactId.startsWith("ratis-")) {
                requireNoticeFile(missing, artifactRoot, "META-INF/maven/org.apache.ratis/"
                        + artifactId + "/pom.xml");
                requireNoticeFile(missing, artifactRoot, "META-INF/maven/org.apache.ratis/"
                        + artifactId + "/pom.properties");
                requireNoticeFile(missing, artifactRoot, "META-INF/LICENSE");
                requireNoticeFile(missing, artifactRoot, "META-INF/DEPENDENCIES");
                if ("ratis-thirdparty-misc".equals(artifactId)) {
                    requireNoticeFile(missing, artifactRoot, "META-INF/LICENSE.txt");
                    requireNoticeFile(missing, artifactRoot, "META-INF/license/LICENSE.boringssl.txt");
                    requireNoticeFile(missing, artifactRoot, "META-INF/license/LICENSE.tomcat-native.txt");
                } else {
                    requireNoticeFile(missing, artifactRoot, "META-INF/NOTICE");
                }
            } else if (artifactId.startsWith("rainbowgum-")) {
                requireNoticeFile(missing, artifactRoot, "META-INF/maven/io.jstach.rainbowgum/"
                        + artifactId + "/pom.xml");
                requireNoticeFile(missing, artifactRoot, "META-INF/maven/io.jstach.rainbowgum/"
                        + artifactId + "/pom.properties");
            }
        }
        requireNoticeFileContaining(missing, noticesRoot.resolve("rainbowgum-maven-parent"),
                "META-INF/maven/io.jstach.rainbowgum/rainbowgum-maven-parent/pom.xml",
                "<name>The BSD 3-Clause License</name>");

        assertEquals(List.of(), missing,
                "Generated adapter distribution notice metadata is incomplete under " + noticesRoot);
    }

    private static void requireNoticeFile(List<String> missing, Path artifactRoot, String relativePath) {
        var file = artifactRoot.resolve(relativePath);
        try {
            if (!Files.isRegularFile(file) || Files.size(file) == 0L) {
                missing.add(file.toString());
            }
        } catch (IOException ex) {
            missing.add(file.toString());
        }
    }

    private static void requireNoticeFileContaining(List<String> missing,
                                                    Path artifactRoot,
                                                    String relativePath,
                                                    String expectedText) {
        requireNoticeFile(missing, artifactRoot, relativePath);
        var file = artifactRoot.resolve(relativePath);
        try {
            if (Files.isRegularFile(file) && !Files.readString(file).contains(expectedText)) {
                missing.add(file + " missing text " + expectedText);
            }
        } catch (IOException ex) {
            missing.add(file.toString());
        }
    }

    private static void requireNonEmptyJarEntry(JarFile jar, String name) throws IOException {
        var entry = jar.getJarEntry(name);
        assertTrue(entry != null && !entry.isDirectory(), "Generated shaded adapter jar is missing " + name);
        try (var input = jar.getInputStream(entry)) {
            assertTrue(input.read() >= 0, "Generated shaded adapter jar entry must not be empty: " + name);
        }
    }

    private static Element transformerByImplementation(List<Element> transformers, String implementation) {
        var result = transformers.stream()
                .filter(transformer -> implementation.equals(transformer.getAttribute("implementation")))
                .toList();
        assertEquals(1, result.size(), "Expected exactly one transformer " + implementation);
        return result.getFirst();
    }

    private static Element pluginByArtifactId(Element profile, String artifactId) {
        var plugins = directChild(directChild(profile, "build"), "plugins");
        var result = new ArrayList<Element>();
        for (var plugin : directChildren(plugins, "plugin")) {
            if (artifactId.equals(childText(plugin, "artifactId"))) {
                result.add(plugin);
            }
        }
        assertEquals(1, result.size(), "Expected exactly one plugin " + artifactId);
        return result.getFirst();
    }

    private static List<String> pluginDependencyCoordinates(Element plugin) {
        var dependencies = directChildren(plugin, "dependencies");
        if (dependencies.isEmpty()) {
            return List.of();
        }
        assertEquals(1, dependencies.size(), "Expected at most one plugin dependencies element");
        return directChildren(dependencies.getFirst(), "dependency").stream()
                .map(RatisPackagingBoundaryTest::coordinateWithVersion)
                .toList();
    }

    private static String coordinateWithVersion(Element dependency) {
        return childText(dependency, "groupId") + ":"
                + childText(dependency, "artifactId") + ":"
                + childText(dependency, "version");
    }

    private static boolean isInsideProfile(Element element, String profileId) {
        for (var parent = element.getParentNode(); parent != null; parent = parent.getParentNode()) {
            if (parent instanceof Element parentElement && "profile".equals(parentElement.getTagName())) {
                return profileId.equals(childText(parentElement, "id"));
            }
        }
        return false;
    }

    private static void walk(Element element, ElementVisitor visitor) {
        visitor.visit(element);
        for (var child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element childElement) {
                walk(childElement, visitor);
            }
        }
    }

    private static List<Element> directChildren(Element element, String tagName) {
        var result = new ArrayList<Element>();
        for (var child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element childElement && tagName.equals(childElement.getTagName())) {
                result.add(childElement);
            }
        }
        return result;
    }

    private static Element directChild(Element element, String tagName) {
        var children = directChildren(element, tagName);
        assertEquals(1, children.size(), "Expected exactly one child element named " + tagName);
        return children.getFirst();
    }

    private static String childText(Element element, String tagName) {
        for (var child : directChildren(element, tagName)) {
            return child.getTextContent().trim();
        }
        return "";
    }

    private static List<String> childTexts(Element element, String tagName) {
        return directChildren(element, tagName).stream()
                .map(child -> child.getTextContent().trim())
                .toList();
    }

    @FunctionalInterface
    private interface ElementVisitor {

        void visit(Element element);
    }
}
