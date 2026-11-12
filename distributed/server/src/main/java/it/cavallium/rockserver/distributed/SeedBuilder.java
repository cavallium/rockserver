package it.cavallium.rockserver.distributed;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.jetbrains.annotations.Nullable;

/**
 * {@code build-seed}: builds the database that seeds a cluster from chosen columns of a stopped single-node
 * Rockserver database, without reading their contents. The single node's column volumes and WAL may be anywhere
 * (its own config says where); the seed is created as a node opens a copy, with the cluster's config: volumes inside
 * the seed, a {@code wal-path} outside it as {@link NodeStore#DETACHED_WAL}. Each column is exported with RocksDB's
 * column family export (its table files hard-linked into an export directory next to its volume) and created in the
 * seed with the source's schema by a column family import, which links the files into the seed's volume. Between
 * file systems, the files are copied instead.
 *
 * <p>The source is opened read-write, as its Rockserver opens it but without automatic compactions: the open replays
 * its WAL, and the export flushes its memtables. Nothing else of it changes; its table files are never written.</p>
 *
 * <p>The seed holds no replication metadata ({@link NodeStore#META_COLUMN}), like an {@code export-seed} copy: a
 * first start records its digest and writes the cluster's genesis. Column ids in the seed are RocksDB's, assigned
 * anew: clients get them by name.</p>
 */
final class SeedBuilder {

	private SeedBuilder() {}

	/** {@code columns} empty: every user column of the source, minus {@code excluded}. */
	record Config(Path sourceDb, @Nullable Path sourceConfig, Path target, @Nullable Path rockserverConfig,
			List<String> columns, List<String> excluded, @Nullable Path exportDir) {}

	/** Wrong options: exit 2. */
	static final class UsageException extends RuntimeException {
		UsageException(String message) {
			super(message);
		}
	}

	/** The seed was not built, and nothing of it is left: exit 1. */
	static final class BuildException extends IOException {
		BuildException(String message) {
			super(message);
		}

		BuildException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	static void run(Config options, PrintStream out) throws IOException {
		if (!options.columns().isEmpty() && !options.excluded().isEmpty()) {
			throw new UsageException("--column and --exclude-column cannot be combined");
		}
		Path sourceDb = options.sourceDb().toAbsolutePath().normalize();
		Path target = options.target().toAbsolutePath().normalize();
		if (target.getFileName() == null || target.getParent() == null) {
			throw new UsageException("invalid --target " + options.target());
		}
		Path building = target.resolveSibling(target.getFileName() + ".tmp");
		if (!Files.isRegularFile(sourceDb.resolve("CURRENT"))) {
			throw new BuildException("No database in " + sourceDb);
		}
		// NOFOLLOW_LINKS: a symbolic link to nothing is refused too, as export-seed refuses it.
		if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
			throw new BuildException("The target " + target + " already exists");
		}
		if (Files.exists(building, LinkOption.NOFOLLOW_LINKS)) {
			throw new BuildException("The target's temporary directory " + building + " already exists: remove it or"
					+ " choose another target");
		}
		Path realSource = NodeStore.realPathOf(sourceDb);
		for (Path written : List.of(target, building)) {
			Path real = NodeStore.realPathOf(written);
			if (real.startsWith(realSource) || realSource.startsWith(real)) {
				throw new BuildException("The target " + target + " is, contains or is inside the source database "
						+ sourceDb);
			}
		}
		Path exportDir = options.exportDir() == null ? null : options.exportDir().toAbsolutePath().normalize();
		if (exportDir != null && !Files.isDirectory(exportDir)) {
			throw new BuildException("The export directory " + exportDir + " does not exist");
		}
		Files.createDirectories(target.getParent());

		// The seed's config first: when it is refused (a volume outside the database), nothing has been written yet.
		Path targetConfig = NodeStore.replicaConfig(options.rockserverConfig(), target, target, "build-seed",
				NodeStore::withSmallFootprint);
		Path sourceConfig = target.resolveSibling("rockserver-build-seed-source-for-" + target.getFileName() + ".conf");
		var exportDirs = new ArrayList<Path>();
		boolean built = false;
		try {
			writeSourceConfig(options.sourceConfig(), sourceConfig);
			try (var source = openSource(sourceDb, sourceConfig)) {
				Files.deleteIfExists(sourceConfig);
				var columns = select(source.getEmbeddedDB(), options);
				try (var seed = new EmbeddedConnection(building, "build-seed", targetConfig)) {
					Files.deleteIfExists(targetConfig);
					copy(source.getEmbeddedDB(), seed.getEmbeddedDB(), columns, exportDir, building, exportDirs, out);
					seed.getEmbeddedDB().flush();
				}
			}
			NodeStore.syncTree(building);
			Files.move(building, target, StandardCopyOption.ATOMIC_MOVE);
			NodeStore.syncDirectory(target.getParent());
			built = true;
		} finally {
			Files.deleteIfExists(sourceConfig);
			Files.deleteIfExists(targetConfig);
			for (Path dir : exportDirs) {
				deleteQuietly(dir);
			}
			if (!built) {
				deleteQuietly(building);
			}
		}
		out.println("OK");
	}

	private static void deleteQuietly(Path dir) {
		try {
			NodeStore.deleteRecursively(dir);
		} catch (IOException | RuntimeException ignored) {
			// The failure that led here is the one to report.
		}
	}

	/**
	 * The source's config (the defaults without one), resolved, without automatic compactions: the open then only
	 * replays the WAL and flushes. Relative paths in it resolve against the database directory, as in the original.
	 */
	private static void writeSourceConfig(@Nullable Path sourceConfig, Path file) throws IOException {
		try {
			var config = sourceConfig == null ? ConfigFactory.empty()
					: ConfigFactory.parseFile(sourceConfig.toFile(), ConfigParseOptions.defaults().setAllowMissing(false));
			config = NodeStore.withoutAutoCompactions(config.resolve());
			NodeStore.writeOwnerOnly(file, config.root().render(ConfigRenderOptions.concise()));
		} catch (ConfigException | IllegalArgumentException e) {
			throw new BuildException("Failed to read the source's Rockserver config " + sourceConfig + ": " + e.getMessage(),
					e);
		}
	}

	private static EmbeddedConnection openSource(Path sourceDb, Path config) throws IOException {
		try {
			return new EmbeddedConnection(sourceDb, "build-seed-source", config);
		} catch (IOException | RuntimeException e) {
			for (Throwable cause = e; cause != null; cause = cause.getCause()) {
				String message = String.valueOf(cause.getMessage());
				if (message.contains("LOCK") || message.contains("lock hold") || message.contains("lock file")) {
					throw new BuildException("The source database " + sourceDb + " is in use (RocksDB holds its LOCK): stop"
							+ " the Rockserver that runs it", e);
				}
			}
			throw new BuildException("Failed to open the source database " + sourceDb + ": " + e, e);
		}
	}

	private static TreeMap<String, ColumnSchema> select(EmbeddedDB source, Config options) throws BuildException {
		var all = new TreeMap<>(source.getAllColumnDefinitions());
		for (String name : options.columns()) {
			if (name.equals(NodeStore.META_COLUMN)) {
				throw new BuildException("Column " + name + " holds a cluster's replication metadata and never goes into a"
						+ " seed");
			}
		}
		// A cluster node's database: its replication metadata stays out, as export-seed leaves it out.
		all.remove(NodeStore.META_COLUMN);
		var named = new ArrayList<>(options.columns());
		named.addAll(options.excluded());
		for (String name : named) {
			if (!all.containsKey(name)) {
				throw new BuildException("Unknown column " + name + ": the source has " + all.keySet());
			}
		}
		if (!options.columns().isEmpty()) {
			all.keySet().retainAll(new HashSet<>(options.columns()));
		}
		options.excluded().forEach(all::remove);
		if (all.isEmpty()) {
			throw new BuildException("No column to put into the seed");
		}
		return all;
	}

	private static void copy(EmbeddedDB source, EmbeddedDB seed, TreeMap<String, ColumnSchema> columns,
			@Nullable Path exportDir, Path building, List<Path> exportDirs, PrintStream out) throws IOException {
		for (var entry : columns.entrySet()) {
			ColumnSchema schema = entry.getValue();
			if (schema.mergeOperatorName() != null || schema.mergeOperatorClass() != null) {
				// The registry of uploaded operators is not carried into the seed, and a column whose schema names its
				// operator is not readable after a reopen of any Rockserver database (the JVM crashes in the merge).
				throw new BuildException("Column " + entry.getKey() + " has a merge operator in its schema ("
						+ (schema.mergeOperatorName() != null
								? schema.mergeOperatorName() + " version " + schema.mergeOperatorVersion()
								: schema.mergeOperatorClass())
						+ "), which build-seed does not carry into a seed: leave the column out (--exclude-column "
						+ entry.getKey() + "). A merge-operator-class of the config is supported");
			}
		}
		// Every memtable to table files, so that the files listed below are all of each column.
		source.flush();
		int number = 0;
		for (var entry : columns.entrySet()) {
			String name = entry.getKey();
			ColumnSchema schema = entry.getValue();
			long sourceId = source.getColumnId(name);
			List<Path> files = source.columnTableFiles(sourceId);
			long seedId;
			long bytes = 0;
			String how;
			if (files.isEmpty()) {
				// RocksDB imports no empty file list.
				seedId = seed.createColumn(name, schema);
				how = "empty";
			} else {
				var sourceFiles = new HashSet<>();
				for (Path file : files) {
					bytes += Files.size(file);
					sourceFiles.add(Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class).fileKey());
				}
				Path volume = Objects.requireNonNull(files.getFirst().getParent());
				Path exportParent;
				if (exportDir != null) {
					exportParent = exportDir;
				} else {
					// Next to the column's volume: on its file system, unless the volume is a mount point.
					exportParent = volume.resolveSibling(volume.getFileName() + ".build-seed-export");
					if (!Files.exists(exportParent, LinkOption.NOFOLLOW_LINKS)) {
						exportDirs.add(exportParent);
					}
					Files.createDirectories(exportParent);
				}
				Path export = exportParent.resolve("build-seed-" + ProcessHandle.current().pid() + "-" + number++);
				if (Files.exists(export, LinkOption.NOFOLLOW_LINKS)) {
					throw new BuildException("The export directory " + export + " already exists: remove it");
				}
				for (Path file : files) {
					if (!sameFileSystem(file.getParent(), exportParent)) {
						out.println("column " + name + ": the export directory " + exportParent + " is on another file system"
								+ " than " + file.getParent() + ": copying " + bytes + " bytes (pass --export-dir on the"
								+ " volume's file system to link them)");
						break;
					}
				}
				if (!sameFileSystem(exportParent, building)) {
					out.println("column " + name + ": the target is on another file system than the export directory "
							+ exportParent + ": copying " + bytes + " bytes into the seed");
				}
				exportDirs.add(export);
				try (var exported = source.exportColumn(sourceId, export)) {
					if (exported.files() != files.size()) {
						throw new BuildException("Column " + name + " changed while it was exported");
					}
					seedId = seed.createColumnFromExport(name, schema, exported);
				} catch (BuildException e) {
					throw e;
				} catch (RuntimeException e) {
					throw new BuildException("Column " + name + " could not be exported and imported: " + rootMessage(e), e);
				}
				NodeStore.deleteRecursively(export);
				boolean linked = true;
				List<Path> seedFiles = seed.columnTableFiles(seedId);
				for (Path file : seedFiles) {
					linked &= sourceFiles.contains(
							Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class).fileKey());
				}
				if (seedFiles.size() != files.size()) {
					throw new BuildException("Column " + name + " has " + files.size() + " table files in the source and "
							+ seedFiles.size() + " in the seed");
				}
				how = linked ? "linked" : "copied";
			}
			if (!schema.equals(seed.getAllColumnDefinitions().get(name))) {
				throw new BuildException("Column " + name + " has another schema in the seed");
			}
			String sourceOperator = source.columnMergeOperatorClass(sourceId);
			String seedOperator = seed.columnMergeOperatorClass(seedId);
			if (!Objects.equals(sourceOperator, seedOperator)) {
				throw new BuildException("Column " + name + " is merged by " + describe(sourceOperator) + " in the source and"
						+ " by " + describe(seedOperator) + " with the cluster's config: give the column the same"
						+ " merge-operator-class in --rockserver-config, or leave the column out");
			}
			out.println("column " + name + ": " + files.size() + " files, " + bytes + " bytes, " + how);
		}
	}

	private static String describe(@Nullable String operator) {
		return operator == null ? "no merge operator" : operator;
	}

	private static String rootMessage(Throwable e) {
		Throwable root = e;
		Set<Throwable> seen = new HashSet<>();
		while (root.getCause() != null && seen.add(root)) {
			root = root.getCause();
		}
		return String.valueOf(root.getMessage());
	}

	private static boolean sameFileSystem(Path a, Path b) throws IOException {
		try {
			return Objects.equals(Files.getAttribute(a, "unix:dev"), Files.getAttribute(b, "unix:dev"));
		} catch (UnsupportedOperationException | IllegalArgumentException e) {
			return Files.getFileStore(a).equals(Files.getFileStore(b));
		}
	}
}
