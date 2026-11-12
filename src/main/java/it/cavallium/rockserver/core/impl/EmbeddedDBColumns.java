package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetAllColumnDefinitions;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumnIfExists;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.EstimateNumKeys;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.RocksDB;
import it.cavallium.rockserver.core.impl.EmbeddedDB.*;

/**
 * Column and schema management of {@link EmbeddedDB}: schema encoding, merge-operator resolution, column
 * registration, create/delete column and the column metadata reads.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBColumns {

	private final EmbeddedDB edb;

	EmbeddedDBColumns(EmbeddedDB edb) {
		this.edb = edb;
	}

	private ColumnSchema decodeColumnSchema(byte[] value) {
		try (var is = new ByteArrayInputStream(value); var dis = new DataInputStream(is)) {
			var check = dis.readByte();
			if (check == 2) {
				var size = dis.readInt();
				var keys = new IntArrayList(size);
				for (int i = 0; i < size; i++) {
					keys.add(dis.readInt());
				}
				size = dis.readInt();
				var colHashTypes = new ObjectArrayList<ColumnHashType>(size);
				for (int i = 0; i < size; i++) {
					colHashTypes.add(ColumnHashType.values()[dis.readUnsignedByte()]);
				}
				var hasValue = dis.readBoolean();
				return new ColumnSchema(keys, colHashTypes, hasValue, null, null, null);
			} else if (check == 3) {
				var size = dis.readInt();
				var keys = new IntArrayList(size);
				for (int i = 0; i < size; i++) {
					keys.add(dis.readInt());
				}
				size = dis.readInt();
				var colHashTypes = new ObjectArrayList<ColumnHashType>(size);
				for (int i = 0; i < size; i++) {
					colHashTypes.add(ColumnHashType.values()[dis.readUnsignedByte()]);
				}
				var hasValue = dis.readBoolean();
				String mergeOperatorName = dis.readBoolean() ? dis.readUTF() : null;
				Long mergeOperatorVersion = dis.readBoolean() ? dis.readLong() : null;
				String mergeOperatorClass = dis.readBoolean() ? dis.readUTF() : null;
				return new ColumnSchema(keys,
						colHashTypes,
						hasValue,
						mergeOperatorName,
						mergeOperatorVersion,
						mergeOperatorClass
				);
			} else {
				throw new IllegalStateException("Unknown schema version: " + check);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private byte[] encodeColumnSchema(ColumnSchema schema) {
		try (var baos = new ByteArrayOutputStream(); var daos = new DataOutputStream(baos)) {
			daos.writeByte(3);
			daos.writeInt(schema.keys().size());
			for (int key : schema.keys()) {
				daos.writeInt(key);
			}
			daos.writeInt(schema.variableTailKeys().size());
			for (ColumnHashType variableTailKey : schema.variableTailKeys()) {
				daos.writeByte(variableTailKey.ordinal());
			}
			daos.writeBoolean(schema.hasValue());
			var mergeOperatorName = schema.mergeOperatorName();
			var mergeOperatorVersion = schema.mergeOperatorVersion();
			var mergeOperatorClass = schema.mergeOperatorClass();
			daos.writeBoolean(mergeOperatorName != null);
			if (mergeOperatorName != null) {
				daos.writeUTF(mergeOperatorName);
			}
			daos.writeBoolean(mergeOperatorVersion != null);
			if (mergeOperatorVersion != null) {
				daos.writeLong(mergeOperatorVersion);
			}
			daos.writeBoolean(mergeOperatorClass != null);
			if (mergeOperatorClass != null) {
				daos.writeUTF(mergeOperatorClass);
			}
			baos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Nullable
	private FFMAbstractMergeOperator resolveMergeOperator(ColumnSchema schema,
			@Nullable FFMAbstractMergeOperator configuredMergeOperator) throws RocksDBException {
		if (configuredMergeOperator != null) {
			return configuredMergeOperator;
		}
		String mergeOperatorName = schema.mergeOperatorName();
		Long mergeOperatorVersion = schema.mergeOperatorVersion();
		String mergeOperatorClass = schema.mergeOperatorClass();
		if ((mergeOperatorName != null || mergeOperatorVersion != null) && mergeOperatorClass != null) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Merge operator name/version and merge operator class cannot be both specified"
			);
		}
		if (mergeOperatorName != null || mergeOperatorVersion != null) {
			if (mergeOperatorName == null || mergeOperatorVersion == null) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Merge operator name and version must both be specified when one of them is set"
				);
			}
			return edb.mergeOperatorRegistry.get(mergeOperatorName, mergeOperatorVersion);
		}
		if (mergeOperatorClass != null && !mergeOperatorClass.isBlank()) {
			try {
				Class<?> clazz = Class.forName(mergeOperatorClass);
				if (!FFMAbstractMergeOperator.class.isAssignableFrom(clazz)) {
					throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
							"Merge operator class does not extend FFMAbstractMergeOperator: " + mergeOperatorClass
					);
				}
				@SuppressWarnings("unchecked") Class<? extends FFMAbstractMergeOperator> typed = (Class<? extends FFMAbstractMergeOperator>) clazz;
				return typed.getConstructor().newInstance();
			} catch (ReflectiveOperationException e) {
				throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
						"Failed to instantiate merge operator: " + mergeOperatorClass,
						e
				);
			}
		}
		return configuredMergeOperator;
	}

	private long internalRegisterColumn(@NotNull String name,
			@NotNull ColumnFamilyHandle cfh,
			@NotNull ColumnSchema schema,
			@Nullable FFMAbstractMergeOperator mergeOp) {
		long id = cfh.getID();
		var pressureColumn = edb.storageMetrics.storagePressureColumn(name, cfh);

		if (edb.columns.containsKey(id)) {
			throw new IllegalStateException("Column ID already registered: " + id);
		}

		Long previous = edb.columnNamesIndex.putIfAbsent(name, id);
		if (previous != null) {
			throw new UnsupportedOperationException("Column already registered: " + name);
		}

		var column = new ColumnInstance(cfh, schema, mergeOp);
		edb.columnWorkloadNames.put(id, edb.columnWorkloadGroups.getOrDefault(name, edb.fallbackWorkloadGroup));
		edb.columns.put(id, column);
		edb.storageMetrics.upsertStoragePressureColumn(new StoragePressureColumn(
				pressureColumn.id(),
				pressureColumn.handle(),
				column,
				pressureColumn.effectiveSoftPendingCompactionBytesLimit(), pressureColumn.effectiveLevel0SlowdownWritesTrigger(),
				pressureColumn.effectiveMaxWriteBufferNumber(), pressureColumn.effectiveMinWriteBufferNumberToMerge()));

		edb.logger.info("Registered column: " + column);
		return id;
	}

	void loadExistingColumns(Map<String, FFMAbstractMergeOperator> initialMergeOperators) {
		Map<String, ColumnFamilyHandle> startupHandles = new HashMap<>();
		for (var entry : edb.db.getStartupColumns().entrySet()) {
			try {
				startupHandles.put(new String(entry.getKey().getName(), StandardCharsets.UTF_8), entry.getValue());
			} catch (Exception e) {
				edb.logger.warn("Failed to decode column name", e);
			}
		}

		try (var it = edb.db.get().newIterator(edb.columnSchemasColumnDescriptorHandle)) {
			it.seekToFirst();
			while (it.isValid()) {
				var key = it.key();
				var name = new String(key, StandardCharsets.UTF_8);
				ColumnSchema value = decodeColumnSchema(it.value());

				var cfh = startupHandles.get(name);
				if (cfh != null) {
					FFMAbstractMergeOperator mergeOp = resolveMergeOperator(value, initialMergeOperators.get(name));
					internalRegisterColumn(name, cfh, value, mergeOp);
				}
				it.next();
			}
		}

		colIt:
		for (var entry : startupHandles.entrySet()) {
			String name = entry.getKey();
			ColumnFamilyHandle cfh = entry.getValue();
			long id = cfh.getID();

			if (edb.columns.containsKey(id)) {
				continue;
			}

			// Skip system columns
			byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
			for (byte[] systemColumn : EmbeddedDB.SYSTEM_COLUMNS) {
				if (Arrays.equals(nameBytes, systemColumn)) {
					continue colIt;
				}
			}

			// Column exists in RocksDB but has no stored schema metadata.
			// Mark as unconfigured — operations are forbidden until createColumn sets the schema.
			edb.logger.info("Found column without stored schema, marking as unconfigured: {}", name);
			edb.unconfiguredColumns.put(name, cfh);
		}
	}

	/**
	 * The column must be unregistered once!!! Do not try to unregister a column that may already be unregistered, or that
	 * may not be registered
	 */
	private ColumnInstance unregisterColumn(long id, @NotNull String name) {
		edb.columnWorkloadNames.remove(id);
		synchronized (edb.columnEditLock) {
			var col = edb.columns.remove(id);
			Objects.requireNonNull(col, () -> "Column does not exist: " + id);
			edb.storageMetrics.removeStoragePressureColumn(id);

			if (edb.columnNamesIndex.remove(name) == null) {
				edb.logger.warn("Column name not found in index during unregister: {}", name);
			}

			ColumnFamilyOptions columnConfig = edb.columnsConifg.remove(name);
			if (columnConfig != null) {
				columnConfig.close();
			} else {
				edb.logger.warn("Column config not found during unregister: {}", name);
			}

			return col;
		}
	}

	private boolean isCompatibleSchemaChange(ColumnSchema oldSchema, ColumnSchema newSchema) {
		return Objects.equals(oldSchema.keys(), newSchema.keys()) &&
				Objects.equals(oldSchema.variableTailKeys(), newSchema.variableTailKeys()) &&
				oldSchema.hasValue() == newSchema.hasValue();
	}

	private void updateColumnSchema(long colId, String name, ColumnSchema newSchema, FFMAbstractMergeOperator newOp) {
		// Persist to disk
		try {
			byte[] key = name.getBytes(StandardCharsets.UTF_8);
			byte[] value = encodeColumnSchema(newSchema);
			edb.db.get().put(edb.columnSchemasColumnDescriptorHandle, key, value);
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Failed to persist updated schema for column: " + name, e);
		}

		// Update in-memory
		synchronized (edb.columnEditLock) {
			ColumnInstance oldCol = edb.columns.get(colId);
			if (oldCol == null) return;

			FFMAbstractMergeOperator currentOp = oldCol.mergeOperator();
			if (currentOp instanceof DelegatingMergeOperator delegateOp && newOp != null) {
				delegateOp.setDelegate(newOp);
				newOp = delegateOp;
			} else if (newOp != null && currentOp != null) {
				edb.logger.warn("Existing merge operator for column {} is not a DelegatingMergeOperator. Hot swap will only take effect after restart.", name);
			}

			ColumnInstance newCol = oldCol.withSchema(newSchema, newOp);
			edb.columns.put(colId, newCol);
		}
		edb.logger.info("Updated schema for column: {} (MergeOperator changed)", name);
	}
	public long createColumn(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return createColumn(name, schema, null);
	}

	/**
	 * Creates the column {@code name}, which must not exist, with {@code schema} and with the table files of
	 * {@code exported} (what {@link #exportColumn} of a column with the same schema returned, of this or of another
	 * database) as its contents: RocksDB links the files into the column's volume (copies them from another file
	 * system). The column is registered exactly as {@link #createColumn}
	 * registers a new one (its options from this database's config, its schema in {@code _column_schemas_}); a merge
	 * operator that the schema names must already be in this database's registry.
	 */
	public long createColumnFromExport(String name, @NotNull ColumnSchema schema,
			@NotNull ExportedColumn exported) throws RocksDBException {
		return createColumn(name, schema, exported.metadata());
	}

	/**
	 * Flushes the column and hard-links its table files (copies those on another file system) into {@code exportDir},
	 * which must not exist and whose parent must: RocksDB's column family export creates it, and takes each table file
	 * from the directory the column keeps it in (its {@code cf_paths} volume or the database directory). The result is
	 * what {@link #createColumnFromExport} takes; the caller closes it once the column is imported, and deletes
	 * {@code exportDir}. Nothing may write to the column or compact it meanwhile. Refused for a column with blob
	 * files, which the export leaves out.
	 */
	public ExportedColumn exportColumn(long columnId, java.nio.file.Path exportDir) throws RocksDBException {
		edb.ops.beginOp();
		try {
			synchronized (edb.columnEditLock) {
				var col = edb.getColumn(columnId);
				var db = edb.db.get();
				if (edb.path == null) {
					throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "An in-memory database exports no column");
				}
				try (var fo = new org.rocksdb.FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
					db.flush(fo, col.cfh());
				}
				long blobFiles = db.getLongProperty(col.cfh(), "rocksdb.num-blob-files");
				if (blobFiles > 0) {
					throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "The column has " + blobFiles
							+ " blob files, which RocksDB's column family export does not carry");
				}
				var files = columnTableFiles(columnId);
				ExportImportFilesMetaData metadata = null;
				boolean done = false;
				try {
					try (var checkpoint = org.rocksdb.Checkpoint.create(db)) {
						metadata = checkpoint.exportColumnFamily(col.cfh(), exportDir.toString());
					}
					// What RocksDB listed is what was listed here: nothing was flushed or compacted in between.
					var expected = new TreeSet<String>();
					files.forEach(file -> expected.add(file.getFileName().toString()));
					var exported = new TreeSet<String>();
					try (var list = java.nio.file.Files.list(exportDir)) {
						list.forEach(file -> exported.add(file.getFileName().toString()));
					}
					if (!exported.equals(expected)) {
						throw new IOException("The column's table files changed during the export: " + expected + " became "
								+ exported);
					}
					done = true;
					return new ExportedColumn(metadata, files.size());
				} finally {
					if (!done) {
						if (metadata != null) {
							metadata.close();
						}
					}
				}
			}
		} catch (org.rocksdb.RocksDBException | IOException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Failed to export column " + columnId + ": "
					+ e.getMessage(), e);
		} finally {
			edb.ops.endOp();
		}
	}

	/** The live table files of the column, as RocksDB lists them now (memtables are not flushed first). */
	public List<java.nio.file.Path> columnTableFiles(long columnId) {
		edb.ops.beginOp();
		try {
			var files = new ArrayList<java.nio.file.Path>();
			for (var level : edb.db.get().getColumnFamilyMetaData(edb.getColumn(columnId).cfh()).levels()) {
				for (var file : level.files()) {
					String fileName = file.fileName();
					files.add(java.nio.file.Path.of(file.path(), fileName.startsWith("/") ? fileName.substring(1) : fileName));
				}
			}
			return files;
		} finally {
			edb.ops.endOp();
		}
	}

	/**
	 * The class name of the operator that merges the column's values (the one a {@link DelegatingMergeOperator} wraps),
	 * null without one: from the schema's registry entry or class, or from the column's {@code merge-operator-class}
	 * in the config.
	 */
	public @Nullable String columnMergeOperatorClass(long columnId) {
		FFMAbstractMergeOperator operator = edb.getColumn(columnId).mergeOperator();
		while (operator instanceof DelegatingMergeOperator delegating) {
			operator = delegating.getDelegate();
		}
		return operator == null ? null : operator.getClass().getName();
	}

	private long createColumn(String name, @NotNull ColumnSchema schema, @Nullable ExportImportFilesMetaData imported)
			throws RocksDBException {
		var nameBytes = name.getBytes(StandardCharsets.UTF_8);
		if (EmbeddedDB.SYSTEM_COLUMNS.stream().anyMatch(system -> Arrays.equals(system, nameBytes))) {
			throw RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL, "Reserved column name: " + name);
		}
		var start = System.nanoTime();
		edb.actionLogger.logAction("CreateColumn", start, name, null, schema, null, null, null, null);
		edb.ops.beginOp();
		try {
			synchronized (edb.columnEditLock) {
				var colId = getColumnIdOrNull(name);
				var col = colId != null ? edb.getColumn(colId) : null;
				if (imported != null && (col != null || edb.unconfiguredColumns.containsKey(name))) {
					throw RocksDBException.of(RocksDBErrorType.COLUMN_EXISTS,
							"Column exists, table files cannot be imported into it: " + name);
				}
				if (col != null) {
					if (schema.equals(col.schema())) {
						return colId;
					} else if (isCompatibleSchemaChange(col.schema(), schema)) {
						GlobalDatabaseConfig globalConfig;
						try {
							globalConfig = edb.config.global();
						} catch (GestaltException e) {
							throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Failed to get global config", e);
						}
						var options = RocksDBLoader.getColumnOptions(name,
								edb.path,
								edb.definitiveDbPath,
								globalConfig,
								edb.logger,
								edb.refs,
								edb.path == null,
								edb.caches, edb.metadataCaches
						);
						// Force use of configured merge operator if available, or resolve from new schema
						var mergeOp = resolveMergeOperator(schema, options.mergeOperator());
						updateColumnSchema(colId, name, schema, mergeOp);
						return colId;
						} else {
						throw RocksDBException.of(RocksDBErrorType.COLUMN_EXISTS,
								"Column exists, with a different schema: " + name
						);
					}
				} else {
					// Check if this column exists in RocksDB but was loaded without a stored schema
					var unconfiguredCfh = edb.unconfiguredColumns.remove(name);
					if (unconfiguredCfh != null) {
						// Column exists in RocksDB but had no schema. Configure it now.
						edb.logger.info("Configuring schema for previously unconfigured column: {}", name);
						try {
							var options = RocksDBLoader.getColumnOptions(name,
									edb.path,
									edb.definitiveDbPath,
									edb.config.global(),
									edb.logger,
									edb.refs,
									edb.path == null,
									edb.caches, edb.metadataCaches
							);
							var mergeOp = resolveMergeOperator(schema, options.mergeOperator());
							if (mergeOp != null && !(mergeOp instanceof DelegatingMergeOperator)) {
								mergeOp = new DelegatingMergeOperator("Delegating-" + name, mergeOp);
							}
							var prev = edb.columnsConifg.put(name, options.options());
							if (prev != null) {
								prev.close();
							}
							byte[] key = name.getBytes(StandardCharsets.UTF_8);
							byte[] value = encodeColumnSchema(schema);
							edb.db.get().put(edb.columnSchemasColumnDescriptorHandle, key, value);
							return internalRegisterColumn(name, unconfiguredCfh, schema, mergeOp);
						} catch (org.rocksdb.RocksDBException | GestaltException e) {
							// Put it back if we failed
							edb.unconfiguredColumns.put(name, unconfiguredCfh);
							throw RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL, e);
						}
					}
					try {
						var options = RocksDBLoader.getColumnOptions(name,
								edb.path,
								edb.definitiveDbPath,
								edb.config.global(),
								edb.logger,
								edb.refs,
								edb.path == null,
								edb.caches, edb.metadataCaches
						);
						var mergeOp = resolveMergeOperator(schema, options.mergeOperator());
						if (mergeOp != null && !(mergeOp instanceof DelegatingMergeOperator)) {
							mergeOp = new DelegatingMergeOperator("Delegating-" + name, mergeOp);
						}
						if (mergeOp != null) {
							options.options().setMergeOperator(mergeOp);
						}

						var prev = edb.columnsConifg.put(name, options.options());
						if (prev != null) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL,
									"ColumnsConfig already exists with name \"" + name + "\""
							);
						}
						byte[] key = name.getBytes(StandardCharsets.UTF_8);
						ColumnFamilyHandle cf;
						if (imported == null) {
							cf = edb.db.get().createColumnFamily(new ColumnFamilyDescriptor(key, options.options()));
						} else {
							try (var importOptions = new org.rocksdb.ImportColumnFamilyOptions().setMoveFiles(true)) {
								cf = edb.db.get().createColumnFamilyWithImport(new ColumnFamilyDescriptor(key, options.options()),
										importOptions, imported);
							} catch (org.rocksdb.RocksDBException e) {
								// Nothing was created: the name stays free.
								edb.columnsConifg.remove(name, options.options());
								throw e;
							}
						}
						byte[] value = encodeColumnSchema(schema);
						edb.db.get().put(edb.columnSchemasColumnDescriptorHandle, key, value);
						return internalRegisterColumn(name, cf, schema, mergeOp);
					} catch (org.rocksdb.RocksDBException | GestaltException e) {
						throw RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL, e);
					}
				}
			}
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.createColumnTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public long uploadMergeOperator(String name, String className, byte[] jarData) {
		return edb.mergeOperatorRegistry.upload(name, className, jarData);
	}
	public Long checkMergeOperator(String name, byte[] hash) {
		return edb.mergeOperatorRegistry.check(name, hash);
	}
	public void deleteColumn(long columnId) throws RocksDBException {
		var start = System.nanoTime();
		edb.actionLogger.logAction("DeleteColumn", start, columnId, null, null, null, null, null, null);
		edb.ops.beginOp();
		try {
			synchronized (edb.columnEditLock) {
				var col = edb.getColumn(columnId);
				try {
					var columnName = new String(col.cfh().getName(), StandardCharsets.UTF_8);
					deleteRegisteredColumnLocked(columnId, columnName, col);
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.COLUMN_DELETE_FAIL, e);
				}
			}
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.deleteColumnTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public boolean deleteColumnIfExists(@NotNull String name) throws RocksDBException {
		Objects.requireNonNull(name, "name");
		var start = System.nanoTime();
		edb.actionLogger.logAction("DeleteColumnIfExists", start, name, null, null, null, null, null, null);
		edb.ops.beginOp();
		try {
			synchronized (edb.columnEditLock) {
				try {
					var columnId = getRegisteredColumnIdOrNull(name);
					if (columnId != null) {
						deleteRegisteredColumnLocked(columnId, name, edb.getColumn(columnId));
						return true;
					}

					var unconfiguredColumn = edb.unconfiguredColumns.get(name);
					if (unconfiguredColumn != null) {
						edb.db.get().dropColumnFamily(unconfiguredColumn);
						edb.unconfiguredColumns.remove(name);
						unconfiguredColumn.close();
						var columnConfig = edb.columnsConifg.remove(name);
						if (columnConfig != null) {
							columnConfig.close();
						} else {
							edb.logger.warn("Column config not found while deleting unconfigured column: {}", name);
						}
						deleteColumnSchemaMetadata(name);
						return true;
					}

					// A previous attempt may have dropped the physical column before metadata cleanup failed.
					deleteColumnSchemaMetadata(name);
					return false;
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.COLUMN_DELETE_FAIL, e);
				}
			}
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.deleteColumnIfExistsTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private void deleteRegisteredColumnLocked(long columnId,
			@NotNull String name,
			@NotNull ColumnInstance column) throws org.rocksdb.RocksDBException {
		// Closing admission linearizes deletion against every data-path acquisition.
		// Existing owners retain the native handle until they finish; holding the edit
		// lock prevents a same-name replacement from overlapping that retirement.
		column.stopNewUsesAndWait();
		edb.db.get().dropColumnFamily(column.cfh());
		unregisterColumn(columnId, name).closeHandle();
		deleteColumnSchemaMetadata(name);
	}

	private void deleteColumnSchemaMetadata(@NotNull String name) throws org.rocksdb.RocksDBException {
		edb.db.get().delete(edb.columnSchemasColumnDescriptorHandle, name.getBytes(StandardCharsets.UTF_8));
	}
	public long getColumnId(@NotNull String name) {
		var start = System.nanoTime();
		edb.actionLogger.logAction("GetColumnId", start, name, null, null, null, null, null, null);
		try {
			var columnId = getColumnIdOrNull(name);
			if (columnId == null) {
				throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND, "Column not found: " + name);
			} else {
				return columnId;
			}
		} finally {
			var end = System.nanoTime();
			edb.getColumnIdTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public long estimateNumKeys(long columnId) {
		var start = System.nanoTime();
		edb.ops.beginOp();
		edb.actionLogger.logAction("EstimateNumKeys", start, columnId, null, null, null, null, null, null);
		ColumnInstance col = null;
		try {
			col = edb.beginColumnUse(columnId);
			try {
				return edb.db.get().getLongProperty(col.cfh(), RocksDBLongProperty.ESTIMATE_NUM_KEYS.getName());
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.GET_PROPERTY_ERROR, e);
			}
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.estimateNumKeysTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	Long getColumnIdOrNull(@NotNull String name) {
		var columnId = getRegisteredColumnIdOrNull(name);
		if (columnId == null) {
			return null;
		}
		var col = edb.columns.get(columnId);
		return col != null && col.isAcceptingUses() ? columnId : null;
	}

	private Long getRegisteredColumnIdOrNull(@NotNull String name) {
		var columnId = (long) edb.columnNamesIndex.getOrDefault(name, -1L);
		ColumnInstance col;
		if (columnId == -1L || (col = edb.columns.get(columnId)) == null || !col.cfh().isOwningHandle()) {
			return null;
		} else {
			return columnId;
		}
	}
	public Map<String, ColumnSchema> getAllColumnDefinitions() throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			edb.actionLogger.logAction("GetAllColumnDefinitions", start, null, null, null, null, null, null, null);
			return edb.columnNamesIndex
					.entrySet()
					.stream()
					.collect(Collectors.toMap(Entry::getKey, e -> edb.columns.get(e.getValue()).schema()));
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.getAllColumnDefinitionsTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	void printStartupInfo() {
		if (!Boolean.parseBoolean(System.getProperty("rockserver.core.print-config", "true"))) {
			return;
		}

		StringBuilder sb = new StringBuilder();

		// Columns
		sb.append("\n");
		ConsoleTable columnsTable = new ConsoleTable();
		columnsTable.setHeaders("ID", "Name", "Schema", "Merge Operator");

		var sortedCols = new ArrayList<>(edb.columns.values());
		sortedCols.sort(Comparator.comparingLong(c -> c.cfh().getID()));

		for (var col : sortedCols) {
			long id = col.cfh().getID();
			String name;
			try {
				name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
			} catch (Exception e) {
				name = "???";
			}
			String schema = formatSchema(col.schema());
			String mergeOp = formatMergeOp(col);

			columnsTable.addRow(String.valueOf(id), name, schema, mergeOp);
		}

		// Show unconfigured columns (exist in RocksDB but have no stored schema)
		for (var entry : edb.unconfiguredColumns.entrySet()) {
			long id = entry.getValue().getID();
			columnsTable.addRow(String.valueOf(id), entry.getKey(), "<unconfigured>", "-");
		}

		sb.append(columnsTable.toString());

		// Merge Operators
		sb.append("\n");
		ConsoleTable opsTable = new ConsoleTable();
		opsTable.setHeaders("Name", "Version", "Class");

		var ops = edb.mergeOperatorRegistry.listAll();
		ops.sort(Comparator.comparing(MergeOperatorRegistry.MergeOperatorInfo::name).thenComparingLong(MergeOperatorRegistry.MergeOperatorInfo::version));

		for (var op : ops) {
			opsTable.addRow(op.name(), String.valueOf(op.version()), op.className());
		}
		sb.append(opsTable.toString());

		edb.logger.info("Startup Information:{}", sb.toString());
	}

	private String formatSchema(ColumnSchema s) {
		StringBuilder sb = new StringBuilder();
		sb.append("Fixed:").append(s.fixedLengthKeysCount());
		if (!s.variableTailKeys().isEmpty()) {
			sb.append(", Var:[");
			for (int i=0; i<s.variableTailKeys().size(); i++) {
				if (i>0) sb.append(",");
				sb.append(s.variableTailKeys().get(i).name());
			}
			sb.append("]");
		}
		sb.append(", Val:").append(s.hasValue() ? "Y" : "N");
		return sb.toString();
	}

	private String formatMergeOp(ColumnInstance col) {
		var s = col.schema();
		if (s.mergeOperatorName() != null) {
			return s.mergeOperatorName() + " (v" + s.mergeOperatorVersion() + ")";
		}
		if (s.mergeOperatorClass() != null) {
			String cls = s.mergeOperatorClass();
			return cls.substring(cls.lastIndexOf('.') + 1);
		}
		if (col.mergeOperator() != null) {
			return "Configured (" + col.mergeOperator().getClass().getSimpleName() + ")";
		}
		return "-";
	}
}
