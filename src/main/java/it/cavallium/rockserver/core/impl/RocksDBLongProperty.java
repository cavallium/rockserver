package it.cavallium.rockserver.core.impl;

public enum RocksDBLongProperty implements RocksDBProperty {
	NUM_FILES_AT_LEVEL_0("num-files-at-level0"),
	NUM_FILES_AT_LEVEL_1("num-files-at-level1"),
	NUM_FILES_AT_LEVEL_2("num-files-at-level2"),
	NUM_FILES_AT_LEVEL_3("num-files-at-level3"),
	NUM_FILES_AT_LEVEL_4("num-files-at-level4"),
	NUM_FILES_AT_LEVEL_5("num-files-at-level5"),
	NUM_FILES_AT_LEVEL_6("num-files-at-level6"),
	NUM_FILES_AT_LEVEL_7("num-files-at-level7"),
	NUM_FILES_AT_LEVEL_8("num-files-at-level8"),
	NUM_FILES_AT_LEVEL_9("num-files-at-level9"),
	COMPRESSION_RATIO_AT_LEVEL_0("compression-ratio-at-level0"),
	COMPRESSION_RATIO_AT_LEVEL_1("compression-ratio-at-level1"),
	COMPRESSION_RATIO_AT_LEVEL_2("compression-ratio-at-level2"),
	COMPRESSION_RATIO_AT_LEVEL_3("compression-ratio-at-level3"),
	COMPRESSION_RATIO_AT_LEVEL_4("compression-ratio-at-level4"),
	COMPRESSION_RATIO_AT_LEVEL_5("compression-ratio-at-level5"),
	COMPRESSION_RATIO_AT_LEVEL_6("compression-ratio-at-level6"),
	COMPRESSION_RATIO_AT_LEVEL_7("compression-ratio-at-level7"),
	COMPRESSION_RATIO_AT_LEVEL_8("compression-ratio-at-level8"),
	COMPRESSION_RATIO_AT_LEVEL_9("compression-ratio-at-level9"),
	NUM_IMMUTABLE_MEM_TABLE("num-immutable-mem-table"),
	NUM_IMMUTABLE_MEM_TABLE_FLUSHED("num-immutable-mem-table-flushed"),
	MEM_TABLE_FLUSH_PENDING("mem-table-flush-pending"),
	NUM_RUNNING_FLUSHES("num-running-flushes", AggregationMode.DB_WIDE),
	COMPACTION_PENDING("compaction-pending"),
	NUM_RUNNING_COMPACTIONS("num-running-compactions", AggregationMode.DB_WIDE),
	BACKGROUND_ERRORS("background-errors", AggregationMode.DB_WIDE),
	CUR_SIZE_ACTIVE_MEM_TABLE("cur-size-active-mem-table"),
	CUR_SIZE_ALL_MEM_TABLES("cur-size-all-mem-tables"),
	SIZE_ALL_MEM_TABLES("size-all-mem-tables"),
	NUM_ENTRIES_ACTIVE_MEM_TABLE("num-entries-active-mem-table"),
	NUM_ENTRIES_IMMUTABLE_MEM_TABLES("num-entries-imm-mem-tables"),
	NUM_DELETES_ACTIVE_MEM_TABLE("num-deletes-active-mem-table"),
	NUM_DELETES_IMMUTABLE_MEM_TABLES("num-deletes-imm-mem-tables"),
	ESTIMATE_NUM_KEYS("estimate-num-keys"),
	ESTIMATE_TABLE_READERS_MEM("estimate-table-readers-mem"),
	IS_FILE_DELETIONS_ENABLED("is-file-deletions-enabled", AggregationMode.DB_WIDE),
	NUM_SNAPSHOTS("num-snapshots", AggregationMode.DB_WIDE),
	OLDEST_SNAPSHOT_TIME("oldest-snapshot-time", AggregationMode.DB_WIDE),
	OLDEST_SNAPSHOT_SEQUENCE("oldest-snapshot-sequence", AggregationMode.DB_WIDE),
	NUM_LIVE_VERSIONS("num-live-versions"),
	CURRENT_SUPER_VERSION_NUMBER("current-super-version-number"),
	ESTIMATE_LIVE_DATA_SIZE("estimate-live-data-size"),
	MIN_LOG_NUMBER_TO_KEEP("min-log-number-to-keep", AggregationMode.DB_WIDE),
	MIN_OBSOLETE_SST_NUMBER_TO_KEEP("min-obsolete-sst-number-to-keep", AggregationMode.DB_WIDE),
	TOTAL_SST_FILES_SIZE("total-sst-files-size"),
	LIVE_SST_FILES_SIZE("live-sst-files-size"),
	LIVE_SST_FILES_SIZE_AT_TEMPERATURE("live-sst-files-size-at-temperature"),
	BASE_LEVEL("base-level"),
	ESTIMATE_PENDING_COMPACTION_BYTES("estimate-pending-compaction-bytes"),
	ACTUAL_DELAYED_WRITE_RATE("actual-delayed-write-rate", AggregationMode.DB_WIDE),
	IS_WRITE_STOPPED("is-write-stopped", AggregationMode.DB_WIDE),
	ESTIMATE_OLDEST_KEY_TIME("estimate-oldest-key-time"),
	BLOCK_CACHE_CAPACITY("block-cache-capacity", AggregationMode.SINGLE_CF),
	BLOCK_CACHE_USAGE("block-cache-usage", AggregationMode.SINGLE_CF),
	BLOCK_CACHE_PINNED_USAGE("block-cache-pinned-usage", AggregationMode.SINGLE_CF),
	NUM_BLOB_FILES("num-blob-files"),
	TOTAL_BLOB_FILE_SIZE("total-blob-file-size"),
	LIVE_BLOB_FILE_SIZE("live-blob-file-size"),
	LIVE_BLOB_FILE_GARBAGE_SIZE("live-blob-file-garbage-size"),
	FILE_READ_DB_OPEN_MICROS("file.read.db.open.micros", AggregationMode.DB_WIDE)
	;

	/**
	 * How to aggregate this property across column families.
	 */
	public enum AggregationMode {
		/** Per-column-family property: sum values across all CFs. */
		PER_CF,
		/** DB-wide property: query once without a CF handle. */
		DB_WIDE,
		/** Shared-resource property (e.g. block cache): query once with any single CF handle. */
		SINGLE_CF
	}

	private final String name;
	private final AggregationMode aggregationMode;

	RocksDBLongProperty(String name) {
		this(name, AggregationMode.PER_CF);
	}

	RocksDBLongProperty(String name, AggregationMode aggregationMode) {
		this.name = name;
		this.aggregationMode = aggregationMode;
	}

	@Override
	public String toString() {
		return "rocksdb." + name;
	}

	@Override
	public String getName() {
		return "rocksdb." + name;
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public boolean isMap() {
		return false;
	}

	@Override
	public boolean isString() {
		return false;
	}

	public AggregationMode getAggregationMode() {
		return aggregationMode;
	}

	/**
	 * @deprecated Use {@link #getAggregationMode()} instead.
	 */
	@Deprecated
	public boolean isDividedByColumnFamily() {
		return aggregationMode == AggregationMode.PER_CF;
	}
}
