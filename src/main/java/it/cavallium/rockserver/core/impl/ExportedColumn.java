package it.cavallium.rockserver.core.impl;

import org.rocksdb.ExportImportFilesMetaData;

/**
 * A column exported by {@link EmbeddedDB#exportColumn}: RocksDB's description of its table files, for
 * {@link EmbeddedDB#createColumnFromExport}. Closing it releases the description; the export directory is the caller's to delete.
 */
public final class ExportedColumn implements AutoCloseable {

	private final ExportImportFilesMetaData metadata;
	private final int files;

	ExportedColumn(ExportImportFilesMetaData metadata, int files) {
		this.metadata = metadata;
		this.files = files;
	}

	ExportImportFilesMetaData metadata() {
		return metadata;
	}

	/** How many table files the column had. */
	public int files() {
		return files;
	}

	@Override
	public void close() {
		metadata.close();
	}
}
