package it.cavallium.rockserver.core.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.rocksdb.ExportImportFilesMetaData;

/**
 * A column exported by {@link EmbeddedDB#exportColumn}: RocksDB's description of its table files, for
 * {@link EmbeddedDB#createColumnFromExport}. Closing it releases the description and removes the symbolic link to the
 * export directory from the database directory; the export directory is the caller's to delete.
 */
public final class ExportedColumn implements AutoCloseable {

	private final ExportImportFilesMetaData metadata;
	private final Path link;
	private final int files;

	ExportedColumn(ExportImportFilesMetaData metadata, Path link, int files) {
		this.metadata = metadata;
		this.link = link;
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
		try {
			Files.deleteIfExists(link);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
