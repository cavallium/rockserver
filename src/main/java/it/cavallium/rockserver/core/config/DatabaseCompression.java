package it.cavallium.rockserver.core.config;

import org.rocksdb.CompressionType;

public enum DatabaseCompression {
	PLAIN(CompressionType.NO_COMPRESSION),
	SNAPPY(CompressionType.SNAPPY_COMPRESSION),
	LZ4(CompressionType.LZ4_COMPRESSION),
	LZ4_HC(CompressionType.LZ4HC_COMPRESSION),
	ZSTD(CompressionType.ZSTD_COMPRESSION),
	ZLIB(CompressionType.ZLIB_COMPRESSION),
	BZLIB2(CompressionType.BZLIB2_COMPRESSION);

	private final CompressionType compressionType;

	DatabaseCompression(CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	public CompressionType compressionType() {
		return compressionType;
	}
}
