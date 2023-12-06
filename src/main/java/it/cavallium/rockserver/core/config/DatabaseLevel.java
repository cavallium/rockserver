package it.cavallium.rockserver.core.config;

import org.rocksdb.CompressionType;

public interface DatabaseLevel {

	CompressionType compression();

	DataSize maxDictBytes();

}
