package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.rocksdb.CompressionType;

public interface ColumnLevelConfig {

	CompressionType compression() throws GestaltException;

	DataSize maxDictBytes() throws GestaltException;

}
