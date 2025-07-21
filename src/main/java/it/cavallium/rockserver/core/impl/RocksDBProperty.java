package it.cavallium.rockserver.core.impl;

public interface RocksDBProperty {

	/**
	 * Get rocksdb property name
	 * @return name, with the "rocksdb." prefix included
	 */
	String getName();

	boolean isNumeric();

	boolean isMap();

	boolean isString();
}
