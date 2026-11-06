package it.cavallium.rockserver.distributed;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System properties of a replicated node. They are named {@value #PREFIX}{@code <name>}; a few had other names before,
 * which are still read when the new one is not set, with a warning.
 */
final class DistributedProperties {

	static final String PREFIX = "it.cavallium.rockserver.distributed.";
	private static final Logger LOG = LoggerFactory.getLogger(DistributedProperties.class);

	private DistributedProperties() {
	}

	/** The value of property {@code name}, else of the first old name that is set, else null. */
	static @Nullable String get(String name, String... oldNames) {
		var value = System.getProperty(name);
		if (value != null) {
			return value;
		}
		for (var oldName : oldNames) {
			value = System.getProperty(oldName);
			if (value != null) {
				LOG.warn("System property {} is deprecated, use {}", oldName, name);
				return value;
			}
		}
		return null;
	}

	/** {@link #get} as a long, {@code defaultValue} if no name is set. */
	static long getLong(long defaultValue, String name, String... oldNames) {
		var value = get(name, oldNames);
		if (value == null) {
			return defaultValue;
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("System property " + name + " must be an integer, but was: " + value, ex);
		}
	}
}
