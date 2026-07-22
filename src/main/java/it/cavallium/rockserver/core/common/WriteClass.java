package it.cavallium.rockserver.core.common;

/**
 * Admission class for caller-selected database mutations.
 *
 * <p>{@link #FOREGROUND} is the compatibility default. {@link #MAINTENANCE}
 * identifies rebuildable or background work so a server can admit it separately
 * without changing the durability or ordering semantics of the mutation.</p>
 */
public enum WriteClass {
	FOREGROUND,
	MAINTENANCE
}
