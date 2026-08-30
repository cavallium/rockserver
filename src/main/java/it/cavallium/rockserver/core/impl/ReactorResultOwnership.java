package it.cavallium.rockserver.core.impl;

import org.jetbrains.annotations.Nullable;

/** Allocation-free helpers for intrusive Reactor result-ownership state machines. */
public final class ReactorResultOwnership {

	private static final Object NULL_RESULT = new Object();

	private ReactorResultOwnership() {
	}

	/** Encode a possibly-null result for storage in an owning task's pending-result field. */
	public static Object encode(@Nullable Object result) {
		return result == null ? NULL_RESULT : result;
	}

	/** Decode a non-null pending marker previously returned by {@link #encode(Object)}. */
	@SuppressWarnings("unchecked")
	public static <T> @Nullable T decode(Object encoded) {
		return encoded == NULL_RESULT ? null : (T) encoded;
	}

}
