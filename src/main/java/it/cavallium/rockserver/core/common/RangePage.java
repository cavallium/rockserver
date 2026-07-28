package it.cavallium.rockserver.core.common;

import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/** Immutable result of one bounded range request. */
public record RangePage<T>(List<T> items, @Nullable Keys resumeAfter, boolean hasMore) {

	public RangePage {
		items = List.copyOf(Objects.requireNonNull(items, "items"));
		if (items.isEmpty() != (resumeAfter == null)) {
			throw new IllegalArgumentException("resumeAfter must be present exactly when the page contains items");
		}
		if (hasMore && items.isEmpty()) {
			throw new IllegalArgumentException("a page with hasMore=true must contain a continuation item");
		}
	}

	public static <T> RangePage<T> empty() {
		return new RangePage<>(List.of(), null, false);
	}
}
