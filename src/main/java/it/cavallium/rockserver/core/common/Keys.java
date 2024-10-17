package it.cavallium.rockserver.core.common;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public record Keys(@NotNull MemorySegment @NotNull ... keys) {

	@Override
	public String toString() {
		return Arrays.stream(keys).map(Utils::toPrettyString).collect(Collectors.joining(";", "[", "]"));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Keys keys1 = (Keys) o;
		return Arrays.equals(keys, keys1.keys);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(keys);
	}
}
