package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public record Keys(@NotNull Buf @NotNull ... keys) {

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
		if (keys.length != keys1.keys.length) {
			return false;
		}
		for (int i = 0; i < keys.length; i++) {
			var k1 = keys[i];
			var k2 = keys1.keys[i];
			if (!Utils.valueEquals(k1, k2)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		for (@NotNull Buf key : keys) {
			hash = hash * 31 + Utils.valueHash(key);
		}
		return hash;
	}
}
