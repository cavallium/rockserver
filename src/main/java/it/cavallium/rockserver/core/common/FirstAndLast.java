package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.Nullable;

public record FirstAndLast<T>(@Nullable T first, @Nullable T last) {
}
