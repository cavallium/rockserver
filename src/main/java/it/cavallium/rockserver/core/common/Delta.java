package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.Nullable;

public record Delta<T>(@Nullable T previous, @Nullable T current) {}
