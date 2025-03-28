package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.Nullable;

public record UpdateContext<T>(@Nullable T previous, long updateId) {}
