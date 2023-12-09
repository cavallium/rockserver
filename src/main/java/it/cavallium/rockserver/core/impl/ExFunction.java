package it.cavallium.rockserver.core.impl;

@FunctionalInterface
public interface ExFunction<T, U> {

	U apply(T input) throws Exception;
}
