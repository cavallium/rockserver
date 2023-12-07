package it.cavallium.rockserver.core.common;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;

public sealed interface Callback<METHOD_DATA_TYPE, RESULT_TYPE> {

	static boolean requiresGettingPreviousValue(PutCallback<?, ?> callback) {
		return callback instanceof CallbackPrevious<?>
				|| callback instanceof CallbackDelta<?>
				|| callback instanceof CallbackChanged;
	}

	static boolean requiresGettingCurrentValue(GetCallback<?, ?> callback) {
		return callback instanceof CallbackCurrent<?>;
	}

	static <T> CallbackPrevious<T> previous() {
		// todo: create static instance
		return new CallbackPrevious<>();
	}

	static <T> CallbackDelta<T> delta() {
		// todo: create static instance
		return new CallbackDelta<>();
	}

	static <U> U safeCast(Object previousValue) {
		//noinspection unchecked
		return (U) previousValue;
	}

	sealed interface PutCallback<T, U> extends Callback<T, U> {}

	sealed interface PatchCallback<T, U> extends Callback<T, U> {}

	sealed interface GetCallback<T, U> extends Callback<T, U> {}

	sealed interface IteratorCallback<T, U> extends Callback<T, U> {}

	record CallbackVoid<T>() implements PutCallback<T, Void>, PatchCallback<T, Void>, IteratorCallback<T, Void>, GetCallback<T, Void> {}

	record CallbackPrevious<T>() implements PutCallback<T, @Nullable T> {}

	record CallbackCurrent<T>() implements GetCallback<T, @Nullable T> {}

	record CallbackExists<T>() implements GetCallback<T, Boolean>, IteratorCallback<T, Boolean> {}

	record CallbackDelta<T>() implements PutCallback<T, Delta<T>> {
	}

	record CallbackMulti<M>() implements IteratorCallback<M, List<M>> {}

	record CallbackChanged<T>() implements PutCallback<T, Boolean>, PatchCallback<T, Boolean> {}
}
