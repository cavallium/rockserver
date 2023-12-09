package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.Callback.CallbackPreviousPresence;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public sealed interface Callback<METHOD_DATA_TYPE, RESULT_TYPE> {

	static boolean requiresGettingPreviousValue(PutCallback<?, ?> callback) {
		return callback instanceof CallbackPrevious<?>
				|| callback instanceof CallbackDelta<?>
				|| callback instanceof CallbackChanged;
	}

	static boolean requiresGettingPreviousPresence(PutCallback<?, ?> callback) {
		return callback instanceof Callback.CallbackPreviousPresence<?>;
	}

	static boolean requiresGettingCurrentValue(GetCallback<?, ?> callback) {
		return callback instanceof CallbackCurrent<?>;
	}

	static <U> U safeCast(Object previousValue) {
		//noinspection unchecked
		return (U) previousValue;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackPrevious<T> previous() {
		return (CallbackPrevious<T>) CallbackPrevious.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackCurrent<T> current() {
		return (CallbackCurrent<T>) CallbackCurrent.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackDelta<T> delta() {
		return (CallbackDelta<T>) CallbackDelta.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackExists<T> exists() {
		return (CallbackExists<T>) CallbackExists.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackMulti<T> multi() {
		return (CallbackMulti<T>) CallbackMulti.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackChanged<T> changed() {
		return (CallbackChanged<T>) CallbackChanged.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackPreviousPresence<T> previousPresence() {
		return (CallbackPreviousPresence<T>) CallbackPreviousPresence.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> CallbackVoid<T> none() {
		return (CallbackVoid<T>) CallbackVoid.INSTANCE;
	}

	sealed interface PutCallback<T, U> extends Callback<T, U> {}

	sealed interface PatchCallback<T, U> extends Callback<T, U> {}

	sealed interface GetCallback<T, U> extends Callback<T, U> {}

	sealed interface IteratorCallback<T, U> extends Callback<T, U> {}

	record CallbackVoid<T>() implements PutCallback<T, Void>, PatchCallback<T, Void>, IteratorCallback<T, Void>, GetCallback<T, Void> {

		private static final CallbackVoid<Object> INSTANCE = new CallbackVoid<>();
	}

	record CallbackPrevious<T>() implements PutCallback<T, @Nullable T> {

		private static final CallbackPrevious<Object> INSTANCE = new CallbackPrevious<>();
	}

	record CallbackCurrent<T>() implements GetCallback<T, @Nullable T> {

		private static final CallbackCurrent<Object> INSTANCE = new CallbackCurrent<>();
	}

	record CallbackExists<T>() implements GetCallback<T, Boolean>, IteratorCallback<T, Boolean> {

		private static final CallbackExists<Object> INSTANCE = new CallbackExists<>();
	}

	record CallbackDelta<T>() implements PutCallback<T, Delta<T>> {

		private static final CallbackDelta<Object> INSTANCE = new CallbackDelta<>();
	}

	record CallbackMulti<M>() implements IteratorCallback<M, List<M>> {

		private static final CallbackMulti<Object> INSTANCE = new CallbackMulti<>();
	}

	record CallbackChanged<T>() implements PutCallback<T, Boolean>, PatchCallback<T, Boolean> {

		private static final CallbackChanged<Object> INSTANCE = new CallbackChanged<>();
	}

	record CallbackPreviousPresence<T>() implements PutCallback<T, Boolean>, PatchCallback<T, Boolean> {

		private static final CallbackPreviousPresence<Object> INSTANCE = new CallbackPreviousPresence<>();
	}
}
