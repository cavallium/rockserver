package it.cavallium.rockserver.core.common;

import java.util.List;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;

public sealed interface Callback<T> {

	static boolean requiresGettingPreviousValue(PutCallback<?> callback) {
		return callback instanceof CallbackPrevious<?>
				|| callback instanceof CallbackDelta<?>
				|| callback instanceof CallbackChanged;
	}

	static boolean requiresGettingCurrentValue(GetCallback<?> callback) {
		return callback instanceof CallbackCurrent<?>;
	}

	sealed interface PutCallback<T> extends Callback<T> {}

	sealed interface PatchCallback<T> extends Callback<T> {}

	sealed interface GetCallback<T> extends Callback<T> {}

	sealed interface IteratorCallback<T> extends Callback<T> {}

	non-sealed interface CallbackVoid<T> extends PutCallback<T>, PatchCallback<T>, IteratorCallback<T>, GetCallback<T> {}

	non-sealed interface CallbackPrevious<T> extends PutCallback<T> {

		void onPrevious(@Nullable T previous);
	}

	non-sealed interface CallbackCurrent<T> extends GetCallback<T> {

		void onCurrent(@Nullable T previous);
	}

	non-sealed interface CallbackExists<T> extends GetCallback<T>, IteratorCallback<T> {

		void onExists(boolean exists);
	}

	non-sealed interface CallbackDelta<T> extends PutCallback<T> {

		void onSuccess(Delta<T> previous);
	}

	non-sealed interface CallbackMulti<T> extends IteratorCallback<T> {

		void onSuccess(List<Entry<T, T>> elements);
	}

	non-sealed interface CallbackChanged extends PutCallback<Object>, PatchCallback<Object> {

		void onChanged(boolean changed);
	}
}
