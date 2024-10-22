package it.cavallium.rockserver.core.common;

import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public sealed interface RequestType<METHOD_DATA_TYPE, RESULT_TYPE> {

	@SuppressWarnings("rawtypes")
	enum RequestTypeId  {
		NOTHING(new RequestNothing()),
		PREVIOUS(new RequestPrevious()),
		CURRENT(new RequestCurrent()),
		FOR_UPDATE(new RequestForUpdate()),
		EXISTS(new RequestExists()),
		DELTA(new RequestDelta()),
		MULTI(new RequestMulti()),
		CHANGED(new RequestChanged()),
		PREVIOUS_PRESENCE(new RequestPreviousPresence()),
		FIRST_AND_LAST(new RequestGetFirstAndLast()),
		ALL_IN_RANGE(new RequestGetAllInRange());

		private final RequestType requestType;

		RequestTypeId(RequestType requestType) {
			this.requestType = requestType;
		}

		public RequestType<?, ?> getRequestType() {
			return requestType;
		}
	}

	RequestTypeId getRequestTypeId();

	static boolean requiresGettingPreviousValue(RequestPut<?, ?> requestType) {
		return requestType instanceof RequestType.RequestPrevious<?>
				|| requestType instanceof RequestType.RequestDelta<?>
				|| requestType instanceof RequestType.RequestChanged;
	}

	static boolean requiresGettingPreviousPresence(RequestPut<?, ?> requestType) {
		return requestType instanceof RequestType.RequestPreviousPresence<?>;
	}

	static boolean requiresGettingCurrentValue(RequestGet<?, ?> requestType) {
		return requestType instanceof RequestType.RequestCurrent<?>
				|| requestType instanceof RequestType.RequestForUpdate<?>;
	}

	static <U> U safeCast(Object previousValue) {
		//noinspection unchecked
		return (U) previousValue;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestPrevious<T> previous() {
		return (RequestPrevious<T>) RequestPrevious.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestCurrent<T> current() {
		return (RequestCurrent<T>) RequestCurrent.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestForUpdate<T> forUpdate() {
		return (RequestForUpdate<T>) RequestForUpdate.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestDelta<T> delta() {
		return (RequestDelta<T>) RequestDelta.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestExists<T> exists() {
		return (RequestExists<T>) RequestExists.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestMulti<T> multi() {
		return (RequestMulti<T>) RequestMulti.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestChanged<T> changed() {
		return (RequestChanged<T>) RequestChanged.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestPreviousPresence<T> previousPresence() {
		return (RequestPreviousPresence<T>) RequestPreviousPresence.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestGetFirstAndLast<T> firstAndLast() {
		return (RequestGetFirstAndLast<T>) RequestGetFirstAndLast.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestGetAllInRange<T> allInRange() {
		return (RequestGetAllInRange<T>) RequestGetAllInRange.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	static <T> RequestNothing<T> none() {
		return (RequestNothing<T>) RequestNothing.INSTANCE;
	}

	sealed interface RequestPut<T, U> extends RequestType<T, U> {}

	sealed interface RequestPatch<T, U> extends RequestType<T, U> {}

	sealed interface RequestGet<T, U> extends RequestType<T, U> {}

	sealed interface RequestReduceRange<T, U> extends RequestType<T, U> {}

	sealed interface RequestGetRange<T, U> extends RequestType<T, U> {}

	sealed interface RequestIterate<T, U> extends RequestType<T, U> {}

	record RequestNothing<T>() implements RequestPut<T, Void>, RequestPatch<T, Void>, RequestIterate<T, Void>,
			RequestGet<T, Void> {

		private static final RequestNothing<Object> INSTANCE = new RequestNothing<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.NOTHING;
		}
	}

	record RequestPrevious<T>() implements RequestPut<T, @Nullable T> {

		private static final RequestPrevious<Object> INSTANCE = new RequestPrevious<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.PREVIOUS;
		}
	}

	record RequestCurrent<T>() implements RequestGet<T, @Nullable T> {

		private static final RequestCurrent<Object> INSTANCE = new RequestCurrent<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.CURRENT;
		}
	}

	record RequestForUpdate<T>() implements RequestGet<T,  @NotNull UpdateContext<@Nullable T>> {

		private static final RequestForUpdate<Object> INSTANCE = new RequestForUpdate<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.FOR_UPDATE;
		}
	}

	record RequestExists<T>() implements RequestGet<T, Boolean>, RequestIterate<T, Boolean> {

		private static final RequestExists<Object> INSTANCE = new RequestExists<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.EXISTS;
		}
	}

	record RequestDelta<T>() implements RequestPut<T, Delta<T>> {

		private static final RequestDelta<Object> INSTANCE = new RequestDelta<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.DELTA;
		}
	}

	record RequestMulti<M>() implements RequestIterate<M, List<M>> {

		private static final RequestMulti<Object> INSTANCE = new RequestMulti<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.MULTI;
		}
	}

	record RequestChanged<T>() implements RequestPut<T, Boolean>, RequestPatch<T, Boolean> {

		private static final RequestChanged<Object> INSTANCE = new RequestChanged<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.CHANGED;
		}
	}

	record RequestPreviousPresence<T>() implements RequestPut<T, Boolean>, RequestPatch<T, Boolean> {

		private static final RequestPreviousPresence<Object> INSTANCE = new RequestPreviousPresence<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.PREVIOUS_PRESENCE;
		}
	}

	record RequestGetFirstAndLast<T>() implements RequestReduceRange<T, FirstAndLast<T>> {

		private static final RequestGetFirstAndLast<Object> INSTANCE = new RequestGetFirstAndLast<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.FIRST_AND_LAST;
		}
	}

	record RequestGetAllInRange<T>() implements RequestGetRange<T, T> {

		private static final RequestGetAllInRange<Object> INSTANCE = new RequestGetAllInRange<>();

		@Override
		public RequestTypeId getRequestTypeId() {
			return RequestTypeId.ALL_IN_RANGE;
		}
	}
}
