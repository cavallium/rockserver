package it.cavallium.rockserver.core.client;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

class CollectListMappedStreamObserver<T, U> extends CompletableFuture<List<U>> implements StreamObserver<T> {

	private final Function<T, U> mapper;
	private final List<U> list;

	public CollectListMappedStreamObserver(Function<T, U> mapper) {
		this.mapper = mapper;
		this.list = new ArrayList<>();
	}

	public CollectListMappedStreamObserver(Function<T, U> mapper, int size) {
		this.mapper = mapper;
		this.list = new ArrayList<>(size);
	}

	@Override
	public void onNext(T t) {
		this.list.add(mapper.apply(t));
	}

	@Override
	public void onError(Throwable throwable) {
		this.completeExceptionally(throwable);
	}

	@Override
	public void onCompleted() {
		this.complete(this.list);
	}
}
