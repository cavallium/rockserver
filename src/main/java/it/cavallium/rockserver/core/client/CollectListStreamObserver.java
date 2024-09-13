package it.cavallium.rockserver.core.client;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class CollectListStreamObserver<T> extends CompletableFuture<List<T>> implements StreamObserver<T> {

	private final List<T> list;

	public CollectListStreamObserver() {
		this.list = new ArrayList<>();
	}

	public CollectListStreamObserver(int size) {
		this.list = new ArrayList<>(size);
	}

	@Override
	public void onNext(T t) {
		this.list.add(t);
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
