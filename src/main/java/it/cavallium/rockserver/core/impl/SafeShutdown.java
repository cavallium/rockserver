package it.cavallium.rockserver.core.impl;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

public class SafeShutdown implements AutoCloseable {

	private volatile boolean closing;

	private final LongAdder pendingOps = new LongAdder();

	public void beginOp() {
		if (closing) {
			throw new IllegalStateException("Closed");
		}
		pendingOps.increment();
	}

	public void endOp() {
		pendingOps.decrement();
	}

	public void closeAndWait(long timeoutMs) throws TimeoutException {
		this.closing = true;
		waitForExit(timeoutMs);
	}

	public void waitForExit(long timeoutMs) throws TimeoutException {
		try {
			long startMs = System.nanoTime();
			while (pendingOps.sum() > 0 && System.nanoTime() - startMs < (timeoutMs * 1000000L)) {
				//noinspection BusyWait
				Thread.sleep(10);
			}
			if (pendingOps.sum() > 0) {
				throw new TimeoutException();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			closeAndWait(Long.MAX_VALUE);
		} catch (TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isOpen() {
		return !closing;
	}
}
