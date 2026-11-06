package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.InputStream;
import it.cavallium.buffer.Buf;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.jetbrains.annotations.Nullable;

final class FastGetResponse implements AutoCloseable {

	private static final VarHandle CLOSED;

	static {
		try {
			CLOSED = MethodHandles.lookup().findVarHandle(FastGetResponse.class, "closed", boolean.class);
		} catch (NoSuchFieldException | IllegalAccessException failure) {
			throw new ExceptionInInitializerError(failure);
		}
	}

	private final boolean present;
	private final @Nullable Buf value;
	private final boolean pinned;
	private final @Nullable EmbeddedDB.FastGetResult owner;
	private volatile boolean closed;

	FastGetResponse(boolean present,
			@Nullable Buf value,
			boolean pinned,
			@Nullable EmbeddedDB.FastGetResult owner) {
		if (present != (value != null)) {
			throw new IllegalArgumentException("present Get responses must have a value, including empty values");
		}
		this.present = present;
		this.value = value;
		this.pinned = pinned;
		this.owner = owner;
	}

	InputStream openStream() {
		if (closed) {
			throw new IllegalStateException("Get response has already been closed");
		}
		return new FastGetInputStream(present, value, pinned);
	}

	@Override
	public void close() {
		if (CLOSED.compareAndSet(this, false, true) && owner != null) {
			owner.close();
		}
	}
}
