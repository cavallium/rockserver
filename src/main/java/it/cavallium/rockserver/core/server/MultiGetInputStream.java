package it.cavallium.rockserver.core.server;


import io.grpc.Drainable;
import io.grpc.KnownLength;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

/** Only framing is allocated; value bytes are borrowed until sendMessage returns. */
final class MultiGetInputStream extends InputStream implements Drainable, KnownLength {
	private final MultiGetPage page;
	private final boolean pinned;
	private int remaining;
	private int slot;
	private InputStream current = InputStream.nullInputStream();
	private @Nullable FastGetInputStream valueStream;

	MultiGetInputStream(MultiGetPage page, boolean pinned) {
		this.page = page;
		this.pinned = pinned;
		long size = page.hasMore() ? 2L : 0L;
		for (var value : page.values()) {
			long inner = value == null ? 0L : 1L + FastGetInputStream.varintSize(value.size()) + value.size();
			if (inner > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("MultiGet value exceeds the gRPC message length limit");
			}
			size += 1L + FastGetInputStream.varintSize((int) inner) + inner;
		}
		if (size > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("MultiGet response exceeds the gRPC message length limit: " + size);
		}
		remaining = (int) size;
	}

	private boolean advance() {
		if (valueStream != null) {
			current = valueStream;
			valueStream = null;
			return true;
		}
		if (slot < page.values().size()) {
			var value = page.values().get(slot++);
			valueStream = new FastGetInputStream(value != null, value, pinned);
			current = new ByteArrayInputStream(
					FastGetInputStream.protobufBytesPrefix(valueStream.available()));
			return true;
		}
		if (slot == page.values().size() && page.hasMore()) {
			slot++;
			current = new ByteArrayInputStream(new byte[]{0x10, 0x01});
			return true;
		}
		return false;
	}

	@Override
	public int read() throws IOException {
		int result;
		while ((result = current.read()) < 0) {
			if (!advance()) {
				return -1;
			}
		}
		remaining--;
		return result;
	}

	@Override
	public int read(byte[] target, int offset, int length) throws IOException {
		Objects.checkFromIndexSize(offset, length, target.length);
		if (length == 0) {
			return 0;
		}
		int count;
		while ((count = current.read(target, offset, length)) < 0) {
			if (!advance()) {
				return -1;
			}
		}
		remaining -= count;
		return count;
	}

	@Override
	public int drainTo(OutputStream target) throws IOException {
		int written = 0;
		do {
			int count = current instanceof Drainable drainable
					? drainable.drainTo(target) : Math.toIntExact(current.transferTo(target));
			remaining -= count;
			written += count;
		} while (advance());
		return written;
	}

	@Override
	public int available() {
		return remaining;
	}
}
