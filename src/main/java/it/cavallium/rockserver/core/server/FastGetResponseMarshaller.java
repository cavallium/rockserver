package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.io.IOException;
import java.io.InputStream;
import it.cavallium.buffer.Buf;


enum FastGetResponseMarshaller implements MethodDescriptor.Marshaller<FastGetResponse> {
	INSTANCE;

	@Override
	public InputStream stream(FastGetResponse value) {
		return value.openStream();
	}

	@Override
	public FastGetResponse parse(InputStream stream) {
		try {
			GetResponse parsed = GetResponse.parseFrom(stream);
			return parsed.hasValue()
					? new FastGetResponse(true, Buf.wrap(parsed.getValue().toByteArray()), false, null)
					: new FastGetResponse(false, null, false, null);
		} catch (IOException exception) {
			throw Status.INTERNAL.withDescription("Invalid Get response").withCause(exception).asRuntimeException();
		}
	}
}
