package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.io.IOException;
import java.io.InputStream;
import it.cavallium.buffer.Buf;
import java.util.ArrayList;
import java.util.List;


enum MultiGetResponseMarshaller implements MethodDescriptor.Marshaller<MultiGetResponse> {
	INSTANCE;

	@Override
	public InputStream stream(MultiGetResponse response) {
		return new MultiGetInputStream(response.page(), response.owner() != null);
	}

	@Override
	public MultiGetResponse parse(InputStream stream) {
		try {
			var parsed = it.cavallium.rockserver.core.common.api.proto.MultiGetPage.parseFrom(stream);
			List<Buf> values = new ArrayList<>(parsed.getValuesCount());
			for (var value : parsed.getValuesList()) {
				values.add(value.hasValue() ? Buf.wrap(value.getValue().toByteArray()) : null);
			}
			return new MultiGetResponse(new MultiGetPage(values, parsed.getHasMore()), null);
		} catch (IOException error) {
			throw Status.INTERNAL.withDescription("Invalid MultiGet response").withCause(error).asRuntimeException();
		}
	}
}
