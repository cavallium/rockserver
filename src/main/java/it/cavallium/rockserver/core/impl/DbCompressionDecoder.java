package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.config.DatabaseCompression;
import java.util.List;
import org.github.gestalt.config.decoder.Decoder;
import org.github.gestalt.config.decoder.DecoderContext;
import org.github.gestalt.config.decoder.Priority;
import org.github.gestalt.config.entity.ValidationError;
import org.github.gestalt.config.node.ConfigNode;
import org.github.gestalt.config.reflect.TypeCapture;
import org.github.gestalt.config.tag.Tags;
import org.github.gestalt.config.utils.ValidateOf;
import org.rocksdb.CompressionType;

public class DbCompressionDecoder implements Decoder<CompressionType> {

	@Override
	public Priority priority() {
		return Priority.HIGH;
	}

	@Override
	public String name() {
		return "DbCompression";
	}

	@Override
	public boolean canDecode(String path, Tags tags, ConfigNode node, TypeCapture<?> type) {
		return type != null && type.isAssignableFrom(CompressionType.class);
	}

	@Override
	public ValidateOf<CompressionType> decode(String path,
			Tags tags,
			ConfigNode node,
			TypeCapture<?> type,
			DecoderContext decoderContext) {
		try {
			return ValidateOf.validateOf(DatabaseCompression.valueOf(node.getValue().orElseThrow()).compressionType(), List.of());
		} catch (Exception ex) {
			return ValidateOf.inValid(new ValidationError.DecodingNumberFormatException(path, node, name()));
		}
	}
}
