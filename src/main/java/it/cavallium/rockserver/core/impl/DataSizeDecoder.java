package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.config.DataSize;
import java.util.List;
import org.github.gestalt.config.decoder.Decoder;
import org.github.gestalt.config.decoder.DecoderContext;
import org.github.gestalt.config.decoder.Priority;
import org.github.gestalt.config.entity.ValidationError;
import org.github.gestalt.config.node.ConfigNode;
import org.github.gestalt.config.reflect.TypeCapture;
import org.github.gestalt.config.tag.Tags;
import org.github.gestalt.config.utils.ValidateOf;

public class DataSizeDecoder implements Decoder<DataSize> {

	@Override
	public Priority priority() {
		return Priority.LOW;
	}

	@Override
	public String name() {
		return "DataSize";
	}

	@Override
	public boolean canDecode(String path, Tags tags, ConfigNode node, TypeCapture<?> type) {
		return type != null && type.isAssignableFrom(DataSize.class);
	}

	@Override
	public ValidateOf<DataSize> decode(String path,
			Tags tags,
			ConfigNode node,
			TypeCapture<?> type,
			DecoderContext decoderContext) {
		try {
			return ValidateOf.validateOf(new DataSize(node.getValue().orElseThrow()), List.of());
		} catch (Exception ex) {
			return ValidateOf.inValid(new ValidationError.DecodingNumberFormatException(path, node, name()));
		}
	}
}
