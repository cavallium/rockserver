package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.config.DataSize;
import java.util.List;
import org.github.gestalt.config.decoder.Decoder;
import org.github.gestalt.config.decoder.DecoderService;
import org.github.gestalt.config.decoder.Priority;
import org.github.gestalt.config.entity.ValidationError;
import org.github.gestalt.config.node.ConfigNode;
import org.github.gestalt.config.reflect.TypeCapture;
import org.github.gestalt.config.utils.ValidateOf;

class DataSizeDecoder implements Decoder<DataSize> {

	@Override
	public Priority priority() {
		return Priority.LOW;
	}

	@Override
	public String name() {
		return "DataSize";
	}

	@Override
	public boolean matches(TypeCapture klass) {
		return klass.isAssignableFrom(DataSize.class);
	}

	@Override
	public ValidateOf<DataSize> decode(String path, ConfigNode node, TypeCapture type, DecoderService decoderService) {
		try {
			return ValidateOf.validateOf(new DataSize(node.getValue().orElseThrow()), List.of());
		} catch (Exception ex) {
			return ValidateOf.inValid(new ValidationError.DecodingNumberFormatException(path, node, name()));
		}
	}
}
