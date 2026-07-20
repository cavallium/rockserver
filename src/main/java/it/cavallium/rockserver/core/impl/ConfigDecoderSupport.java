package it.cavallium.rockserver.core.impl;

import org.github.gestalt.config.entity.ValidationError;
import org.github.gestalt.config.node.ConfigNode;
import org.github.gestalt.config.node.NodeType;
import org.github.gestalt.config.utils.GResultOf;

final class ConfigDecoderSupport {

	private ConfigDecoderSupport() {
	}

	static GResultOf<String> leafValue(String path, ConfigNode node, String decoderName) {
		if (node == null || node.getNodeType() != NodeType.LEAF) {
			return GResultOf.errors(new ValidationError.DecodingExpectedLeafNodeType(path, node, decoderName));
		}
		return node.getValue()
				.map(GResultOf::result)
				.orElseGet(() -> GResultOf.errors(new ValidationError.DecodingLeafMissingValue(path, decoderName)));
	}
}
