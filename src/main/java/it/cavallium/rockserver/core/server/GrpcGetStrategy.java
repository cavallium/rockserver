package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.util.Locale;


enum GrpcGetStrategy {
	LEGACY,
	EXACT_HEAP,
	PINNED,
	AUTOMATIC;

	static GrpcGetStrategy configured() {
		String value = System.getProperty("rockserver.grpc.fast-get.strategy", "automatic")
				.strip()
				.toLowerCase(Locale.ROOT);
		return switch (value) {
			case "legacy" -> LEGACY;
			case "exact-heap", "heap" -> EXACT_HEAP;
			case "pinned" -> PINNED;
			case "automatic", "auto" -> AUTOMATIC;
			default -> throw new IllegalArgumentException(
					"Unknown rockserver.grpc.fast-get.strategy: " + value);
		};
	}
}
