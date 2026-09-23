package it.cavallium.rockserver.core.impl.benchmark;

final class BenchmarkJson {
	private BenchmarkJson() {}

	static void appendJsonString(StringBuilder json, String value) {
		json.append('"');
		for (int index = 0; index < value.length(); index++) {
			char character = value.charAt(index);
			switch (character) {
				case '"' -> json.append("\\\"");
				case '\\' -> json.append("\\\\");
				case '\n' -> json.append("\\n");
				case '\r' -> json.append("\\r");
				case '\t' -> json.append("\\t");
				default -> {
					if (character < 0x20) {
						json.append("\\u%04x".formatted((int) character));
					} else {
						json.append(character);
					}
				}
			}
		}
		json.append('"');
	}
}
