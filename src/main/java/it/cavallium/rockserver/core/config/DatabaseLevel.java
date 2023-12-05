package it.cavallium.rockserver.core.config;

public interface DatabaseLevel {

	DatabaseCompression compression();

	DataSize maxDictBytes();

	static String stringify(DatabaseLevel o) {
		return """
      {
              "compression": "%s",
              "max-dict-bytes": "%s"
            }\
      """.formatted(o.compression(), o.maxDictBytes());
	}
}
