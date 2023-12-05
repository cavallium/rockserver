package it.cavallium.rockserver.core.config;

public interface DatabaseConfig {

	GlobalDatabaseConfig global();

	static String stringify(DatabaseConfig o) {
		return """
        {
          "global": %s
        }""".formatted(GlobalDatabaseConfig.stringify(o.global()));
	}
}
