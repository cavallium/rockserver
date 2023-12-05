package it.cavallium.rockserver.core.config;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public interface GlobalDatabaseConfig {

	boolean spinning();

	boolean checksum();

	boolean useDirectIo();

	boolean allowRocksdbMemoryMapping();

	int maximumOpenFiles();

	boolean optimistic();

	DataSize blockCache();

	DataSize writeBufferManager();

	Path logPath();

	FallbackColumnOptions fallbackColumnOptions();
	NamedColumnOptions[] columnOptions();

	static String stringify(GlobalDatabaseConfig o) {
		return """
        {
            "spinning": %b,
            "checksum": %b,
            "use-direct-io": %b,
            "allow-rocksdb-memory-mapping": %b,
            "maximum-open-files": %d,
            "optimistic": %b,
            "block-cache": "%s",
            "write-buffer-manager": "%s",
            "log-path": "%s",
            "fallback-column-options": %s,
            "column-options": %s
          }\
        """.formatted(o.spinning(),
				o.checksum(),
				o.useDirectIo(),
				o.allowRocksdbMemoryMapping(),
				o.maximumOpenFiles(),
				o.optimistic(),
				o.blockCache(),
				o.writeBufferManager(),
				o.logPath(),
				FallbackColumnOptions.stringify(o.fallbackColumnOptions()),
				Arrays.stream(Objects.requireNonNullElse(o.columnOptions(), new NamedColumnOptions[0]))
						.map(NamedColumnOptions::stringify)
						.collect(Collectors.joining(",", "[", "]"))
		);
	}
}
