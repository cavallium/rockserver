package it.cavallium.rockserver.core.config;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.DataSizeDecoder;
import it.cavallium.rockserver.core.impl.DbCompressionDecoder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.github.gestalt.config.builder.GestaltBuilder;
import org.github.gestalt.config.builder.SourceBuilder;
import org.github.gestalt.config.exceptions.GestaltException;
import org.github.gestalt.config.source.ClassPathConfigSourceBuilder;
import org.github.gestalt.config.source.FileConfigSourceBuilder;

public class ConfigParser {

	private final GestaltBuilder gsb;
	private final List<SourceBuilder<?, ?>> sourceBuilders = new ArrayList<>();

	public ConfigParser() {
		gsb = new GestaltBuilder();
			gsb
					.setTreatMissingArrayIndexAsError(false)
					.setTreatNullValuesInClassAsErrors(false)
					.setTreatMissingValuesAsErrors(false)
					.addDecoder(new DataSizeDecoder())
					.addDecoder(new DbCompressionDecoder())
					.addDefaultConfigLoaders()
					.addDefaultDecoders();
	}

	public static DatabaseConfig parse(Path configPath) {
		var parser = new ConfigParser();
		parser.addSource(configPath);
		return parser.parse();
	}

	public static DatabaseConfig parseDefault() {
		var parser = new ConfigParser();
		return parser.parse();
	}


	public void addSource(Path path) {
		if (path != null) {
			sourceBuilders.add(FileConfigSourceBuilder.builder().setPath(path));
		}
	}

	public DatabaseConfig parse() {
		try {
			gsb.addSource(ClassPathConfigSourceBuilder
					.builder().setResource("it/cavallium/rockserver/core/resources/default.conf").build());
			for (SourceBuilder<?, ?> sourceBuilder : sourceBuilders) {
				gsb.addSource(sourceBuilder.build());
			}
			var gestalt = gsb.build();
			gestalt.loadConfigs();

			return gestalt.getConfig("database", DatabaseConfig.class);
		} catch (GestaltException ex) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, ex);
		}
	}
}
