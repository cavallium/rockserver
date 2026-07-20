package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.config.DataSize;
import it.cavallium.rockserver.core.impl.DataSizeDecoder;
import it.cavallium.rockserver.core.impl.DbCompressionDecoder;

import org.github.gestalt.config.entity.ValidationError;
import org.github.gestalt.config.node.LeafNode;
import org.github.gestalt.config.reflect.TypeCapture;
import org.github.gestalt.config.tag.Tags;
import org.github.gestalt.config.utils.GResultOf;
import org.junit.jupiter.api.Test;
import org.rocksdb.CompressionType;

class ConfigDecoderTest {

	@Test
	void dataSizeDecoderDecodesValidValue() {
		var result = new DataSizeDecoder().decode(
				"rocks.size",
				Tags.of(),
				new LeafNode("64MiB"),
				TypeCapture.of(DataSize.class),
				null
		);

		assertFalse(result.hasErrors());
		assertEquals(64L * 1024L * 1024L, result.results().longValue());
	}

	@Test
	void dataSizeDecoderReportsMissingLeafValue() {
		var result = new DataSizeDecoder().decode(
				"rocks.size",
				Tags.of(),
				new LeafNode(null),
				TypeCapture.of(DataSize.class),
				null
		);

		assertOnlyError(result, ValidationError.DecodingLeafMissingValue.class);
	}

	@Test
	void dataSizeDecoderReportsParsingFailureAsDecodeException() {
		var result = new DataSizeDecoder().decode(
				"rocks.size",
				Tags.of(),
				new LeafNode("12b"),
				TypeCapture.of(DataSize.class),
				null
		);

		assertOnlyError(result, ValidationError.ErrorDecodingException.class);
	}

	@Test
	void dataSizeDecoderReportsExpectedLeafForNullNode() {
		var result = new DataSizeDecoder().decode(
				"rocks.size",
				Tags.of(),
				null,
				TypeCapture.of(DataSize.class),
				null
		);

		assertOnlyError(result, ValidationError.DecodingExpectedLeafNodeType.class);
	}

	@Test
	void compressionDecoderDecodesValidValue() {
		var result = new DbCompressionDecoder().decode(
				"rocks.compression",
				Tags.of(),
				new LeafNode("ZSTD"),
				TypeCapture.of(CompressionType.class),
				null
		);

		assertFalse(result.hasErrors());
		assertEquals(CompressionType.ZSTD_COMPRESSION, result.results());
	}

	@Test
	void compressionDecoderReportsMissingLeafValue() {
		var result = new DbCompressionDecoder().decode(
				"rocks.compression",
				Tags.of(),
				new LeafNode(null),
				TypeCapture.of(CompressionType.class),
				null
		);

		assertOnlyError(result, ValidationError.DecodingLeafMissingValue.class);
	}

	@Test
	void compressionDecoderReportsUnknownEnumValue() {
		var result = new DbCompressionDecoder().decode(
				"rocks.compression",
				Tags.of(),
				new LeafNode("NOPE"),
				TypeCapture.of(CompressionType.class),
				null
		);

		assertOnlyError(result, ValidationError.EnumValueNotFound.class);
	}

	private static <T extends ValidationError> void assertOnlyError(GResultOf<?> result, Class<T> errorType) {
		assertTrue(result.hasErrors());
		assertEquals(1, result.getErrors().size());
		assertInstanceOf(errorType, result.getErrors().getFirst());
	}
}
