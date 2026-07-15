package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.impl.RocksDBLongProperty.AggregationMode.DB_WIDE;
import static it.cavallium.rockserver.core.impl.RocksDBLongProperty.AggregationMode.PER_CF;
import static it.cavallium.rockserver.core.impl.RocksDBLongProperty.AggregationMode.SINGLE_CF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.RocksDBLongProperty;
import java.util.EnumSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class RocksDBLongPropertyTest {

	@Test
	void everyPropertyHasAStableNumericRocksDbName() {
		for (var property : RocksDBLongProperty.values()) {
			assertTrue(property.getName().startsWith("rocksdb."), property::name);
			assertEquals(property.getName(), property.toString(), property::name);
			assertTrue(property.isNumeric(), property::name);
			assertFalse(property.isMap(), property::name);
			assertFalse(property.isString(), property::name);
			assertEquals(property.getAggregationMode() == PER_CF,
					property.isDividedByColumnFamily(), property::name);
		}
	}

	@Test
	void dbWidePropertiesAreExplicitlyClassified() {
		assertEquals(EnumSet.of(
				RocksDBLongProperty.NUM_RUNNING_FLUSHES,
				RocksDBLongProperty.NUM_RUNNING_COMPACTIONS,
				RocksDBLongProperty.BACKGROUND_ERRORS,
				RocksDBLongProperty.IS_FILE_DELETIONS_ENABLED,
				RocksDBLongProperty.NUM_SNAPSHOTS,
				RocksDBLongProperty.OLDEST_SNAPSHOT_TIME,
				RocksDBLongProperty.OLDEST_SNAPSHOT_SEQUENCE,
				RocksDBLongProperty.MIN_LOG_NUMBER_TO_KEEP,
				RocksDBLongProperty.MIN_OBSOLETE_SST_NUMBER_TO_KEEP,
				RocksDBLongProperty.ACTUAL_DELAYED_WRITE_RATE,
				RocksDBLongProperty.IS_WRITE_STOPPED,
				RocksDBLongProperty.FILE_READ_DB_OPEN_MICROS
		), propertiesWithMode(DB_WIDE));
	}

	@Test
	void sharedBlockCachePropertiesUseOneColumnFamilyHandle() {
		assertEquals(EnumSet.of(
				RocksDBLongProperty.BLOCK_CACHE_CAPACITY,
				RocksDBLongProperty.BLOCK_CACHE_USAGE,
				RocksDBLongProperty.BLOCK_CACHE_PINNED_USAGE
		), propertiesWithMode(SINGLE_CF));
	}

	@Test
	void allRemainingPropertiesArePerColumnFamily() {
		var classified = Stream.concat(propertiesWithMode(DB_WIDE).stream(), propertiesWithMode(SINGLE_CF).stream())
				.collect(Collectors.toCollection(() -> EnumSet.noneOf(RocksDBLongProperty.class)));
		var expectedPerCf = EnumSet.allOf(RocksDBLongProperty.class);
		expectedPerCf.removeAll(classified);

		assertEquals(expectedPerCf, propertiesWithMode(PER_CF));
	}

	private static EnumSet<RocksDBLongProperty> propertiesWithMode(
			RocksDBLongProperty.AggregationMode mode) {
		return Stream.of(RocksDBLongProperty.values())
				.filter(property -> property.getAggregationMode() == mode)
				.collect(Collectors.toCollection(() -> EnumSet.noneOf(RocksDBLongProperty.class)));
	}
}
