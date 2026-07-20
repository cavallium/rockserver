package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class RocksDBSyncAPITest {

	@Test
	void ensureMergeOperatorReturnsExistingVersionWithoutUpload() {
		var api = new RecordingAPI(7L, 3L);

		assertEquals(7L, api.ensureMergeOperator("operator", "Operator", new byte[]{1}));
		assertEquals(1, api.checks);
		assertEquals(0, api.uploads);
	}

	@Test
	void ensureMergeOperatorUploadsWhenHashIsMissing() {
		var jar = new byte[]{1, 2, 3};
		var api = new RecordingAPI(null, 5L);

		assertEquals(5L, api.ensureMergeOperator("operator", "Operator", jar));

		assertEquals(1, api.checks);
		assertEquals(1, api.uploads);
		assertArrayEquals(sha256(jar), api.lastHash);
		assertArrayEquals(jar, api.lastJar);
	}

	private static byte[] sha256(byte[] value) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(value);
		} catch (NoSuchAlgorithmException e) {
			throw new AssertionError(e);
		}
	}

	private static final class RecordingAPI implements RocksDBSyncAPI {
		private final Long existingVersion;
		private final long uploadedVersion;
		private int checks;
		private int uploads;
		private byte[] lastHash;
		private byte[] lastJar;

		private RecordingAPI(Long existingVersion, long uploadedVersion) {
			this.existingVersion = existingVersion;
			this.uploadedVersion = uploadedVersion;
		}

		@Override
		public Long checkMergeOperator(String name, byte[] hash) {
			checks++;
			lastHash = Arrays.copyOf(hash, hash.length);
			return existingVersion;
		}

		@Override
		public long uploadMergeOperator(String name, String className, byte[] jarData) {
			uploads++;
			lastJar = Arrays.copyOf(jarData, jarData.length);
			return uploadedVersion;
		}
	}
}
