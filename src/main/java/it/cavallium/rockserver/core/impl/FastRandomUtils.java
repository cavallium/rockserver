package it.cavallium.rockserver.core.impl;

import java.util.concurrent.ThreadLocalRandom;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

public class FastRandomUtils {

	public static <T> long allocateNewValue(NonBlockingHashMapLong<T> map, T value, long minId, long maxId) {
		long newTransactionId;
		do {
			newTransactionId = getRandomId(minId, maxId);
		} while (map.putIfAbsent(newTransactionId, value) != null);
		return newTransactionId;
	}

	private static long getRandomId(long minId, long maxId) {
		return ThreadLocalRandom.current().nextLong(minId, maxId);
	}
}
