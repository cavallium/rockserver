package it.cavallium.rockserver.core.impl.rocksdb;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.rocksdb.RocksObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksLeakDetector {

	private static final Logger LOG = LoggerFactory.getLogger(RocksLeakDetector.class);

	protected static final boolean ENABLE_LEAK_DETECTION = Boolean.parseBoolean(System.getProperty(
			"it.cavallium.rockserver.leakdetection",
			"true"
	));
	public static final Cleaner CLEANER = Cleaner.create();

	public static void register(RocksObject nativeReference, String label, AtomicBoolean owningHandle) {
		if (ENABLE_LEAK_DETECTION) {
			var resourceClass = nativeReference.getClass();
			CLEANER.register(nativeReference, () -> {
				if (owningHandle.get()) {
					LOG.error("Resource leak of type {} with label \"{}\"", resourceClass, label);
				}
			});
		}
	}
}
