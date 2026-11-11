package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * The child JVM of {@link ProcessHaltTest}: a RocksDB database with the FFM merge operator of the cluster tests, four
 * threads that merge into it so that flushes and compactions call the operator in Java all the time, and after
 * {@code args[1]} ms the halt of a crash point ({@link CrashPoints#halt}) with exit status {@code args[2]}.
 * {@code args[0]} is the database directory.
 */
public final class ProcessHaltMain {

	private ProcessHaltMain() {}

	public static void main(String[] args) throws Exception {
		RocksDB.loadLibrary();
		var options = new Options()
				.setCreateIfMissing(true)
				.setWriteBufferSize(1 << 20)
				.setMaxBackgroundJobs(4)
				.setMergeOperator(new MyStringAppendOperator());
		var db = RocksDB.open(options, args[0]);
		for (int i = 0; i < 4; i++) {
			Thread.ofPlatform().daemon().name("merger-" + i).start(() -> {
				var random = ThreadLocalRandom.current();
				var operand = "x".repeat(256).getBytes();
				try {
					while (true) {
						db.merge(ByteBuffer.allocate(Integer.BYTES).putInt(random.nextInt(20_000)).array(), operand);
					}
				} catch (RocksDBException ex) {
					ex.printStackTrace();
				}
			});
		}
		Thread.sleep(Long.parseLong(args[1]));
		System.out.println("HALTING");
		System.out.flush();
		CrashPoints.halt.accept(Integer.parseInt(args[2]));
	}
}
