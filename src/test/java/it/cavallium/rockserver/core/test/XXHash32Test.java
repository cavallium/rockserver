package it.cavallium.rockserver.core.test;

import it.cavallium.rockserver.core.impl.XXHash32;
import it.cavallium.buffer.Buf;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class XXHash32Test {

	@Test
	public void testMemorySegment() {
		var safeXxhash32 = net.jpountz.xxhash.XXHashFactory.safeInstance().hash32();
		var myXxhash32 = XXHash32.getInstance();
		for (int runs = 0; runs < 3; runs++) {
			for (int len = 0; len < 600; len++) {
				byte[] bytes = new byte[len];
				ThreadLocalRandom.current().nextBytes(bytes);
				var hash = safeXxhash32.hash(bytes, 0, bytes.length, Integer.MIN_VALUE);
				var result = Buf.createZeroes(Integer.BYTES);
				myXxhash32.hash(Buf.wrap(bytes), 0, bytes.length, Integer.MIN_VALUE, result);
				var resultInt = result.getInt(0);
				Assertions.assertEquals(hash, resultInt);
			}
		}
	}

	@Test
	public void testBytes() {
		var myXxhash32 = XXHash32.getInstance();
		for (int runs = 0; runs < 3; runs++) {
			for (int len = 0; len < 600; len++) {
				byte[] bytes = new byte[len];
				ThreadLocalRandom.current().nextBytes(bytes);
				var hash = myXxhash32.hash(bytes, 0, bytes.length, Integer.MIN_VALUE);
				var result = Buf.createZeroes(Integer.BYTES);
				myXxhash32.hash(Buf.wrap(bytes), 0, bytes.length, Integer.MIN_VALUE, result);
				var resultInt = result.getInt(0);
				Assertions.assertEquals(hash, resultInt);
			}
		}
	}
}
