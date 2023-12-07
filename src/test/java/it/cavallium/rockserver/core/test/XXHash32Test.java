package it.cavallium.rockserver.core.test;

import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.cavallium.rockserver.core.impl.XXHash32;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.ValueLayout.OfByte;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class XXHash32Test {

	public static void main(String[] args) {
		new XXHash32Test().test();
	}

	@Test
	public void test() {
		var safeXxhash32 = net.jpountz.xxhash.XXHashFactory.safeInstance().hash32();
		var myXxhash32 = XXHash32.getInstance();
		for (int runs = 0; runs < 3; runs++) {
			for (int len = 0; len < 600; len++) {
				byte[] bytes = new byte[len];
				ThreadLocalRandom.current().nextBytes(bytes);
				var hash = safeXxhash32.hash(bytes, 0, bytes.length, Integer.MIN_VALUE);
				var a = Arena.global();
				var result = myXxhash32.hash(a, a.allocateArray(OfByte.JAVA_BYTE, bytes), 0, bytes.length, Integer.MIN_VALUE);
				var resultInt = result.get(ColumnInstance.BIG_ENDIAN_INT, 0);
				Assertions.assertEquals(hash, resultInt);
			}
		}
	}
}
