package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntList;
import it.cavallium.buffer.Buf;

public class EmbeddedDBVarKeysWithoutValueTest extends EmbeddedDBTest {

	protected Keys getKeyI(int i) {
		return new Keys(new Buf[] {
				toBufSimple(1, 2, 3),
				toBufSimple(8, 2, 5, 1, 7, i)
		});
	}

	protected Keys getNotFoundKeyI(int i) {
		return new Keys(new Buf[] {
				toBufSimple(1, 2, 3),
				toBufSimple(8, 2, 5, 1, 0, i)
		});
	}

	protected Keys getKey2() {
		return new Keys(new Buf[] {
				toBufSimple(1, 2, 3),
				toBufSimple(6, 7, 7)
		});
	}

	protected Keys getCollidingKey1() {
		return new Keys(new Buf[] {
				toBufSimple(1, 2, 3),
				toBufSimple(6, 7, -48)
		});
	}

	protected Keys getKey1() {
		return new Keys(new Buf[] {
				toBufSimple(1, 2, 3),
				toBufSimple(6, 7, 8)
		});
	}

	@Override
	protected IntList getSchemaFixedKeys() {
		return IntList.of();
	}

	@Override
	protected Buf getValue1() {
		return emptyBuf();
	}

	@Override
	protected Buf getValue2() {
		return emptyBuf();
	}

	@Override
	protected Buf getValueI(int i) {
		return emptyBuf();
	}

	@Override
	protected boolean getHasValues() {
		return false;
	}

	@Override
	protected Buf getBigValue() {
		return emptyBuf();
	}
}
