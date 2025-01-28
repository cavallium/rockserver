package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.cavallium.buffer.Buf;

public class EmbeddedDBFixedWithoutValueTest extends EmbeddedDBTest {

	@Override
	protected boolean getHasValues() {
		return false;
	}

	@Override
	protected Keys getKeyI(int i) {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 7),
				toBufSimple(i)
		});
	}

	@Override
	protected Keys getNotFoundKeyI(int i) {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(5, 6),
				toBufSimple(i)
		});
	}

	@Override
	protected Keys getKey1() {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(3)
		});
	}

	@Override
	protected Keys getKey2() {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(4)
		});
	}

	@Override
	protected Keys getCollidingKey1() {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(5)
		});
	}

	@Override
	protected ObjectList<ColumnHashType> getSchemaVarKeys() {
		return ObjectList.of();
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
	protected Buf getBigValue() {
		return emptyBuf();
	}
}
