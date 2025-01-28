package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;

import it.cavallium.buffer.Buf;

public class EmbeddedDBFixedAndVarKeysWithoutValueTest extends EmbeddedDBTest {

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
