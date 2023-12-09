package it.cavallium.rockserver.core.impl.test;

import java.lang.foreign.MemorySegment;

public class EmbeddedDBFixedAndVarKeysWithoutValueTest extends EmbeddedDBTest {

	@Override
	protected MemorySegment getValue1() {
		return MemorySegment.NULL;
	}

	@Override
	protected MemorySegment getValue2() {
		return MemorySegment.NULL;
	}

	@Override
	protected MemorySegment getValueI(int i) {
		return MemorySegment.NULL;
	}

	@Override
	protected boolean getHasValues() {
		return false;
	}

	@Override
	protected MemorySegment getBigValue() {
		return MemorySegment.NULL;
	}
}
