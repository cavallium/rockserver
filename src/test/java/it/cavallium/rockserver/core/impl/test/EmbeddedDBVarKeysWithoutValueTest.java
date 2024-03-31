package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegmentSimple;

import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntList;
import java.lang.foreign.MemorySegment;

public class EmbeddedDBVarKeysWithoutValueTest extends EmbeddedDBTest {

	protected Keys getKeyI(int i) {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 8, 2, 5, 1, 7, i)
		});
	}

	protected Keys getNotFoundKeyI(int i) {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 8, 2, 5, 1, 0, i)
		});
	}

	protected Keys getKey2() {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, 7)
		});
	}

	protected Keys getCollidingKey1() {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, -48)
		});
	}

	protected Keys getKey1() {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, 8)
		});
	}

	@Override
	protected IntList getSchemaFixedKeys() {
		return IntList.of();
	}

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
