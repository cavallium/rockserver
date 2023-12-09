package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegmentSimple;

import it.unimi.dsi.fastutil.ints.IntList;
import java.lang.foreign.MemorySegment;

public class EmbeddedDBVarKeysWithValueTest extends EmbeddedDBTest {

	protected MemorySegment[] getKeyI(int i) {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 8, 2, 5, 1, 7, i)
		};
	}

	protected MemorySegment[] getNotFoundKeyI(int i) {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 8, 2, 5, 1, 0, i)
		};
	}

	protected MemorySegment[] getKey2() {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, 7)
		};
	}

	protected MemorySegment[] getCollidingKey1() {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, -48)
		};
	}

	protected MemorySegment[] getKey1() {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, 8)
		};
	}

	@Override
	protected IntList getSchemaFixedKeys() {
		return IntList.of();
	}
}
