package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegmentSimple;

import it.cavallium.rockserver.core.common.ColumnHashType;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.MemorySegment;

public class EmbeddedDBFixedWithoutValueTest extends EmbeddedDBTest {

	@Override
	protected boolean getHasValues() {
		return false;
	}

	@Override
	protected MemorySegment[] getKeyI(int i) {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 7),
				toMemorySegmentSimple(arena, i)
		};
	}

	@Override
	protected MemorySegment[] getNotFoundKeyI(int i) {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 5, 6),
				toMemorySegmentSimple(arena, i)
		};
	}

	@Override
	protected MemorySegment[] getKey1() {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 3)
		};
	}

	@Override
	protected MemorySegment[] getKey2() {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 4)
		};
	}

	@Override
	protected MemorySegment[] getCollidingKey1() {
		return new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 5)
		};
	}

	@Override
	protected ObjectList<ColumnHashType> getSchemaVarKeys() {
		return ObjectList.of();
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
	protected MemorySegment getBigValue() {
		return MemorySegment.NULL;
	}
}
