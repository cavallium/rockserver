package it.cavallium.rockserver.core.impl;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

public class SlicingArena implements Arena {

	final Arena arena = Arena.ofConfined();
	final SegmentAllocator slicingAllocator;

	public SlicingArena(long size) {
		slicingAllocator = SegmentAllocator.slicingAllocator(arena.allocate(size));
	}

	public MemorySegment allocate(long byteSize, long byteAlignment) {
		return slicingAllocator.allocate(byteSize, byteAlignment);
	}

	public MemorySegment.Scope scope() {
		return arena.scope();
	}

	public void close() {
		arena.close();
	}

}