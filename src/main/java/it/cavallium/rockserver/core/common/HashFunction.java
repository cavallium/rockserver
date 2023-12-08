package it.cavallium.rockserver.core.common;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public interface HashFunction {

	void hash(MemorySegment inputData, MemorySegment hashResult);
}
