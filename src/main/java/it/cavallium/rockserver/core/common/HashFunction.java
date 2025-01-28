package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;

public interface HashFunction {

	void hash(Buf inputData, Buf hashResult);
}
