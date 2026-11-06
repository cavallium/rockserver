package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;

import org.jetbrains.annotations.Nullable;

record MultiGetResponse(MultiGetPage page, @Nullable EmbeddedDB.PinnedMultiGetResult owner)
		implements AutoCloseable {
	@Override
	public void close() {
		if (owner != null) {
			owner.close();
		}
	}
}
