package it.cavallium.rockserver.core.impl;

import org.jetbrains.annotations.Nullable;

public interface InternalConnection {

	RWScheduler getScheduler();

	/** Returns the local database when this connection can expose embedded-only operations. */
	default @Nullable EmbeddedDB getEmbeddedDB() {
		return this instanceof EmbeddedDB embeddedDB ? embeddedDB : null;
	}

}
