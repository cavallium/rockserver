module rockserver.core {
	requires rocksdbjni;
	requires net.sourceforge.argparse4j;
	requires inet.ipaddr;
	requires java.logging;
	requires typesafe.config;
	requires org.jetbrains.annotations;
	requires high.scale.lib;
	requires org.github.gestalt.core;
	requires org.github.gestalt.hocon;
	requires it.unimi.dsi.fastutil.core;

	exports it.cavallium.rockserver.core.client;
	exports it.cavallium.rockserver.core.common;
	exports it.cavallium.rockserver.core.config;
	opens it.cavallium.rockserver.core.resources;
	opens it.cavallium.rockserver.core.config to org.github.gestalt.core, org.github.gestalt.hocon;
	exports it.cavallium.rockserver.core.impl.rocksdb;
	exports it.cavallium.rockserver.core.impl;
}