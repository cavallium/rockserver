module rockserver.core {
	requires rocksdbjni;
	requires net.sourceforge.argparse4j;
	requires java.logging;
	requires org.jetbrains.annotations;
	requires high.scale.lib;
	requires org.github.gestalt.core;
	requires org.github.gestalt.hocon;
	requires it.unimi.dsi.fastutil;
	requires org.apache.thrift;
	requires org.slf4j;

	exports it.cavallium.rockserver.core.client;
	exports it.cavallium.rockserver.core.common;
	exports it.cavallium.rockserver.core.config;
	opens it.cavallium.rockserver.core.resources;
	opens it.cavallium.rockserver.core.config to org.github.gestalt.core, org.github.gestalt.hocon;
	exports it.cavallium.rockserver.core.impl.rocksdb;
	exports it.cavallium.rockserver.core.impl;
}