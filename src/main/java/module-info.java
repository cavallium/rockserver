module rockserver.core {
	requires rocksdbjni;
	requires net.sourceforge.argparse4j;
	requires inet.ipaddr;
	requires java.logging;
	requires org.jetbrains.annotations;
	requires high.scale.lib;
	requires org.github.gestalt.core;
	requires org.github.gestalt.hocon;
	requires it.unimi.dsi.fastutil;
	requires io.netty5.buffer;
	requires io.netty5.codec;
	requires io.netty5.codec.http2;
	requires io.netty5.common;
	requires io.netty5.handler;
	requires io.netty5.transport;
	requires io.netty5.transport.classes.io_uring;
	requires io.netty5.transport.io_uring;
	requires io.netty5.transport.unix.common;
	requires io.netty5.codec.http;
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