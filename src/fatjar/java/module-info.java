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
	requires protobuf.java;
	requires io.grpc.protobuf;
	requires io.grpc.stub;
	requires io.grpc;
	requires jsr305;
	requires com.google.common;
	requires io.grpc.netty;
	requires io.jstach.rainbowgum;
	requires io.jstach.rainbowgum.pattern;
	requires io.netty.common;
	requires proto.google.common.protos;
	requires io.netty.handler;
	requires io.netty.buffer;
	requires io.netty.transport;
	requires io.netty.codec.http;
	requires io.netty.codec;
	requires io.netty.codec.http2;
	requires jdk.unsupported;
    requires io.netty.transport.classes.epoll;

    exports it.cavallium.rockserver.core.client;
	exports it.cavallium.rockserver.core.common;
	exports it.cavallium.rockserver.core.config;
	opens it.cavallium.rockserver.core.resources;
	opens it.cavallium.rockserver.core.config to org.github.gestalt.core, org.github.gestalt.hocon;
	exports it.cavallium.rockserver.core.impl.rocksdb;
	exports it.cavallium.rockserver.core.impl;
}