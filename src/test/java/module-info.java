module rockserver.core.test {
	requires org.lz4.java;
	requires rockserver.core;
	requires com.google.common;
	requires org.slf4j;
	requires org.github.gestalt.core;
	requires rocksdbjni;
	requires org.reactivestreams;
	requires it.cavallium.datagen;
	requires it.unimi.dsi.fastutil;
	requires org.jetbrains.annotations;
	requires org.junit.jupiter.params;
	requires org.apache.httpcomponents.client5.httpclient5;
	requires io.grpc;
	requires org.checkerframework.checker.qual;
	requires reactor.core;
	requires org.apache.thrift;
	requires jdk.unsupported;
	requires reactor.test;
	requires org.mockito;
	opens it.cavallium.rockserver.core.test;
	opens it.cavallium.rockserver.core.impl.test;
}