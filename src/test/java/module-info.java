module rockserver.core.test {
	requires org.lz4.java;
	requires rockserver.core;
	requires org.junit.jupiter.api;
	requires it.unimi.dsi.fastutil;
    requires com.google.common;
	requires org.slf4j;
	requires org.github.gestalt.core;
	requires org.jetbrains.annotations;
	requires rocksdbjni;
	requires org.reactivestreams;
	opens it.cavallium.rockserver.core.test;
	opens it.cavallium.rockserver.core.impl.test;
}