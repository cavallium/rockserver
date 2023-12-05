module rockserver.core.test {
	requires org.lz4.java;
	requires rockserver.core;
	requires org.junit.jupiter.api;
	opens it.cavallium.rockserver.core.test;
	opens it.cavallium.rockserver.core.impl.test;
}