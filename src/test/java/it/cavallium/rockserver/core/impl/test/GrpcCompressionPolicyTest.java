package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

class GrpcCompressionPolicyTest {

	@Test
	void disablesCompressionForEveryLoopbackAndUnixTransport() throws Exception {
		assertFalse(GrpcServer.shouldCompressGrpcResponse(
				new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5333)));
		assertFalse(GrpcServer.shouldCompressGrpcResponse(
				new InetSocketAddress(InetAddress.getByName("127.42.17.9"), 5333)));
		assertFalse(GrpcServer.shouldCompressGrpcResponse(
				new InetSocketAddress(InetAddress.getByName("::1"), 5333)));
		assertFalse(GrpcServer.shouldCompressGrpcResponse(
				InetSocketAddress.createUnresolved("localhost", 5333)));
		assertFalse(GrpcServer.shouldCompressGrpcResponse(
				new DomainSocketAddress("/tmp/rockserver-grpc-test.sock")));
	}

	@Test
	void keepsCompressionForRemoteAndUnknownTransports() throws Exception {
		assertTrue(GrpcServer.shouldCompressGrpcResponse(
				new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 5333)));
		assertTrue(GrpcServer.shouldCompressGrpcResponse(null));
	}
}
