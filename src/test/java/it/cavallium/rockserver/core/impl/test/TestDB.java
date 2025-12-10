package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionMethod;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionType;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.cavallium.rockserver.core.server.ThriftServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import reactor.core.publisher.Flux;

public class TestDB implements AutoCloseable {

	private final ConnectionType type;
	private final ConnectionMethod method;

	private final RocksDBAPI api;
	private final EmbeddedConnection db;
	private final Closeable server;

	public TestDB(EmbeddedConnection embeddedConnection, ConnectionConfig config) {
		this.type = config.type();
		this.method = config.method();

		this.db = embeddedConnection;

		try {
			this.server = switch (method) {
				case EMBEDDED -> {
					this.api = new ForceAPIType(type, this.db);
					yield this.db;
				}
				case GRPC -> {
					int port = findFreePort();
					var grpcServer = new GrpcServer(this.db, new InetSocketAddress("127.0.0.1", port));
					grpcServer.start();
					var grpcClient = GrpcConnection.forHostAndPort("test", new HostAndPort("127.0.0.1", port));

					this.api = new ForceAPIType(type, grpcClient);
					yield grpcServer;
				}
				case THRIFT -> {
					int port = findFreePort();
					var thriftServer = new ThriftServer(this.db, "127.0.0.1", port);
					thriftServer.start();
					it.cavallium.rockserver.core.client.ThriftConnection thriftClient;
					try {
						thriftClient = new it.cavallium.rockserver.core.client.ThriftConnection("test", "127.0.0.1", port);
					} catch (org.apache.thrift.TException e) {
						throw new IOException(e);
					}

					this.api = new ForceAPIType(type, thriftClient);
					yield thriftServer;
				}
			};
		} catch (IOException e) {
			try {
				this.db.close();
			} catch (IOException ex) {
			}
			throw new RuntimeException(e);
		}
	}

	private static int findFreePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	public RocksDBAPI getAPI() {
		return api;
	}

	@Override
	public void close() {
		if (api instanceof AutoCloseable autoCloseable && !(api instanceof EmbeddedConnection)) {
			try {
				autoCloseable.close();
			} catch (Exception e) {
				// Ignore exceptions during close
			}
		}
		if (!(server instanceof EmbeddedConnection)) {
			try {
				this.server.close();
			} catch (Exception e) {
				// Ignore exceptions during close
			}
		}
	}

	private static class ForceAPIType implements RocksDBAPI, Closeable {

		private final ConnectionType type;
		private final RocksDBAPI db;

		public ForceAPIType(ConnectionType type, RocksDBAPI db) {
			this.type = type;
			this.db = db;
		}

		@Override
		public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
			return switch (type) {
				case SYNC -> switch (req) {
					case RocksDBAPICommandSingle<?> single -> {
						RS result;
						try {
							result = req.handleSync(db);
						} catch (Throwable ex) {
							yield (RA) CompletableFuture.failedFuture(ex);
						}
						yield (RA) CompletableFuture.completedFuture(result);
					}
					case RocksDBAPICommand.RocksDBAPICommandStream<?> stream -> {
						Stream<?> result;
						try {
							result = (Stream<?>) req.handleSync(db);
						} catch (Throwable ex) {
							yield (RA) Flux.error(ex);
						}
						yield (RA) Flux.fromStream(result);
					}
				};
				case ASYNC -> db.requestAsync(req);
			};
		}

		@Override
		public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
			return switch (type) {
				case SYNC -> db.requestSync(req);
				case ASYNC -> switch (req) {
						case RocksDBAPICommandSingle<?> single -> {
						try {
							var result = (CompletableFuture<?>) req.handleAsync(db);
							yield (RS) result.join();
						} catch (CompletionException ce) {
							var cause = ce.getCause();
							if (cause instanceof RocksDBException exception) {
								throw exception;
							} else if (cause instanceof RuntimeException ex) {
								throw ex;
							} else {
								throw ce;
							}
						}
					}
					case RocksDBAPICommand.RocksDBAPICommandStream<?> stream -> {
						var result = (Flux<?>) req.handleAsync(db);

						yield (RS) result.toStream();
					}
				};
			};
		}

		@Override
		public void close() {
			if (db instanceof AutoCloseable closeable && !(db instanceof EmbeddedConnection)) {
				try {
					closeable.close();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
