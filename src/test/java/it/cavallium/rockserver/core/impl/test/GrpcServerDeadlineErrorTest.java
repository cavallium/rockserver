package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;

@Timeout(30)
class GrpcServerDeadlineErrorTest {

	@Test
	void readDeadlineExceededSurvivesARealGrpcServerRoundTrip() throws Exception {
		var backend = new DeadlineBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var error = assertThrows(RocksDBException.class, () -> {
					try (var range = client.getSyncApi().getRange(0,
							0,
							null,
							null,
							false,
							RequestType.allInRangeNoCache(),
							1_000)) {
						range.toList();
					}
				});

				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
				assertEquals("Deadline exceeded", error.getMessage());
			}
		}
	}

	private static final class DeadlineBackendConnection implements RocksDBConnection {

		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {};
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?>) {
					return (RA) Flux.error(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"Deadline exceeded"));
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://deadline-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi() {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi() {
			return asyncApi;
		}

		@Override
		public void close() {
		}
	}
}
