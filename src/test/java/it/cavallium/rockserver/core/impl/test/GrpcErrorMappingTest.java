package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Mono;

@Timeout(30)
class GrpcErrorMappingTest {

	private static final String ERROR_PREFIX = "RocksDBError: [uid:";

	private Server server;
	private GrpcConnection client;

	@BeforeEach
	void setUp() throws IOException {
		server = ServerBuilder.forPort(0)
				.addService(new ErrorService())
				.build()
				.start();
		client = GrpcConnection.forHostAndPort("grpc-error-mapping",
				new Utils.HostAndPort("127.0.0.1", server.getPort()));
	}

	@AfterEach
	void tearDown() throws IOException, InterruptedException {
		if (client != null) {
			client.close();
		}
		if (server != null) {
			server.shutdownNow();
			server.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	@Test
	void validRockserverErrorsRetainTheirTypedContract() {
		var error = assertThrows(RocksDBException.class,
				() -> client.getSyncApi().getColumnId("valid"));

		assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
		assertEquals("missing column", error.getMessage());
	}

	@Test
	void readDeadlineExceededRetainsItsTypedContract() {
		var error = assertThrows(RocksDBException.class,
				() -> client.getSyncApi().getColumnId("read-deadline"));

		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
		assertEquals("Deadline exceeded", error.getMessage());
	}

	@Test
	void getErrorWithDeadlineTextIsNotReclassifiedAsReadDeadlineExceeded() {
		var error = assertThrows(RocksDBException.class,
				() -> client.getSyncApi().getColumnId("get-deadline-text"));

		assertEquals(RocksDBErrorType.GET_1, error.getErrorUniqueId());
		assertEquals("Deadline exceeded", error.getMessage());
	}

	@Test
	void updateRetryRetainsItsSpecializedExceptionType() {
		assertThrows(RocksDBRetryException.class,
				() -> client.getSyncApi().getColumnId("retry"));
	}

	@Test
	void truncatedRockserverErrorDescriptionDoesNotCrashTheMapper() {
		var error = assertThrows(StatusRuntimeException.class,
				() -> client.getSyncApi().getColumnId("truncated"));

		assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
		assertEquals(ERROR_PREFIX, error.getStatus().getDescription());
	}

	@Test
	void unknownRockserverErrorCodeDoesNotCrashTheMapper() {
		String description = ERROR_PREFIX + "FUTURE_SERVER_ERROR] unknown";
		var error = assertThrows(StatusRuntimeException.class,
				() -> client.getSyncApi().getColumnId("unknown"));

		assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
		assertEquals(description, error.getStatus().getDescription());
	}

	@Test
	void unrelatedGrpcStatusIsNotReclassified() {
		var error = assertThrows(StatusRuntimeException.class,
				() -> client.getSyncApi().getColumnId("unrelated"));

		assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
		assertEquals("plain grpc error", error.getStatus().getDescription());
	}

	@Test
	void noCacheRangeFailsClearlyAgainstAnIncompatiblePeer() {
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

		assertEquals(RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
		assertEquals("The connected Rockserver does not support RequestType.allInRangeNoCache(); "
				+ "upgrade the peer to version 1.2.8 or newer", error.getMessage());
	}

	private static final class ErrorService extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		@Override
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			return Mono.error(switch (request.getName()) {
				case "valid" -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "COLUMN_NOT_FOUND] missing column")
						.asRuntimeException();
				case "read-deadline" -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "READ_DEADLINE_EXCEEDED] Deadline exceeded")
						.asRuntimeException();
				case "get-deadline-text" -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "GET_1] Deadline exceeded")
						.asRuntimeException();
				case "retry" -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "UPDATE_RETRY] retry")
						.asRuntimeException();
				case "truncated" -> Status.INTERNAL
						.withDescription(ERROR_PREFIX)
						.asRuntimeException();
				case "unknown" -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "FUTURE_SERVER_ERROR] unknown")
						.asRuntimeException();
				default -> Status.INVALID_ARGUMENT
						.withDescription("plain grpc error")
						.asRuntimeException();
			});
		}
	}
}
