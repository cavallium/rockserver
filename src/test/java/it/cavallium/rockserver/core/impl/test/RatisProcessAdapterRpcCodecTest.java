package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcCodec;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcLimits;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcOperation;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcRequest;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcResponse;

import org.junit.jupiter.api.Test;

class RatisProcessAdapterRpcCodecTest {

    @Test
    void rpcPayloadLimitsLeaveDefaultSocketFrameHeadroom() {
        assertEquals(RatisProcessAdapterRpcLimits.MAX_PAYLOAD_BYTES,
                RatisProcessAdapterRpcLimits.requirePayloadBytes(RatisProcessAdapterRpcLimits.MAX_PAYLOAD_BYTES));
        assertEquals(RatisProcessAdapterRpcLimits.MAX_ENCODED_PAYLOAD_CHARACTERS,
                RatisProcessAdapterRpcLimits.requireEncodedPayloadCharacters(
                        RatisProcessAdapterRpcLimits.MAX_ENCODED_PAYLOAD_CHARACTERS));
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcLimits.requirePayloadBytes(
                        RatisProcessAdapterRpcLimits.MAX_PAYLOAD_BYTES + 1));
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcLimits.requireEncodedPayloadCharacters(
                        RatisProcessAdapterRpcLimits.MAX_ENCODED_PAYLOAD_CHARACTERS + 1));
    }

    @Test
    void requestAndResponseModelsRejectOversizedPayloads() {
        var oversized = new byte[RatisProcessAdapterRpcLimits.MAX_PAYLOAD_BYTES + 1];

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcRequest.propose("request-1", "partition-a", oversized));
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcResponse.payload("request-1", oversized));
    }

    @Test
    void requestAndResponseModelsRejectNullFieldsWithExplicitDiagnostics() {
        var nullRequestId = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcRequest.propose(null, "partition-a", new byte[0]));
        var nullPartitionId = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcRequest.propose("request-1", null, new byte[0]));
        var nullOperation = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterRpcRequest(
                        RatisProcessAdapterRpcRequest.CURRENT_PROTOCOL_VERSION,
                        "request-1",
                        "partition-a",
                        null,
                        new byte[0],
                        0,
                        0
                ));
        var nullRequestPayload = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcRequest.propose("request-1", "partition-a", null));
        var nullResponsePayload = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcResponse.payload("request-1", null));
        var nullErrorType = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcResponse.error("request-1", null, "message"));
        var nullMessage = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcResponse.error("request-1", "BAD_REQUEST", null));
        var nullExceptionType = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterRpcException(null, "message"));

        assertEquals("requestId must not be null", nullRequestId.getMessage());
        assertEquals("partitionId must not be null", nullPartitionId.getMessage());
        assertEquals("operation must not be null", nullOperation.getMessage());
        assertEquals("payload must not be null", nullRequestPayload.getMessage());
        assertEquals("payload must not be null", nullResponsePayload.getMessage());
        assertEquals("errorType must not be null", nullErrorType.getMessage());
        assertEquals("message must not be null", nullMessage.getMessage());
        assertEquals("errorType must not be null", nullExceptionType.getMessage());
    }

    @Test
    void codecRejectsNullArgumentsWithExplicitDiagnostics() {
        var nullRequest = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcCodec.encodeRequest(null)
        );
        var nullResponse = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcCodec.encodeResponse(null)
        );
        var nullRequestPayload = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcCodec.decodeRequest(null)
        );
        var nullResponsePayload = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcCodec.decodeResponse(null)
        );

        assertEquals("request must not be null", nullRequest.getMessage());
        assertEquals("response must not be null", nullResponse.getMessage());
        assertEquals("payload must not be null", nullRequestPayload.getMessage());
        assertEquals("payload must not be null", nullResponsePayload.getMessage());
    }

    @Test
    void modelsRejectOversizedOrLineUnsafeTextFields() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcRequest.propose(
                "x".repeat(RatisProcessAdapterRpcLimits.MAX_REQUEST_ID_CHARACTERS + 1),
                "partition-a",
                new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcRequest.propose(
                "request-1",
                "x".repeat(RatisProcessAdapterRpcLimits.MAX_PARTITION_ID_CHARACTERS + 1),
                new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcResponse.error(
                "request-1",
                "x".repeat(RatisProcessAdapterRpcLimits.MAX_ERROR_TYPE_CHARACTERS + 1),
                "message"
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcResponse.error(
                "request-1",
                "BAD_REQUEST",
                "x".repeat(RatisProcessAdapterRpcLimits.MAX_ERROR_MESSAGE_CHARACTERS + 1)
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcRequest.propose(
                "request-\0injected",
                "partition-a",
                new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcRequest.propose(
                "request-\u2028injected",
                "partition-a",
                new byte[0]
        ));
    }

    @Test
    void proposeRequestRoundTripsWithBase64Payload() {
        var request = RatisProcessAdapterRpcRequest.propose(
                "request-1",
                "partition-a",
                new byte[]{1, 2, 3, 4}
        );

        var decoded = RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(request));

        assertEquals(request, decoded);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, decoded.payload());
    }

    @Test
    void readAndCommitOperationsRoundTrip() {
        var linearizable = RatisProcessAdapterRpcRequest.queryLinearizable(
                "request-2",
                "partition-a",
                new byte[]{9}
        );
        var stale = RatisProcessAdapterRpcRequest.queryStale(
                "request-3",
                "partition-a",
                new byte[]{8},
                42
        );
        var local = RatisProcessAdapterRpcRequest.queryLocal(
                "request-4",
                "partition-a",
                new byte[]{7}
        );
        var wait = RatisProcessAdapterRpcRequest.waitForCommit("request-5", "partition-a", 99);
        var reload = RatisProcessAdapterRpcRequest.reloadMetadata("request-6");

        assertEquals(RatisProcessAdapterRpcOperation.QUERY_LINEARIZABLE,
                RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(linearizable)).operation());
        assertEquals(42,
                RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(stale)).minAppliedIndex());
        assertEquals(RatisProcessAdapterRpcOperation.QUERY_LOCAL,
                RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(local)).operation());
        assertEquals(99, RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(wait)).index());
        assertEquals(RatisProcessAdapterRpcOperation.RELOAD_METADATA,
                RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(reload)).operation());
        assertEquals(RatisProcessAdapterRpcRequest.ADAPTER_CONTROL_PARTITION_ID,
                RatisProcessAdapterRpcCodec.decodeRequest(RatisProcessAdapterRpcCodec.encodeRequest(reload)).partitionId());
    }

    @Test
    void responsesRoundTrip() {
        var commit = RatisProcessAdapterRpcResponse.commit("request-1", 7, true);
        var commitWithPayload = RatisProcessAdapterRpcResponse.commit("request-2", 8, true, new byte[]{1, 2});
        var payload = RatisProcessAdapterRpcResponse.payload("request-3", new byte[]{5, 6});
        var error = RatisProcessAdapterRpcResponse.error("request-4", "NOT_LEADER", "leader moved");

        assertEquals(commit, RatisProcessAdapterRpcCodec.decodeResponse(RatisProcessAdapterRpcCodec.encodeResponse(commit)));
        assertEquals(commitWithPayload,
                RatisProcessAdapterRpcCodec.decodeResponse(RatisProcessAdapterRpcCodec.encodeResponse(commitWithPayload)));
        assertEquals(payload, RatisProcessAdapterRpcCodec.decodeResponse(RatisProcessAdapterRpcCodec.encodeResponse(payload)));
        assertEquals(error, RatisProcessAdapterRpcCodec.decodeResponse(RatisProcessAdapterRpcCodec.encodeResponse(error)));
    }

    @Test
    void codecRejectsUnknownDuplicateAndFutureProtocolKeys() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=1
                        requestId=request-1
                        partitionId=partition-a
                        operation=PROPOSE
                        payload=AQID
                        index=0
                        minAppliedIndex=0
                        extra=value
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=1
                        requestId=request-1
                        requestId=request-2
                        partitionId=partition-a
                        operation=PROPOSE
                        payload=AQID
                        index=0
                        minAppliedIndex=0
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeResponse(
                """
                        version=2
                        requestId=request-1
                        success=true
                        payload=
                        commitIndex=0
                        committed=false
                        errorType=
                        message=
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeResponse(
                """
                                version=1
                                requestId=request-1
                                success=yes
                                payload=
                                commitIndex=0
                                committed=false
                                errorType=
                                message=
                        """
        ));
    }

    @Test
    void codecRejectsUnknownKeyBeforeRetainingLargeTail() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                "extra=value\n" + "ignored=\n".repeat(10_000)
        ));

        assertEquals("Unknown Ratis adapter RPC key: extra", failure.getMessage());
    }

    @Test
    void codecRejectsMalformedLineBeforeScanningLargeTail() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                "malformed\n" + "ignored=\n".repeat(10_000)
        ));

        assertEquals("Malformed Ratis adapter RPC line: malformed", failure.getMessage());
    }

    @Test
    void codecRejectsInvalidOperationsWithAdapterDiagnostic() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=1
                        requestId=request-1
                        partitionId=partition-a
                        operation=BROKEN
                        payload=
                        index=0
                        minAppliedIndex=0
                        """
        ));

        assertEquals("Invalid Ratis adapter RPC operation: BROKEN", failure.getMessage());
    }

    @Test
    void codecBoundsKnownFieldsAndUnknownKeyDiagnosticsBeforeRetention() {
        var oversizedValue = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                "requestId=" + "x".repeat(RatisProcessAdapterRpcLimits.MAX_REQUEST_ID_CHARACTERS + 1)
        ));
        var oversizedUnknownKey = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                "x".repeat(10_000) + "=value"
        ));

        assertEquals("Ratis adapter RPC value for key requestId must not exceed "
                + RatisProcessAdapterRpcLimits.MAX_REQUEST_ID_CHARACTERS + " characters", oversizedValue.getMessage());
        assertTrue(oversizedUnknownKey.getMessage().length() < 256);
    }

    @Test
    void codecRejectsDuplicateKeysBeforeRetainingReplacementValues() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                "requestId=request-1\nrequestId="
                        + "x".repeat(RatisProcessAdapterRpcLimits.MAX_REQUEST_ID_CHARACTERS + 1)
        ));

        assertEquals("Duplicate Ratis adapter RPC key: requestId", failure.getMessage());
    }

    @Test
    void codecRejectsNonCanonicalEmptyLines() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                "version=1\n\nrequestId=request-1"
        ));

        assertEquals("Ratis adapter codec payload contains an empty line", failure.getMessage());
    }

    @Test
    void codecRejectsOutOfRangeProtocolVersionsWithAdapterDiagnostic() {
        var failure = assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=2147483648
                        requestId=request-1
                        partitionId=partition-a
                        operation=PROPOSE
                        payload=
                        index=0
                        minAppliedIndex=0
                        """
        ));

        assertEquals("Invalid Ratis adapter RPC protocol version: 2147483648", failure.getMessage());
    }

    @Test
    void codecRejectsNonCanonicalNumbers() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=01
                        requestId=request-1
                        partitionId=partition-a
                        operation=PROPOSE
                        payload=AQID
                        index=0
                        minAppliedIndex=0
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=1
                        requestId=request-1
                        partitionId=partition-a
                        operation=PROPOSE
                        payload=AQID
                        index=00
                        minAppliedIndex=0
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=1
                        requestId=request-1
                        partitionId=partition-a
                        operation=QUERY_STALE
                        payload=AQID
                        index=0
                        minAppliedIndex=+42
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeResponse(
                """
                        version=1
                        requestId=request-1
                        success=true
                        payload=
                        commitIndex=00
                        committed=false
                        errorType=
                        message=
                        """
        ));
    }

    @Test
    void codecRejectsNonCanonicalBase64Payloads() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeRequest(
                """
                        version=1
                        requestId=request-1
                        partitionId=partition-a
                        operation=PROPOSE
                        payload=AQI
                        index=0
                        minAppliedIndex=0
                        """
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcCodec.decodeResponse(
                """
                        version=1
                        requestId=request-1
                        success=true
                        payload=BAU
                        commitIndex=0
                        committed=false
                        errorType=
                        message=
                        """
        ));
    }

    @Test
    void codecRejectsMultiLineWireIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcRequest.propose(
                "request-1\ninjected=true",
                "partition-a",
                new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcRequest.propose(
                "request-1",
                "partition-a\ninjected=true",
                new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterRpcResponse.error(
                "request-1\ninjected=true",
                "NOT_LEADER",
                "leader moved"
        ));
    }

    @Test
    void requestModelRejectsOperationSpecificFieldMismatch() {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterRpcRequest(
                RatisProcessAdapterRpcRequest.CURRENT_PROTOCOL_VERSION,
                "request-1",
                "partition-a",
                RatisProcessAdapterRpcOperation.PROPOSE,
                new byte[0],
                1,
                0
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterRpcRequest(
                RatisProcessAdapterRpcRequest.CURRENT_PROTOCOL_VERSION,
                "request-1",
                "partition-a",
                RatisProcessAdapterRpcOperation.WAIT_FOR_COMMIT,
                new byte[]{1},
                1,
                0
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterRpcRequest(
                RatisProcessAdapterRpcRequest.CURRENT_PROTOCOL_VERSION,
                "request-1",
                "partition-a",
                RatisProcessAdapterRpcOperation.RELOAD_METADATA,
                new byte[0],
                0,
                0
        ));
    }

    @Test
    void responseModelRejectsInconsistentErrorFields() {
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterRpcResponse.commit("request-1", 0, true));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterRpcResponse(
                RatisProcessAdapterRpcResponse.CURRENT_PROTOCOL_VERSION,
                "request-1",
                true,
                new byte[0],
                0,
                false,
                "NOT_LEADER",
                ""
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterRpcResponse(
                RatisProcessAdapterRpcResponse.CURRENT_PROTOCOL_VERSION,
                "request-1",
                false,
                new byte[0],
                0,
                false,
                "",
                "leader moved"
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterRpcResponse(
                RatisProcessAdapterRpcResponse.CURRENT_PROTOCOL_VERSION,
                "request-1",
                false,
                new byte[]{1},
                0,
                false,
                "NOT_LEADER",
                "leader moved"
        ));
    }
}
