package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;

/** Node-local fenced bootstrap on an already registered anchor; fresh catalogue/source authority stays server-owned. */
public final class RatisTopologyLearnerJoinRpc {
    public static final int KIND=108,MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+RatisTopologyJoinAuthority.REQUEST_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+32+NativeTopologyLearnerRegistration.MAX_BYTES;
    private RatisTopologyLearnerJoinRpc(){}
    public static byte[] request(RatisTopologyJoinAuthority.Request request){if(request.purpose()!=RatisTopologyJoinAuthority.Purpose.REGISTER)throw new IllegalArgumentException("Expected learner registration");return ScopedTransactionRpc.frame(KIND,RatisTopologyJoinAuthority.request(request));}
    public static RatisTopologyJoinAuthority.Request decode(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Invalid learner join kind");return RatisTopologyJoinAuthority.decodeRequest(packet.body());}
    public static byte[] reply(RatisTopologyJoinAuthority.Request request,NativeTopologyLearnerRegistration registration){require(request,registration);byte[] bytes=registration.encode(),digest=OperatorUpload.sha256(request(request));var out=java.nio.ByteBuffer.allocate(32+bytes.length).put(digest).put(bytes);return ScopedTransactionRpc.frame(KIND,out.array());}
    public static NativeTopologyLearnerRegistration reply(byte[] bytes,RatisTopologyJoinAuthority.Request request){var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=KIND || packet.body().length<32)throw new IllegalArgumentException("Invalid learner join reply");if(!Arrays.equals(Arrays.copyOfRange(packet.body(),0,32),OperatorUpload.sha256(request(request))))throw new IllegalArgumentException("Crossed learner join reply");var registration=NativeTopologyLearnerRegistration.decode(Arrays.copyOfRange(packet.body(),32,packet.body().length));require(request,registration);return registration;}
    private static void require(RatisTopologyJoinAuthority.Request request,NativeTopologyLearnerRegistration registration){var header=registration.definition().header();if(!request.cluster().equals(header.clusterId()) || !request.database().equals(header.databaseId()) || !request.domain().equals(header.domainId()) || !request.intent().equals(header.intentId()) || !request.receiver().equals(registration.receiver().replicaId()) || !Arrays.equals(request.digest(),header.definitionDigest()))throw new IllegalArgumentException("Learner registration differs from request");}
}
