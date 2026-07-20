package it.cavallium.rockserver.ratis;

import org.apache.ratis.client.RaftClientConfigKeys;

public final class RatisClasspathPartitionClientProbe {

    private RatisClasspathPartitionClientProbe() {
    }

    public static void run() {
        var properties = RatisClasspathPartitionClient.clientProperties();
        require(
                RaftClientConfigKeys.Rpc.requestTimeout(properties).equals(RatisClasspathPartitionClient.REQUEST_TIMEOUT),
                "Ratis classpath partition client request timeout must be bounded"
        );
        require(
                RaftClientConfigKeys.Rpc.watchRequestTimeout(properties)
                        .equals(RatisClasspathPartitionClient.REQUEST_TIMEOUT),
                "Ratis classpath partition client watch timeout must be bounded"
        );
        require(
                RatisClasspathPartitionClient.retryPolicy().getMaxAttempts()
                        == RatisClasspathPartitionClient.MAX_REQUEST_ATTEMPTS,
                "Ratis classpath partition client retry count must be bounded"
        );
        require(
                RatisClasspathPartitionClient.maxRequestEnvelopeMillis() < AdapterMain.RPC_TIMEOUT.toMillis(),
                "Ratis classpath partition client retry envelope must fit adapter RPC timeout"
        );
        System.out.printf(
                "Ratis classpath partition-client timeout probe OK: requestTimeout=%s attempts=%s envelopeMillis=%s%n",
                RatisClasspathPartitionClient.REQUEST_TIMEOUT,
                RatisClasspathPartitionClient.MAX_REQUEST_ATTEMPTS,
                RatisClasspathPartitionClient.maxRequestEnvelopeMillis()
        );
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
