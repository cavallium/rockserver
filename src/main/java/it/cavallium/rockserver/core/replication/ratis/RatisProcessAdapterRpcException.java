package it.cavallium.rockserver.core.replication.ratis;

public final class RatisProcessAdapterRpcException extends RuntimeException {

    private final String errorType;

    public RatisProcessAdapterRpcException(String errorType, String message) {
        super(message);
        this.errorType = RatisProcessAdapterRpcLimits.requireErrorTypeNonBlank(errorType);
    }

    public String errorType() {
        return errorType;
    }
}
