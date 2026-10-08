package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;

/** The original frozen read/prepare authority cannot be supplied; it must not be rebased. */
public final class RatisPreparationUnavailableException extends IOException {
    public RatisPreparationUnavailableException(String message) { super(message); }
    public RatisPreparationUnavailableException(String message,Throwable cause) { super(message,cause); }
}
