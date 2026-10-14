package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;

/** Only this bulk input ordinal expired; the original whole-request token remains live. */
public final class RatisBulkInputExpiredException extends IOException {
    private final long ordinal,retiredThrough;
    public RatisBulkInputExpiredException(long ordinal,long retiredThrough){super("Bulk input ordinal "+ordinal+" expired through "+retiredThrough);this.ordinal=ordinal;this.retiredThrough=retiredThrough;}
    public long ordinal(){return ordinal;}
    public long retiredThrough(){return retiredThrough;}
}
