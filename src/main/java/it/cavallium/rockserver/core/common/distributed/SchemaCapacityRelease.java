package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;
import java.util.UUID;

/** Trusted committed authority, qualified by the caller before the native release seam. */
public final class SchemaCapacityRelease {
    public enum Kind { ABORTED_BEFORE_FENCING, INSTALLED, RETIRED }
    private final Kind kind;
    private final SchemaChange.Status status;
    private final NativeSchemaInstallProof installation;
    private final UUID catalogueGroup;
    private final ApplyPosition cataloguePosition;
    private SchemaCapacityRelease(Kind kind,SchemaChange.Status status,NativeSchemaInstallProof installation,UUID group,ApplyPosition position) {
        this.kind=kind; this.status=status; this.installation=installation; catalogueGroup=group; cataloguePosition=position;
    }
    public static SchemaCapacityRelease aborted(SchemaChange.Status status,UUID group,ApplyPosition cut) {
        if(status.phase()!=SchemaChange.Phase.ABORTED) throw new IllegalArgumentException("Capacity abort requires ABORTED before FENCING");
        return catalogue(Kind.ABORTED_BEFORE_FENCING,status,group,cut);
    }
    public static SchemaCapacityRelease retired(SchemaChange.Status status,UUID group,ApplyPosition cut) {
        if(status.phase()!=SchemaChange.Phase.RETIRED) throw new IllegalArgumentException("Capacity retirement requires RETIRED");
        return catalogue(Kind.RETIRED,status,group,cut);
    }
    public static SchemaCapacityRelease completed(NativeSchemaInstallProof proof) { return new SchemaCapacityRelease(Kind.INSTALLED,null,Objects.requireNonNull(proof),null,null); }
    private static SchemaCapacityRelease catalogue(Kind kind,SchemaChange.Status status,UUID group,ApplyPosition cut) {
        Objects.requireNonNull(group); Objects.requireNonNull(cut); if(cut.term()<=0) throw new IllegalArgumentException("Catalogue authority requires committed cut");
        return new SchemaCapacityRelease(kind,Objects.requireNonNull(status),null,group,cut);
    }
    public Kind kind() { return kind; }
    public SchemaChange.Status status() { return status; }
    public NativeSchemaInstallProof installation() { return installation; }
    public UUID catalogueGroup() { return catalogueGroup; }
    public ApplyPosition cataloguePosition() { return cataloguePosition; }
}
