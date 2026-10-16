package it.cavallium.rockserver.core.common.schema;

import com.google.protobuf.MessageLite;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import java.util.*;

/** One bounded current wire; enums and optional outcomes must describe the same retained UUID. */
public final class SchemaOperationsWire {
    private SchemaOperationsWire(){}
    public static <T extends MessageLite>T bounded(T value){if(Objects.requireNonNull(value).getSerializedSize()>SchemaOperations.MAX_FRAME_BYTES)throw new IllegalArgumentException("Schema operation frame exceeds bound");return value;}
    private static <T>T tag(int value,T[] values){if(value<0 || value>=values.length)throw new IllegalArgumentException("Unknown schema operation enum");return values[value];}
    public static SchemaOperationStatus status(SchemaOperations.Status value){
        var result=SchemaOperationStatus.newBuilder().setRequestId(DistributedCdcWire.uuid(value.requestId())).setKindValue(value.kind().ordinal()).setStateValue(value.state().ordinal());
        if(value.phase()!=null)result.setPhaseValue(value.phase().ordinal());
        if(value.result()!=null){var r=value.result();result.setResult(SchemaOperationResult.newBuilder().setRequestId(DistributedCdcWire.uuid(r.requestId())).setKindValue(r.kind().ordinal()).setValue(r.value()).setChanged(r.changed()).setAborted(r.aborted()));}
        return bounded(result.build());
    }
    public static SchemaOperations.Status status(SchemaOperationStatus value){
        bounded(value);var result=value.hasResult()?value.getResult():null;
        return new SchemaOperations.Status(DistributedCdcWire.uuid(value.getRequestId()),tag(value.getKindValue(),SchemaOperations.Kind.values()),tag(value.getStateValue(),SchemaOperations.State.values()),value.hasPhase()?tag(value.getPhaseValue(),SchemaOperations.Phase.values()):null,result==null?null:new SchemaOperations.Result(DistributedCdcWire.uuid(result.getRequestId()),tag(result.getKindValue(),SchemaOperations.Kind.values()),result.getValue(),result.getChanged(),result.getAborted()));
    }
    public static SchemaOperations.Status status(SchemaOperationStatus value,UUID expected){return SchemaOperations.require(status(value),expected);}
    public static SchemaOperationInventory inventory(List<SchemaOperations.Status> values){var result=SchemaOperationInventory.newBuilder();SchemaOperations.inventory(values).forEach(value->result.addOperations(status(value)));return bounded(result.build());}
    public static List<SchemaOperations.Status> inventory(SchemaOperationInventory value){bounded(value);if(value.getOperationsCount()>SchemaOperations.MAX_OPERATIONS)throw new IllegalArgumentException("Schema inventory exceeds bound");var decoded=value.getOperationsList().stream().map(SchemaOperationsWire::status).toList();var canonical=SchemaOperations.inventory(decoded);if(!canonical.equals(decoded))throw new IllegalArgumentException("Noncanonical schema inventory");return canonical;}
}
