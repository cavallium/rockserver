package it.cavallium.rockserver.core.common.schema;

import java.util.*;

/** Bounded retained schema outcomes. A new ordinary schema call creates a new UUID. */
public final class SchemaOperations {
    public static final int MAX_OPERATIONS=128,MAX_FRAME_BYTES=65536;
    private SchemaOperations(){}
    public enum Kind { CREATE,DELETE,DELETE_IF_EXISTS,UPLOAD }
    public enum State { QUEUED,NOT_STARTED,RUNNING,PAUSED,DONE,ABORTED,CONFLICT }
    public enum Phase { ALLOCATE,SCHEMA,DEFINITION,BEGIN,ARTIFACTS,CAPACITY,ADMISSIONS,FENCING,FENCE,INSTALL,PUBLISH,ACTIVATE,RETIRE,CLEANUP,DONE,ABORTED,CONFLICT,OPERATOR_BEGIN,OPERATOR_CHUNKS,OPERATOR_PUBLISH }
    public record Result(UUID requestId,Kind kind,long value,boolean changed,boolean aborted){
        public Result{Objects.requireNonNull(requestId);Objects.requireNonNull(kind);if(changed && aborted)throw new IllegalArgumentException("Aborted schema operation cannot report a change");}
    }
    public record Status(UUID requestId,Kind kind,State state,Phase phase,Result result){
        public Status{
            Objects.requireNonNull(requestId);Objects.requireNonNull(kind);Objects.requireNonNull(state);
            boolean terminal=state==State.DONE || state==State.ABORTED;
            if(terminal!=(result!=null) || result!=null && (!requestId.equals(result.requestId()) || kind!=result.kind() || (state==State.ABORTED)!=result.aborted())
                    || state==State.DONE && phase!=null && phase!=Phase.DONE || state==State.ABORTED && phase!=null && phase!=Phase.ABORTED || state==State.CONFLICT && phase!=null && phase!=Phase.CONFLICT)throw new IllegalArgumentException("Inconsistent schema operation identity/state/result");
        }
    }
    public static List<Status> inventory(List<Status> values){
        Objects.requireNonNull(values);if(values.size()>MAX_OPERATIONS)throw new IllegalArgumentException("Schema operation inventory exceeds bound");
        var ids=new HashSet<UUID>();for(var value:values)if(!ids.add(Objects.requireNonNull(value).requestId()))throw new IllegalArgumentException("Duplicate schema operation UUID");
        return values.stream().sorted(Comparator.comparing(Status::requestId,SchemaOperations::compare)).toList();
    }
    private static int compare(UUID a,UUID b){int n=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());return n!=0?n:Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());}
    public static Status require(Status value,UUID id){if(!Objects.requireNonNull(value).requestId().equals(Objects.requireNonNull(id)))throw new IllegalArgumentException("Crossed schema operation UUID");return value;}
}
