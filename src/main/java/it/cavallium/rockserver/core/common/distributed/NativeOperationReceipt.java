package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** One current native outcome shape. Classification preserves original command semantics. */
public final class NativeOperationReceipt {
    public enum Kind { BUSINESS, CONTROL }
    public static final int KEY_BYTES=48,HEADER_BYTES=33,MIN_BYTES=HEADER_BYTES+8,MAX_BYTES=HEADER_BYTES+CommandResult.MAX_BYTES,MINIMUM_ROW_BYTES=KEY_BYTES+MIN_BYTES;
    private NativeOperationReceipt(){}
    public static Kind classify(DistributedCommand command){
        Objects.requireNonNull(command);
        if(command.kind()==DistributedCommand.Kind.SESSION_FLOOR || command.kind()==DistributedCommand.Kind.SESSION_CONTROL)return Kind.CONTROL;
        return command.kind()==DistributedCommand.Kind.DATA && command.mutations().isEmpty() && command.predicates().isEmpty() && command.acceptance()==null && !command.transactionRejection() && command.acknowledgedThrough()>=0?Kind.CONTROL:Kind.BUSINESS;
    }
    public static byte[] encode(DistributedCommand command,byte[] result){
        Objects.requireNonNull(result);var kind=classify(command);if(result.length>CommandResult.MAX_BYTES)throw new IllegalArgumentException("Native receipt exceeds result bound");
        byte[] value=ByteBuffer.allocate(HEADER_BYTES+result.length).put((byte)kind.ordinal()).put(command.receiptDigest()).put(result).array();validate(value);return value;
    }
    public static Kind validate(byte[] value){
        Objects.requireNonNull(value);if(value.length<MIN_BYTES || value.length>MAX_BYTES)throw new IllegalArgumentException("Invalid native receipt length");
        var kind=switch(value[0]){case 0->Kind.BUSINESS;case 1->Kind.CONTROL;default->throw new IllegalArgumentException("Invalid native receipt semantic");};if(kind==Kind.CONTROL){if(value.length!=HEADER_BYTES+8)throw new IllegalArgumentException("Control receipt contains business values");var result=CommandResult.decode(Arrays.copyOfRange(value,HEADER_BYTES,value.length));if(result.valueCount()!=0)throw new IllegalArgumentException("Control receipt contains business values");}
        return kind;
    }
    public static byte[] result(byte[] value,byte[] digest){
        if(digest==null || digest.length!=32)throw new IllegalArgumentException("Invalid native receipt digest");validate(value);if(!Arrays.equals(value,1,HEADER_BYTES,digest,0,32))throw new IllegalArgumentException("Operation identity reused with different payload");return Arrays.copyOfRange(value,HEADER_BYTES,value.length);
    }
}
