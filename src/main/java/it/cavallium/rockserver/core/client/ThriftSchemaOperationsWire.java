package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.schema.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;

public final class ThriftSchemaOperationsWire {
    private ThriftSchemaOperationsWire(){}
    public static ThriftSchemaOperationStatus status(SchemaOperations.Status value){
        var result=new ThriftSchemaOperationStatus(ThriftRemoteMutationWire.uuid(value.requestId()),ThriftSchemaOperationKind.valueOf(value.kind().name()),ThriftSchemaOperationState.valueOf(value.state().name()));
        if(value.phase()!=null)result.setPhase(ThriftSchemaOperationPhase.valueOf(value.phase().name()));
        if(value.result()!=null){var r=value.result();result.setResult(new ThriftSchemaOperationResult(ThriftRemoteMutationWire.uuid(r.requestId()),ThriftSchemaOperationKind.valueOf(r.kind().name()),r.value(),r.changed(),r.aborted()));}return result;
    }
    public static SchemaOperations.Status status(ThriftSchemaOperationStatus value){
        Objects.requireNonNull(value);try{value.validate();}catch(TException malformed){throw new IllegalArgumentException("Invalid schema status",malformed);}
        var r=value.result;if(r!=null && (!r.isSetValue() || !r.isSetChanged() || !r.isSetAborted()))throw new IllegalArgumentException("Missing schema result fields");
        return new SchemaOperations.Status(ThriftRemoteMutationWire.uuid(value.requestId),SchemaOperations.Kind.valueOf(value.kind.name()),SchemaOperations.State.valueOf(value.state.name()),value.phase==null?null:SchemaOperations.Phase.valueOf(value.phase.name()),r==null?null:new SchemaOperations.Result(ThriftRemoteMutationWire.uuid(r.requestId),SchemaOperations.Kind.valueOf(r.kind.name()),r.value,r.changed,r.aborted));
    }
    public static SchemaOperations.Status status(ThriftSchemaOperationStatus value,UUID id){return SchemaOperations.require(status(value),id);}
    public static List<SchemaOperations.Status> inventory(List<ThriftSchemaOperationStatus> value){if(value==null || value.size()>SchemaOperations.MAX_OPERATIONS)throw new IllegalArgumentException("Schema inventory exceeds bound");var list=value.stream().map(ThriftSchemaOperationsWire::status).toList();var canonical=SchemaOperations.inventory(list);if(!canonical.equals(list))throw new IllegalArgumentException("Noncanonical schema inventory");return canonical;}
    public static TProtocol boundedProtocol(TProtocol protocol,boolean request){return new TProtocolDecorator(protocol){
        private short field;
        @Override public TField readFieldBegin()throws TException{var next=super.readFieldBegin();field=next.id;return next;}
        @Override public ByteBuffer readBinary()throws TException{int size=readI32();if(field!=5 || size!=16)throw new TProtocolException(TProtocolException.SIZE_LIMIT,"Schema UUID must contain 16 bytes");getTransport().checkReadBytesAvailable(size);var bytes=new byte[size];getTransport().readAll(bytes,0,size);return ByteBuffer.wrap(bytes);}
        @Override public TList readListBegin()throws TException{var list=super.readListBegin();if(request || field!=0 || list.size<0 || list.size>SchemaOperations.MAX_OPERATIONS)throw new TProtocolException(TProtocolException.SIZE_LIMIT,"Schema inventory exceeds bound");return list;}
        @Override public TMap readMapBegin()throws TException{throw new TProtocolException("No schema operation maps");}
        @Override public TSet readSetBegin()throws TException{throw new TProtocolException("No schema operation sets");}
    };}
}
