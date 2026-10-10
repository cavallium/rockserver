package it.cavallium.rockserver.distributed.ratis;

import java.util.Arrays;

/** Current bounded native import preparation; the echoed record grants no transferable authority. */
public final class RatisRangeImportRpc {
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+RatisRangeImportRecord.MAX_BYTES;
    private RatisRangeImportRpc() {}
    public static void requireFile(it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest manifest,RatisReferencedFile.Request request) {
        boolean expected=manifest.groupId().equals(request.groupId()) && request.length()==manifest.encode().length && Arrays.equals(request.sha256(),manifest.digest());
        if(!expected && manifest.groupId().equals(request.groupId()))expected=manifest.columns().stream().flatMap(column->column.files().stream())
                .anyMatch(file->file.bytes()==request.length() && Arrays.equals(file.sha256(),request.sha256()));
        if(!expected)throw new IllegalArgumentException("Streamed bytes are outside the frozen import manifest");
    }
    public static byte[] prepare(RatisRangeImportRecord record) { return ScopedTransactionRpc.frame(ScopedTransactionRpc.IMPORT_APPLY_PREPARE,record.encode()); }
    public static byte[] cancel(RatisRangeImportRecord record) { return ScopedTransactionRpc.frame(ScopedTransactionRpc.IMPORT_APPLY_CANCEL,record.encode()); }
    public static byte[] prepare(RatisReferencedFile.Request request) { return ScopedTransactionRpc.frame(ScopedTransactionRpc.IMPORT_APPLY_PREPARE,request.encode()); }
    public static byte[] cancel(RatisReferencedFile.Request request) { return ScopedTransactionRpc.frame(ScopedTransactionRpc.IMPORT_APPLY_CANCEL,request.encode()); }
    static Object proposal(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES); if(!ScopedTransactionRpc.importApply(packet.kind()))throw new IllegalArgumentException("Expected native import preparation");
        return RatisReferencedFile.Request.isRequest(packet.body()) ? RatisReferencedFile.Request.decode(packet.body()) : RatisRangeImportRecord.decode(packet.body());
    }
    public static RatisRangeImportRecord record(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES); if(!ScopedTransactionRpc.importApply(packet.kind()))throw new IllegalArgumentException("Expected native import preparation");
        return RatisRangeImportRecord.decode(packet.body());
    }
    public static void requireReply(byte[] bytes,int kind,RatisRangeImportRecord expected) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if(packet.kind()!=kind || !Arrays.equals(record(bytes).encode(),expected.encode()))throw new IllegalArgumentException("Native import reply changed exact request");
    }
    public static void requireReply(byte[] bytes,int kind,RatisReferencedFile.Request expected) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if(packet.kind()!=kind || !(proposal(bytes) instanceof RatisReferencedFile.Request) || !Arrays.equals(packet.body(),expected.encode()))throw new IllegalArgumentException("Reference preparation reply changed exact request");
    }
}
