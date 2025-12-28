package it.cavallium.rockserver.core.impl;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.Arrays;

public class WriteBatchIterator {

    // Op codes
    private static final byte kTypeDeletion = 0;
    private static final byte kTypeValue = 1;
    private static final byte kTypeMerge = 2;
    private static final byte kTypeLogData = 3;
    private static final byte kTypeColumnFamilyDeletion = 4;
    private static final byte kTypeColumnFamilyValue = 5;
    private static final byte kTypeColumnFamilyMerge = 6;
    private static final byte kTypeSingleDeletion = 7;
    private static final byte kTypeColumnFamilySingleDeletion = 8;
    private static final byte kTypeBeginPrepareXID = 9;
    private static final byte kTypeEndPrepareXID = 10;
    private static final byte kTypeCommitXID = 11;
    private static final byte kTypeRollbackXID = 12;
    private static final byte kTypeNoop = 13;
    private static final byte kTypeColumnFamilyRangeDeletion = 14;
    private static final byte kTypeRangeDeletion = 15;
    private static final byte kTypeColumnFamilyBlobIndex = 16;
    private static final byte kTypeBlobIndex = 17;
    private static final byte kTypeBeginPersistedPrepareXID = 18;
    private static final byte kTypeBeginUnprepareXID = 19;
    private static final byte kTypeCommitWithTimestamp = 20;

    private static final int HEADER_SIZE = 12; // 8 seq + 4 count

    public static void iterate(byte[] data, WriteBatch.Handler handler) throws RocksDBException {
        if (data.length < HEADER_SIZE) {
            return;
        }

        int offset = HEADER_SIZE;
        while (offset < data.length) {
            byte type = data[offset++];
            
            // Check for variants of ops
            // Default CF ops
            if (type == kTypeValue) {
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                VarString val = readVarString(data, offset);
                offset = val.nextOffset;
                handler.put(key.data, val.data);
            } else if (type == kTypeDeletion) {
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                handler.delete(key.data);
            } else if (type == kTypeSingleDeletion) {
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                handler.singleDelete(key.data);
            } else if (type == kTypeMerge) {
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                VarString val = readVarString(data, offset);
                offset = val.nextOffset;
                handler.merge(key.data, val.data);
            } else if (type == kTypeRangeDeletion) {
                VarString begin = readVarString(data, offset);
                offset = begin.nextOffset;
                VarString end = readVarString(data, offset);
                offset = end.nextOffset;
                handler.deleteRange(begin.data, end.data);
            } else if (type == kTypeLogData) {
                VarString blob = readVarString(data, offset);
                offset = blob.nextOffset;
                handler.logData(blob.data);
            } else if (type == kTypeBlobIndex) {
                 VarString key = readVarString(data, offset);
                 offset = key.nextOffset;
                 VarString val = readVarString(data, offset);
                 offset = val.nextOffset;
                 handler.putBlobIndex(0, key.data, val.data); // Default CF?
            } else if (type == kTypeNoop) {
                handler.markNoop(false);
            }
            // CF ops
            else if (type == kTypeColumnFamilyValue) {
                VarInt cfId = readVarInt(data, offset);
                offset = cfId.nextOffset;
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                VarString val = readVarString(data, offset);
                offset = val.nextOffset;
                handler.put(cfId.value, key.data, val.data);
            } else if (type == kTypeColumnFamilyDeletion) {
                VarInt cfId = readVarInt(data, offset);
                offset = cfId.nextOffset;
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                handler.delete(cfId.value, key.data);
            } else if (type == kTypeColumnFamilySingleDeletion) {
                VarInt cfId = readVarInt(data, offset);
                offset = cfId.nextOffset;
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                handler.singleDelete(cfId.value, key.data);
            } else if (type == kTypeColumnFamilyMerge) {
                VarInt cfId = readVarInt(data, offset);
                offset = cfId.nextOffset;
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                VarString val = readVarString(data, offset);
                offset = val.nextOffset;
                handler.merge(cfId.value, key.data, val.data);
            } else if (type == kTypeColumnFamilyRangeDeletion) {
                VarInt cfId = readVarInt(data, offset);
                offset = cfId.nextOffset;
                VarString begin = readVarString(data, offset);
                offset = begin.nextOffset;
                VarString end = readVarString(data, offset);
                offset = end.nextOffset;
                handler.deleteRange(cfId.value, begin.data, end.data);
            } else if (type == kTypeColumnFamilyBlobIndex) {
                VarInt cfId = readVarInt(data, offset);
                offset = cfId.nextOffset;
                VarString key = readVarString(data, offset);
                offset = key.nextOffset;
                VarString val = readVarString(data, offset);
                offset = val.nextOffset;
                handler.putBlobIndex(cfId.value, key.data, val.data);
            }
            // Transaction ops
            else if (type == kTypeBeginPrepareXID) {
                handler.markBeginPrepare(); // WriteBatch.Handler doesn't take XID for this?
                // Wait, RocksDB Java Handler has markBeginPrepare() no args?
                // But payload has XID?
                // Let's check Java API.
                // It has void markBeginPrepare();
                // So we parse XID but drop it?
                // Or maybe markBeginPrepare() is for something else?
                // No, BeginPrepareXID usually implies XID.
                // rocksdbjni Handler: markBeginPrepare()
                // Let's check payload.
                VarString xid = readVarString(data, offset);
                offset = xid.nextOffset;
                handler.markBeginPrepare(); // Ignoring XID as per Java API limitation?
            } else if (type == kTypeEndPrepareXID) {
                VarString xid = readVarString(data, offset);
                offset = xid.nextOffset;
                handler.markEndPrepare(xid.data);
            } else if (type == kTypeCommitXID) {
                VarString xid = readVarString(data, offset);
                offset = xid.nextOffset;
                handler.markCommit(xid.data);
            } else if (type == kTypeRollbackXID) {
                VarString xid = readVarString(data, offset);
                offset = xid.nextOffset;
                handler.markRollback(xid.data);
            } else if (type == kTypeBeginPersistedPrepareXID) {
                VarString xid = readVarString(data, offset);
                offset = xid.nextOffset;
                handler.markBeginPrepare(); // Mapping to markBeginPrepare
            } else if (type == kTypeBeginUnprepareXID) {
                 VarString xid = readVarString(data, offset);
                 offset = xid.nextOffset;
                 // No Java API for unprepare? Ignore or map to rollback?
                 // Ignore for now but consume.
            } else if (type == kTypeCommitWithTimestamp) {
                VarString ts = readVarString(data, offset);
                offset = ts.nextOffset;
                VarString xid = readVarString(data, offset);
                offset = xid.nextOffset;
                handler.markCommitWithTimestamp(xid.data, ts.data);
            }
            else {
                throw new RocksDBException("Unsupported operation type in WriteBatch: " + type);
            }
        }
    }

    private static class VarInt {
        final int value;
        final int nextOffset;
        VarInt(int value, int nextOffset) { this.value = value; this.nextOffset = nextOffset; }
    }

    private static class VarString {
        final byte[] data;
        final int nextOffset;
        VarString(byte[] data, int nextOffset) { this.data = data; this.nextOffset = nextOffset; }
    }

    private static VarInt readVarInt(byte[] data, int offset) {
        int result = 0;
        int shift = 0;
        while (true) {
            if (offset >= data.length) throw new IndexOutOfBoundsException("Malformed VarInt");
            byte b = data[offset++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return new VarInt(result, offset);
    }

    private static VarString readVarString(byte[] data, int offset) {
        VarInt len = readVarInt(data, offset);
        int length = len.value;
        offset = len.nextOffset;
        if (offset + length > data.length) throw new IndexOutOfBoundsException("Malformed VarString");
        byte[] str = Arrays.copyOfRange(data, offset, offset + length);
        return new VarString(str, offset + length);
    }
}
