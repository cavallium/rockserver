package it.cavallium.rockserver.core.impl;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.Arrays;
import java.util.Objects;

public class WriteBatchIterator {

    // Op codes
    private static final int kTypeDeletion = 0x00;
    private static final int kTypeValue = 0x01;
    private static final int kTypeMerge = 0x02;
    private static final int kTypeLogData = 0x03;
    private static final int kTypeColumnFamilyDeletion = 0x04;
    private static final int kTypeColumnFamilyValue = 0x05;
    private static final int kTypeColumnFamilyMerge = 0x06;
    private static final int kTypeSingleDeletion = 0x07;
    private static final int kTypeColumnFamilySingleDeletion = 0x08;
    private static final int kTypeBeginPrepareXID = 0x09;
    private static final int kTypeEndPrepareXID = 0x0A;
    private static final int kTypeCommitXID = 0x0B;
    private static final int kTypeRollbackXID = 0x0C;
    private static final int kTypeNoop = 0x0D;
    private static final int kTypeColumnFamilyRangeDeletion = 0x0E;
    private static final int kTypeRangeDeletion = 0x0F;
    private static final int kTypeColumnFamilyBlobIndex = 0x10;
    private static final int kTypeBlobIndex = 0x11;
    private static final int kTypeBeginPersistedPrepareXID = 0x12;
    private static final int kTypeBeginUnprepareXID = 0x13;
    private static final int kTypeDeletionWithTimestamp = 0x14;
    private static final int kTypeCommitWithTimestamp = 0x15;
    private static final int kTypeWideColumnEntity = 0x16;
    private static final int kTypeColumnFamilyWideColumnEntity = 0x17;
    private static final int kTypeValuePreferredSeqno = 0x18;
    private static final int kTypeColumnFamilyValuePreferredSeqno = 0x19;

    private static final int HEADER_SIZE = 12; // 8 seq + 4 count

    public static void iterate(byte[] data, WriteBatch.Handler handler) throws RocksDBException {
        cursor(data).iterate(handler, Integer.MAX_VALUE);
    }

    /**
     * Create a resumable decoder over one serialized WriteBatch. The cursor owns the
     * byte-array view and can safely move between scheduler threads as long as callers
     * serialize access to it. Each call decodes at most {@code maxRecords}
     * sequence-consuming mutations, retaining the byte offset for the next slice.
     */
    public static Cursor cursor(byte[] data) throws RocksDBException {
        return new Cursor(data);
    }

    public static final class Cursor {

        private final byte[] data;
        private final long expectedRecords;
        private int offset = HEADER_SIZE;
        private long foundRecords;
        private boolean emptyBatch = true;
        private boolean finished;
        private int lastSequenceRecordOffset = -1;
        private boolean emptyBatchBeforeLastSequenceRecord;

        private Cursor(byte[] data) throws RocksDBException {
            this.data = Objects.requireNonNull(data, "data");
            if (data.length < HEADER_SIZE) {
                throw malformed("header is shorter than " + HEADER_SIZE + " bytes");
            }
            this.expectedRecords = readRecordCount(data);
            finishIfAtEnd();
        }

        public int iterate(WriteBatch.Handler handler, int maxRecords) throws RocksDBException {
            Objects.requireNonNull(handler, "handler");
            if (maxRecords <= 0) {
                throw new IllegalArgumentException("maxRecords must be positive");
            }
            if (finished) {
                return 0;
            }

            int decodedRecords = 0;
            lastSequenceRecordOffset = -1;
            while (offset < data.length && decodedRecords < maxRecords) {
                if (!handler.shouldContinue()) {
                    break;
                }

                int nextOffset = offset;
                int recordOffset = offset;
                boolean emptyBatchBeforeRecord = emptyBatch;
                int type = data[nextOffset++] & 0xFF;
                boolean sequenceRecord = false;

                // Default-column-family records.
                if (type == kTypeValue) {
                    VarString key = readVarString(data, nextOffset);
                    VarString value = readVarString(data, key.nextOffset);
                    handler.put(key.data, value.data);
                    nextOffset = value.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeDeletion) {
                    VarString key = readVarString(data, nextOffset);
                    handler.delete(key.data);
                    nextOffset = key.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeSingleDeletion) {
                    VarString key = readVarString(data, nextOffset);
                    handler.singleDelete(key.data);
                    nextOffset = key.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeMerge) {
                    VarString key = readVarString(data, nextOffset);
                    VarString value = readVarString(data, key.nextOffset);
                    handler.merge(key.data, value.data);
                    nextOffset = value.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeRangeDeletion) {
                    VarString begin = readVarString(data, nextOffset);
                    VarString end = readVarString(data, begin.nextOffset);
                    handler.deleteRange(begin.data, end.data);
                    nextOffset = end.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeLogData) {
                    VarString blob = readVarString(data, nextOffset);
                    handler.logData(blob.data);
                    nextOffset = blob.nextOffset;
                } else if (type == kTypeBlobIndex) {
                    VarString key = readVarString(data, nextOffset);
                    VarString value = readVarString(data, key.nextOffset);
                    handler.putBlobIndex(0, key.data, value.data);
                    nextOffset = value.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeNoop) {
                    handler.markNoop(emptyBatch);
                    emptyBatch = true;
                }
                // Explicit-column-family records.
                else if (type == kTypeColumnFamilyValue) {
                    VarInt cfId = readVarInt(data, nextOffset);
                    VarString key = readVarString(data, cfId.nextOffset);
                    VarString value = readVarString(data, key.nextOffset);
                    handler.put(cfId.value, key.data, value.data);
                    nextOffset = value.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeColumnFamilyDeletion) {
                    VarInt cfId = readVarInt(data, nextOffset);
                    VarString key = readVarString(data, cfId.nextOffset);
                    handler.delete(cfId.value, key.data);
                    nextOffset = key.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeColumnFamilySingleDeletion) {
                    VarInt cfId = readVarInt(data, nextOffset);
                    VarString key = readVarString(data, cfId.nextOffset);
                    handler.singleDelete(cfId.value, key.data);
                    nextOffset = key.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeColumnFamilyMerge) {
                    VarInt cfId = readVarInt(data, nextOffset);
                    VarString key = readVarString(data, cfId.nextOffset);
                    VarString value = readVarString(data, key.nextOffset);
                    handler.merge(cfId.value, key.data, value.data);
                    nextOffset = value.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeColumnFamilyRangeDeletion) {
                    VarInt cfId = readVarInt(data, nextOffset);
                    VarString begin = readVarString(data, cfId.nextOffset);
                    VarString end = readVarString(data, begin.nextOffset);
                    handler.deleteRange(cfId.value, begin.data, end.data);
                    nextOffset = end.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                } else if (type == kTypeColumnFamilyBlobIndex) {
                    VarInt cfId = readVarInt(data, nextOffset);
                    VarString key = readVarString(data, cfId.nextOffset);
                    VarString value = readVarString(data, key.nextOffset);
                    handler.putBlobIndex(cfId.value, key.data, value.data);
                    nextOffset = value.nextOffset;
                    sequenceRecord = true;
                    emptyBatch = false;
                }
                // Transaction/control records do not consume sequence numbers.
                else if (type == kTypeBeginPrepareXID
                        || type == kTypeBeginPersistedPrepareXID
                        || type == kTypeBeginUnprepareXID) {
                    handler.markBeginPrepare();
                } else if (type == kTypeEndPrepareXID) {
                    VarString xid = readVarString(data, nextOffset);
                    handler.markEndPrepare(xid.data);
                    nextOffset = xid.nextOffset;
                    emptyBatch = true;
                } else if (type == kTypeCommitXID) {
                    VarString xid = readVarString(data, nextOffset);
                    handler.markCommit(xid.data);
                    nextOffset = xid.nextOffset;
                    emptyBatch = true;
                } else if (type == kTypeRollbackXID) {
                    VarString xid = readVarString(data, nextOffset);
                    handler.markRollback(xid.data);
                    nextOffset = xid.nextOffset;
                    emptyBatch = true;
                } else if (type == kTypeCommitWithTimestamp) {
                    VarString timestamp = readVarString(data, nextOffset);
                    VarString xid = readVarString(data, timestamp.nextOffset);
                    handler.markCommitWithTimestamp(xid.data, timestamp.data);
                    nextOffset = xid.nextOffset;
                    emptyBatch = true;
                } else {
                    throw unsupported(type);
                }

                // Commit cursor state only after the handler accepted the record. A
                // handler failure therefore never leaves a half-consumed record.
                offset = nextOffset;
                if (sequenceRecord) {
                    foundRecords++;
                    decodedRecords++;
                    lastSequenceRecordOffset = recordOffset;
                    emptyBatchBeforeLastSequenceRecord = emptyBatchBeforeRecord;
                }
            }

            finishIfAtEnd();
            return decodedRecords;
        }

        public long recordsRead() {
            return foundRecords;
        }

        public boolean isFinished() {
            return finished;
        }

        /**
         * Rewind the last sequence-consuming record decoded by the most recent slice.
         * This is used when a consumer inspected an event but could not accept it due
         * to a byte boundary. No subsequent record may have been decoded.
         */
        public void rewindLastRecord() {
            if (lastSequenceRecordOffset < 0) {
                throw new IllegalStateException("No sequence record is available to rewind");
            }
            offset = lastSequenceRecordOffset;
            foundRecords--;
            emptyBatch = emptyBatchBeforeLastSequenceRecord;
            finished = false;
            lastSequenceRecordOffset = -1;
        }

        private void finishIfAtEnd() throws RocksDBException {
            if (offset != data.length) {
                return;
            }
            if (foundRecords != expectedRecords) {
                throw malformed("header declares " + expectedRecords + " records, but decoded " + foundRecords);
            }
            finished = true;
        }
    }

    private static long readRecordCount(byte[] data) {
        return (data[8] & 0xFFL)
                | ((data[9] & 0xFFL) << 8)
                | ((data[10] & 0xFFL) << 16)
                | ((data[11] & 0xFFL) << 24);
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

    private static VarInt readVarInt(byte[] data, int offset) throws RocksDBException {
        int result = 0;
        int shift = 0;
        while (true) {
            if (offset >= data.length) {
                throw malformed("truncated VarInt");
            }
            int b = data[offset++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                if (shift == 28 && (b & 0xF0) != 0) {
                    throw malformed("VarInt exceeds 32 bits");
                }
                return new VarInt(result, offset);
            }
            if (shift == 28) {
                throw malformed("VarInt exceeds 32 bits");
            }
            shift += 7;
        }
    }

    private static VarString readVarString(byte[] data, int offset) throws RocksDBException {
        VarInt len = readVarInt(data, offset);
        long length = Integer.toUnsignedLong(len.value);
        offset = len.nextOffset;
        long nextOffset = offset + length;
        if (nextOffset > data.length) {
            throw malformed("VarString length " + length + " exceeds remaining input");
        }
        int nextOffsetInt = Math.toIntExact(nextOffset);
        byte[] str = Arrays.copyOfRange(data, offset, nextOffsetInt);
        return new VarString(str, nextOffsetInt);
    }

    private static RocksDBException unsupported(int type) {
        String detail;
        if (type == kTypeDeletionWithTimestamp) {
            detail = "deletion with timestamp";
        } else if (type == kTypeWideColumnEntity || type == kTypeColumnFamilyWideColumnEntity) {
            detail = "wide-column entity";
        } else if (type == kTypeValuePreferredSeqno || type == kTypeColumnFamilyValuePreferredSeqno) {
            detail = "timed put";
        } else {
            detail = "unknown tag";
        }
        return new RocksDBException("Unsupported operation type in WriteBatch: " + type + " (" + detail + ")");
    }

    private static RocksDBException malformed(String detail) {
        return new RocksDBException("Malformed WriteBatch: " + detail);
    }
}
