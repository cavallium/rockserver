package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.HomeRecoveryReceipt;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.PreparedParticipant;
import it.cavallium.rockserver.core.common.distributed.AbortedParticipant;
import it.cavallium.rockserver.core.common.distributed.RejectedParticipant;
import it.cavallium.rockserver.core.common.distributed.SnapshotGate;
import it.cavallium.rockserver.core.common.distributed.CommittedParticipant;
import it.cavallium.rockserver.core.common.distributed.RangeMutationHistory;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.RangeSessionControl;
import it.cavallium.rockserver.core.common.distributed.SessionAuthority;
import it.cavallium.rockserver.core.common.distributed.SessionEpochLedger;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Portable logical range image from one native snapshot cut; no host paths or physical SST identity. */
public final class NativeRangeImage {
    private static final int MAGIC = 0x4e52494d; // NRIM
    private static final int VERSION = 1;
    private static final int HASH_BYTES = 32;
    private static final Comparator<UUID> UUID_ORDER = (a, b) -> {
        int first = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
        return first != 0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
    };

    /** maxRowBytes bounds key+value bytes together, and also each encoded interval bound. */
    public record Limits(int maxRowBytes, long maxImageBytes, int maxColumns) {
        public Limits {
            if (maxRowBytes <= 0 || maxImageBytes <= 0 || maxColumns <= 0) {
                throw new IllegalArgumentException("Image limits must be positive");
            }
        }
    }

    /** sha256 hashes the entire file, including its embedded body checksum; bytes is the file size. */
    public record Manifest(RangeOwnership.Owner owner, RangeOwnership.Interval interval,
                           byte[] schemaDigest, ApplyPosition position, Set<UUID> deterministicMergeColumns,
                           long rows, long bytes, byte[] sha256) {
        public Manifest {
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(interval, "interval");
            Objects.requireNonNull(position, "position");
            if (schemaDigest.length != HASH_BYTES || sha256.length != HASH_BYTES || rows < 0 || bytes < 0) {
                throw new IllegalArgumentException("Invalid image manifest");
            }
            schemaDigest = schemaDigest.clone();
            sha256 = sha256.clone();
            deterministicMergeColumns = Set.copyOf(deterministicMergeColumns);
            if (!interval.columns().containsAll(deterministicMergeColumns)) {
                throw new IllegalArgumentException("Merge admission refers to another column");
            }
        }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public byte[] sha256() { return sha256.clone(); }
    }

    public enum Section { USER, STATE, RECEIPTS }

    @FunctionalInterface
    public interface RowVisitor {
        /** columnId is null for STATE and RECEIPTS. */
        void accept(Section section, UUID columnId, byte[] key, byte[] value) throws IOException;
    }

    private record HomeContext(UUID cluster,UUID database,UUID column) {}
    private record Header(RangeOwnership.Owner owner, RangeOwnership.Interval interval,
                          byte[] schema, ApplyPosition position, Set<UUID> mergeColumns) {}

    private NativeRangeImage() {}

    /**
     * Streams only the supplied pin's cut and forces a private temporary file. An atomic hard-link
     * creation publishes the complete image without overwriting a destination; directory fsync
     * precedes temporary unlink and another directory fsync. This is namespace publication, not
     * an independently retained backup. Filesystem hard links and directory fsync are required.
     * A failure after publication is reported and the caller must not advertise that image.
     */
    public static Manifest write(Path destination, ReplicatedRangeStore.SnapshotPin pin,
                                 RangeOwnership.Interval interval, Limits limits)
            throws IOException, RocksDBException {
        Objects.requireNonNull(pin, "pin");
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(limits, "limits");
        if (!pin.interval().equals(interval) || !pin.columns().equals(interval.columns()) || pin.position() == null) {
            throw new IllegalArgumentException("Image requires exact pin interval/columns and a completed position");
        }
        Header header = new Header(new RangeOwnership.Owner(pin.groupId(), pin.rangeEpoch()), interval,
                pin.schemaDigest(), pin.position(), Set.copyOf(pin.deterministicMergeColumns()));
        validateHeader(header, limits);
        Path target = destination.toAbsolutePath();
        Path directory = target.getParent();
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) throw new FileAlreadyExistsException(target.toString());
        Path temporary = Files.createTempFile(directory, ".native-range-building-", ".tmp");
        try {
            MessageDigest whole = digest();
            MessageDigest body = digest();
            long bytes;
            long rows;
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                var counted = new BoundedOutput(new DigestOutputStream(new BufferedOutputStream(
                        Channels.newOutputStream(channel)), whole), limits.maxImageBytes());
                var bodyOutput = new DigestOutputStream(counted, body);
                var output = new DataOutputStream(bodyOutput);
                writeHeader(output, header);
                var tracker = new Rows(header, limits);
                RowVisitor append = (section, column, key, value) -> {
                    tracker.accept(section, column, key, value);
                    output.writeByte(section.ordinal() + 1);
                    if (column != null) writeUuid(output, column);
                    output.writeInt(key.length);
                    output.writeInt(value.length);
                    output.write(key);
                    output.write(value);
                };
                for (UUID column : sorted(interval.columns())) {
                    pin.visitColumn(column, interval.lowerInclusive(), interval.upperExclusive(),
                            (key, value) -> append.accept(Section.USER, column, key, value));
                }
                pin.visitState((key, value) -> append.accept(Section.STATE, null, key, value));
                pin.visitReceipts((key, value) -> append.accept(Section.RECEIPTS, null, key, value));
                tracker.finish();
                try { pin.validateHomeCreditImage(tracker.homeCredits); }
                catch (IllegalArgumentException | IllegalStateException invalid) { throw new IOException("HOME image user/promise reverse proof differs",invalid); }
                output.writeByte(0);
                output.writeLong(tracker.count);
                bodyOutput.on(false);
                output.write(body.digest());
                output.flush();
                channel.force(true);
                rows = tracker.count;
                bytes = counted.count;
            }
            Files.createLink(target, temporary);
            forceDirectory(directory);
            Files.delete(temporary);
            forceDirectory(directory);
            return manifest(header, rows, bytes, whole.digest());
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** Fully validates the image without exposing rows. */
    public static Manifest inspect(Path image, Limits limits) throws IOException {
        return visit(image, limits, (section, column, key, value) -> {});
    }

    /** Qualified TX HOME reverse proof, including an image with no credit rows. Does not activate native admission. */
    public static Manifest inspectHomeRecoveryReservations(Path image,Limits limits,UUID cluster,UUID database,UUID column) throws IOException {
        return visitHomeRecoveryReservations(image,limits,cluster,database,column,(section,id,key,value) -> {});
    }
    public static Manifest visitHomeRecoveryReservations(Path image,Limits limits,UUID cluster,UUID database,UUID column,RowVisitor visitor) throws IOException {
        return visit(image,limits,visitor,true,new HomeContext(Objects.requireNonNull(cluster),Objects.requireNonNull(database),Objects.requireNonNull(column)));
    }

    /**
     * Verifies all bytes, row order, ownership and marker identity. Rows are delivered before the final
     * checksum is verified: an installer must remain inactive until this method returns successfully.
     * Use inspect first for validation before effects, and still require visit success afterward.
     */
    public static Manifest visit(Path image, Limits limits, RowVisitor visitor) throws IOException { return visit(image,limits,visitor,true,null); }
    private static Manifest visit(Path image, Limits limits, RowVisitor visitor,boolean verifyCreditUsers,HomeContext required) throws IOException {
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(visitor, "visitor");
        long size = Files.size(image);
        if (size <= HASH_BYTES || size > limits.maxImageBytes()) throw new IOException("Invalid image byte size");
        MessageDigest whole = digest();
        MessageDigest body = digest();
        try (var source = new BufferedInputStream(Files.newInputStream(image))) {
            var counted = new BoundedInput(new DigestInputStream(source, whole), size);
            var bodyInput = new DigestInputStream(counted, body);
            var input = new DataInputStream(bodyInput);
            Header header = readHeader(input, counted, limits);
            var tracker = new Rows(header, limits);
            while (true) {
                int tag = input.readUnsignedByte();
                if (tag == 0) break;
                if (tag > Section.values().length) throw new IOException("Unknown image section");
                Section section = Section.values()[tag - 1];
                UUID column = section == Section.USER ? readUuid(input) : null;
                int keyLength = input.readInt();
                int valueLength = input.readInt();
                long rowBytes = (long) keyLength + valueLength;
                if (keyLength < 0 || valueLength < 0 || rowBytes > limits.maxRowBytes()
                        || rowBytes > counted.remaining() - 41) {
                    throw new IOException("Invalid image row lengths");
                }
                byte[] key = new byte[keyLength];
                byte[] value = new byte[valueLength];
                input.readFully(key);
                input.readFully(value);
                tracker.accept(section, column, key, value);
                visitor.accept(section, column, key, value);
            }
            long declaredRows = input.readLong();
            if (declaredRows != tracker.count) throw new IOException("Image row count mismatch");
            tracker.finish();
            bodyInput.on(false);
            byte[] embedded = new byte[HASH_BYTES];
            input.readFully(embedded);
            if (!MessageDigest.isEqual(embedded, body.digest())) throw new IOException("Image body checksum mismatch");
            if (input.read() != -1 || counted.count != size) throw new IOException("Trailing or changing image bytes");
            var manifest=manifest(header, tracker.count, size, whole.digest());
            var homeContext=tracker.homeContext(required);
            if (verifyCreditUsers && homeContext!=null) {
                var matched=new HashSet<UUID>();
                var checked=visit(image,limits,(section,column,key,value) -> { if (section==Section.USER) tracker.requireHomeUser(homeContext,column,key,value,matched); },false,null);
                if (matched.size()!=tracker.homeCredits.size() || !Arrays.equals(manifest.sha256(),checked.sha256())) throw new IOException("HOME image promises have missing user rows or image changed between passes");
            }
            return manifest;
        } catch (IllegalArgumentException malformed) {
            throw new IOException("Invalid image metadata", malformed);
        }
    }

    private static Manifest manifest(Header header, long rows, long bytes, byte[] sha256) {
        return new Manifest(header.owner(), header.interval(), header.schema(), header.position(), header.mergeColumns(), rows, bytes, sha256);
    }

    private static void validateHeader(Header header, Limits limits) throws IOException {
        if (header.schema().length != HASH_BYTES || header.interval().columns().size() > limits.maxColumns()
                || !header.interval().columns().containsAll(header.mergeColumns())) {
            throw new IOException("Image schema/column contract mismatch");
        }
        for (byte[] bound : new byte[][] {header.interval().lowerInclusive(), header.interval().upperExclusive()}) {
            if (bound != null && bound.length > limits.maxRowBytes()) throw new IOException("Image bound exceeds byte limit");
        }
    }

    private static void writeHeader(DataOutputStream output, Header header) throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(VERSION);
        writeUuid(output, header.owner().groupId());
        output.writeLong(header.owner().epoch());
        output.write(header.schema());
        output.writeLong(header.position().term());
        output.writeLong(header.position().index());
        writeUuid(output, header.interval().domainId());
        writeIds(output, header.interval().columns());
        writeBound(output, header.interval().lowerInclusive());
        writeBound(output, header.interval().upperExclusive());
        writeIds(output, header.mergeColumns());
    }

    private static Header readHeader(DataInputStream input, BoundedInput counted, Limits limits) throws IOException {
        if (input.readInt() != MAGIC || input.readInt() != VERSION) throw new IOException("Unsupported image magic/version");
        var owner = new RangeOwnership.Owner(readUuid(input), input.readLong());
        byte[] schema = new byte[HASH_BYTES];
        input.readFully(schema);
        var position = new ApplyPosition(input.readLong(), input.readLong());
        UUID domain = readUuid(input);
        Set<UUID> columns = readIds(input, counted, limits, false);
        byte[] lower = readBound(input, counted, limits);
        byte[] upper = readBound(input, counted, limits);
        Set<UUID> mergeColumns = readIds(input, counted, limits, true);
        var header = new Header(owner, new RangeOwnership.Interval(domain, columns, lower, upper), schema, position, mergeColumns);
        validateHeader(header, limits);
        return header;
    }

    private static List<UUID> sorted(Set<UUID> ids) { return ids.stream().sorted(UUID_ORDER).toList(); }

    private static void writeIds(DataOutputStream output, Set<UUID> ids) throws IOException {
        output.writeInt(ids.size());
        for (UUID id : sorted(ids)) writeUuid(output, id);
    }

    private static Set<UUID> readIds(DataInputStream input, BoundedInput counted, Limits limits, boolean emptyAllowed) throws IOException {
        int count = input.readInt();
        if (count < (emptyAllowed ? 0 : 1) || count > limits.maxColumns() || (long) count * 16 > counted.remaining() - 41) {
            throw new IOException("Invalid image column count");
        }
        var ids = new HashSet<UUID>();
        UUID previous = null;
        for (int i = 0; i < count; i++) {
            UUID id = readUuid(input);
            if (previous != null && UUID_ORDER.compare(previous, id) >= 0) throw new IOException("Noncanonical image column order");
            ids.add(id);
            previous = id;
        }
        return Set.copyOf(ids);
    }

    private static void writeBound(DataOutputStream output, byte[] bound) throws IOException {
        output.writeInt(bound == null ? -1 : bound.length);
        if (bound != null) output.write(bound);
    }

    private static byte[] readBound(DataInputStream input, BoundedInput counted, Limits limits) throws IOException {
        int length = input.readInt();
        if (length == -1) return null;
        if (length < 0 || length > limits.maxRowBytes() || length > counted.remaining() - 41) {
            throw new IOException("Invalid image bound length");
        }
        byte[] bound = new byte[length];
        input.readFully(bound);
        return bound;
    }

    private static void writeUuid(DataOutputStream output, UUID id) throws IOException {
        output.writeLong(id.getMostSignificantBits());
        output.writeLong(id.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream input) throws IOException { return new UUID(input.readLong(), input.readLong()); }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static final class Rows {
        private final Header header;
        private final Limits limits;
        private final byte[] groupPrefix;
        private final byte[] lower;
        private final byte[] upper;
        private Section previousSection;
        private UUID previousColumn;
        private byte[] previousKey;
        private long count;
        private boolean marker;
        private boolean usagePresent;
        private SessionAuthority sessionAuthority;
        private int sessionBindings;
        private final List<PreparedParticipant> prepared=new ArrayList<>();
        private final Map<UUID,AbortedParticipant> aborted=new HashMap<>();
        private final Map<UUID,RejectedParticipant> rejected=new HashMap<>();
        private final Map<UUID,SnapshotGate> gates=new HashMap<>();
        private long gateBytes,homeRecoveryBytes,homeCreditBytes;
        private int homeCreditCount;
        private final Map<UUID,ReplicatedRangeStore.HomeRecoveryCredit> homeCredits=new HashMap<>();
        private final Map<OperationId,HomeRecoveryReceipt> homeRecoveries=new HashMap<>();
        private final Map<UUID,CommittedParticipant> committed=new HashMap<>();
        private long preparedBytes,preparedScopeBytes;
        private long usageCount, usageBytes, bucketCount, bucketBytes, receiptCount, receiptBytes, epochFloor;

        Rows(Header header, Limits limits) {
            this.header = header;
            this.limits = limits;
            lower = header.interval().lowerInclusive();
            upper = header.interval().upperExclusive();
            UUID group = header.owner().groupId();
            groupPrefix = ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
        }

        void accept(Section section, UUID column, byte[] key, byte[] value) throws IOException {
            if ((long) key.length + value.length > limits.maxRowBytes()) throw new IOException("Image row exceeds byte limit");
            if (section == Section.USER) {
                if (column == null || !header.interval().columns().contains(column)) throw new IOException("Unknown image user column");
                if ((lower != null && Arrays.compareUnsigned(key, lower) < 0)
                        || (upper != null && Arrays.compareUnsigned(key, upper) >= 0)) {
                    throw new IOException("Image user key outside interval");
                }
            } else {
                if (column != null || key.length < groupPrefix.length
                        || !Arrays.equals(key, 0, groupPrefix.length, groupPrefix, 0, groupPrefix.length)) {
                    throw new IOException("Wrong image group metadata prefix");
                }
                if (section == Section.STATE && key.length > 16 && (key[16] == 0x13 || key[16] == 0x14)) {
                    boolean binding = key[16] == 0x14;
                    if (key.length != (binding ? 25 : 17)
                            || value.length != (binding ? RangeSessionControl.BIND_BYTES : RangeSessionControl.ADVANCE_BYTES)) {
                        throw new IOException("Invalid image range session control shape");
                    }
                    try {
                        var control = RangeSessionControl.decode(value);
                        if (control.binding() != binding) throw new IOException("Invalid image range session control kind");
                        if (binding) {
                            if (sessionAuthority == null) throw new IOException("Image binding has no range authority");
                            sessionAuthority.requireAtLeast(control.authority());
                            if (ByteBuffer.wrap(key, 17, 8).getLong() != control.grant().epoch()
                                    || control.grant().epoch() < epochFloor || ++sessionBindings > SessionEpochLedger.MAX_GRANTS) {
                                throw new IOException("Invalid image range session binding epoch or count");
                            }
                        } else sessionAuthority = control.authority();
                    } catch (IllegalArgumentException invalid) {
                        throw new IOException("Invalid image range session control", invalid);
                    }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x20) {
                    try {
                        var record=PreparedParticipant.decode(value); var preparation=record.preparation(); var plan=preparation.business(); var scope=preparation.scope();
                        byte[] expected=ByteBuffer.allocate(33).put(groupPrefix).put((byte)0x20).putLong(preparation.transactionId().getMostSignificantBits()).putLong(preparation.transactionId().getLeastSignificantBits()).array();
                        if (!Arrays.equals(key,expected) || !plan.groupId().equals(header.owner().groupId()) || plan.rangeEpoch()!=header.owner().epoch()
                                || !Arrays.equals(plan.schemaDigest(),header.schema()) || !preparation.registration().domainId().equals(header.interval().domainId())
                                || record.position().index()>header.position().index() || record.position().term()>header.position().term() || record.position().index()==header.position().index() && record.position().term()!=header.position().term()) throw new IOException("Prepared image identity/cut differs");
                        for (var span:scope.spans()) {
                            byte[] lo=span.lowerInclusive(),hi=span.upperExclusive();
                            if (!header.interval().columns().contains(span.columnId()) || lower!=null && (lo==null || Arrays.compareUnsigned(lo,lower)<0)
                                    || upper!=null && (span.isPoint() ? Arrays.compareUnsigned(lo,upper)>=0 : hi==null || Arrays.compareUnsigned(hi,upper)>0)) throw new IOException("Prepared image scope crosses ownership");
                        }
                        for (var mutation:plan.mutations()) if (mutation.kind()==Mutation.Kind.MERGE && !header.mergeColumns().contains(mutation.columnId())) throw new IOException("Prepared image merge operator is unavailable");
                        for (var old:prepared) if (old.preparation().sharesOperation(preparation)) throw new IOException("Prepared image identities/scopes overlap");
                        preparedBytes+=key.length+(long)value.length+ReplicatedRangeStore.FINALIZATION_RESERVATION; preparedScopeBytes+=RangeMutationHistory.accountedScopeBytes(scope);
                        if (prepared.size()>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS || preparedBytes>ReplicatedRangeStore.MAX_PREPARED_BYTES || preparedScopeBytes>ReplicatedRangeStore.MAX_PREPARED_BYTES) throw new IOException("Prepared image capacity exceeded");
                        prepared.add(record);
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid prepared image record",invalid); }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x21) {
                    try {
                        var abort=AbortedParticipant.decode(value); var finalization=abort.finalization(); var participant=finalization.participant();
                        byte[] expected=ByteBuffer.allocate(33).put(groupPrefix).put((byte)0x21).putLong(finalization.transactionId().getMostSignificantBits()).putLong(finalization.transactionId().getLeastSignificantBits()).array();
                        if (!Arrays.equals(key,expected) || !participant.groupId().equals(header.owner().groupId()) || participant.rangeEpoch()!=header.owner().epoch()
                                || !Arrays.equals(participant.schemaDigest(),header.schema()) || !finalization.decision().domainId().equals(header.interval().domainId())
                                || abort.position().index()>header.position().index() || abort.position().term()>header.position().term() || abort.position().index()==header.position().index() && abort.position().term()!=header.position().term()) throw new IOException("ABORT image identity/cut differs");
                        if (aborted.size()>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS) throw new IOException("ABORT image count exceeded");
                        aborted.put(finalization.transactionId(),abort);
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid ABORT image record",invalid); }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x22) {
                    try {
                        var commit=CommittedParticipant.decode(value); var finalization=commit.finalization(); var participant=finalization.participant();
                        byte[] expected=ByteBuffer.allocate(33).put(groupPrefix).put((byte)0x22).putLong(finalization.transactionId().getMostSignificantBits()).putLong(finalization.transactionId().getLeastSignificantBits()).array();
                        if (!Arrays.equals(key,expected) || !participant.groupId().equals(header.owner().groupId()) || participant.rangeEpoch()!=header.owner().epoch()
                                || !Arrays.equals(participant.schemaDigest(),header.schema()) || !finalization.decision().domainId().equals(header.interval().domainId())
                                || commit.position().index()>header.position().index() || commit.position().term()>header.position().term() || commit.position().index()==header.position().index() && commit.position().term()!=header.position().term()) throw new IOException("COMMIT image identity/cut differs");
                        if (committed.size()>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS) throw new IOException("COMMIT image count exceeded"); committed.put(finalization.transactionId(),commit);
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid COMMIT image record",invalid); }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x23) {
                    try {
                        var record=RejectedParticipant.decode(value);
                        var participant=record.registration().participants().stream().filter(p -> p.groupId().equals(record.participantGroup())).findFirst().orElseThrow();
                        byte[] expected=ByteBuffer.allocate(33).put(groupPrefix).put((byte)0x23).putLong(record.transactionId().getMostSignificantBits()).putLong(record.transactionId().getLeastSignificantBits()).array();
                        if (!Arrays.equals(key,expected) || !record.participantGroup().equals(header.owner().groupId()) || participant.rangeEpoch()!=header.owner().epoch()
                                || !Arrays.equals(participant.schemaDigest(),header.schema()) || !record.registration().domainId().equals(header.interval().domainId())
                                || record.position().index()>header.position().index() || record.position().term()>header.position().term() || record.position().index()==header.position().index() && record.position().term()!=header.position().term()) throw new IOException("Rejected image identity/cut differs");
                        if (rejected.size()>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS) throw new IOException("Rejected image count exceeded");
                        rejected.put(record.transactionId(),record);
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid rejected image record",invalid); }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x24) {
                    try {
                        var gate=SnapshotGate.decode(value); gate.requireCompletedCut(header.position()); var fence=gate.fence(); var participant=fence.participant(gate.participantGroup());
                        byte[] expected=ByteBuffer.allocate(33).put(groupPrefix).put((byte)0x24).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array();
                        if (!Arrays.equals(key,expected) || !gate.participantGroup().equals(header.owner().groupId()) || participant.rangeEpoch()!=header.owner().epoch()
                                || !Arrays.equals(participant.schemaDigest(),header.schema()) || !fence.domainId().equals(header.interval().domainId())) throw new IOException("Gate image identity/cut differs");
                        for (var span:participant.scope().spans()) {
                            byte[] lo=span.lowerInclusive(),hi=span.upperExclusive();
                            if (!header.interval().columns().contains(span.columnId()) || lower!=null && (lo==null || Arrays.compareUnsigned(lo,lower)<0)
                                    || upper!=null && (span.isPoint() ? Arrays.compareUnsigned(lo,upper)>=0 : hi==null || Arrays.compareUnsigned(hi,upper)>0)) throw new IOException("Gate image scope crosses ownership");
                        }
                        gateBytes+=ReplicatedRangeStore.snapshotGateBytes(fence);
                        if (gates.size()>=ReplicatedRangeStore.MAX_SNAPSHOT_GATES || gateBytes>ReplicatedRangeStore.MAX_SNAPSHOT_GATE_BYTES) throw new IOException("Gate image capacity exceeded");
                        gates.put(fence.fenceId(),gate);
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid gate image record",invalid); }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x25) {
                    try {
                        var receipt=HomeRecoveryReceipt.decode(value); receipt.requireCut(header.position()); var recovery=receipt.recovery(); var command=recovery.command(); var mutation=command.mutations().getFirst(); var id=recovery.operation();
                        byte[] expected=ByteBuffer.allocate(49).put(groupPrefix).put((byte)0x25).putLong(id.sessionId().getMostSignificantBits()).putLong(id.sessionId().getLeastSignificantBits()).putLong(id.epoch()).putLong(id.sequence()).array();
                        if (!Arrays.equals(key,expected) || !Arrays.equals(value,receipt.encode()) || !command.groupId().equals(header.owner().groupId()) || command.rangeEpoch()!=header.owner().epoch()
                                || !Arrays.equals(command.schemaDigest(),header.schema()) || header.interval().columns().size()!=1 || !header.interval().columns().contains(mutation.columnId())
                                || lower!=null && Arrays.compareUnsigned(mutation.key(),lower)<0 || upper!=null && Arrays.compareUnsigned(mutation.key(),upper)>=0) throw new IOException("HOME recovery image ownership/key/schema differs");
                        homeRecoveryBytes+=key.length+(long)value.length;
                        if (homeRecoveries.size()>=ReplicatedRangeStore.MAX_HOME_RECOVERIES || homeRecoveryBytes>ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES || homeRecoveries.putIfAbsent(id,receipt)!=null) throw new IOException("HOME recovery image capacity/identity exceeded");
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid HOME recovery image receipt",invalid); }
                }
                if (section==Section.STATE && key.length>16 && key[16]==0x26) {
                    try {
                        var credit=ReplicatedRangeStore.HomeRecoveryCredit.decode(value); credit.requireCut(header.position()); credit.requireOwner(header.owner(),header.interval(),header.schema());
                        byte[] expected=ByteBuffer.allocate(33).put(groupPrefix).put((byte)0x26).putLong(credit.current().transactionId().getMostSignificantBits()).putLong(credit.current().transactionId().getLeastSignificantBits()).array();
                        if (!Arrays.equals(key,expected)) throw new IOException("HOME promise image key differs");
                        if (!homeCredits.isEmpty()) { var previous=homeCredits.values().iterator().next(); if (!previous.cluster().equals(credit.cluster()) || !previous.database().equals(credit.database()) || !previous.column().equals(credit.column())) throw new IOException("HOME promise image context differs"); }
                        homeCreditCount+=credit.reservedCount(); homeCreditBytes+=credit.reservedBytes();
                        if (homeCreditCount>ReplicatedRangeStore.MAX_HOME_RECOVERIES || homeCreditBytes>ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES || homeCredits.putIfAbsent(credit.current().transactionId(),credit)!=null) throw new IOException("HOME promise image bound exceeded");
                    } catch (IllegalArgumentException invalid) { throw new IOException("Invalid HOME image promise",invalid); }
                }
                if (section == Section.STATE && key.length > 16 && key[16] == 0x12) {
                    throw new IOException("Image contains unsupported receipt accounting metadata");
                }
                if (section == Section.STATE && key.length > 16 && (key[16] == 0x10 || key[16] == 0x11)) {
                    boolean total = key[16] == 0x10;
                    if (key.length != (total ? 17 : 41) || (total ? value.length != 36 : value.length != 20)) {
                        throw new IOException("Invalid image receipt usage shape");
                    }
                    var usage = ByteBuffer.wrap(value);
                    if (usage.getInt() != 1) throw new IOException("Unsupported image receipt usage version");
                    long count = usage.getLong(), bytes = usage.getLong();
                    if (count < 0 || bytes < 0 || (count == 0 ? bytes != 0 : bytes / count < 84)) {
                        throw new IOException("Invalid image receipt usage totals");
                    }
                    if (total) {
                        usagePresent = true;
                        if (usage.getLong() != header.position().term() || usage.getLong() != header.position().index()) {
                            throw new IOException("Image receipt usage position differs from header");
                        }
                        usageCount = count;
                        usageBytes = bytes;
                    } else {
                        if (ByteBuffer.wrap(key, 17, 8).getLong() < 0
                                || ByteBuffer.wrap(key, 17, 8).getLong() < epochFloor || count == 0) {
                            throw new IOException("Invalid image receipt usage epoch bucket");
                        }
                        bucketCount = addUsage(bucketCount, count);
                        bucketBytes = addUsage(bucketBytes, bytes);
                    }
                }
                if (section == Section.RECEIPTS) {
                    if (key.length != 48 || value.length < 36 || value.length > 36 + 2 * 1024 * 1024
                            || ByteBuffer.wrap(value).getInt() != 1 || ByteBuffer.wrap(key, 32, 8).getLong() < epochFloor
                            || ByteBuffer.wrap(key, 40, 8).getLong() < 0) {
                        throw new IOException("Invalid image receipt row");
                    }
                    var receipt=ByteBuffer.wrap(key); receipt.position(16); var id=new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(receipt.getLong(),receipt.getLong()),receipt.getLong(),receipt.getLong());
                    for (var commit:committed.values()) if (commit.ownsOperation(id)) throw new IOException("COMMIT image identity also has ordinary receipt");
                    for (var abort:aborted.values()) if (abort.ownsOperation(id)) throw new IOException("ABORT image identity also has ordinary receipt");
                    for (var record:rejected.values()) if (record.ownsOperation(id)) throw new IOException("Rejected image identity also has ordinary receipt");
                    for (var gate:gates.values()) if (gate.ownsOperation(id)) throw new IOException("Gate image identity also has ordinary receipt");
                    if (homeRecoveries.containsKey(id)) throw new IOException("HOME recovery image identity also has ordinary receipt");
                    receiptCount = addUsage(receiptCount, 1);
                    receiptBytes = addUsage(receiptBytes, (long) key.length + value.length);
                }
                if (section == Section.STATE && key.length > 16 && key[16] == 3) {
                    if (key.length != 17 || value.length != 12) throw new IOException("Invalid image epoch floor shape");
                    var epochValue = ByteBuffer.wrap(value);
                    if (epochValue.getInt() != 1 || (epochFloor = epochValue.getLong()) < 0) throw new IOException("Invalid image epoch floor");
                }
                if (section == Section.STATE && key.length > 16 && key[16] == 2) {
                    if (key.length != 41 || value.length != 12 || ByteBuffer.wrap(key, 33, 8).getLong() < 0) {
                        throw new IOException("Invalid image session floor shape");
                    }
                    var floorValue = ByteBuffer.wrap(value);
                    if (floorValue.getInt() != 1) throw new IOException("Unsupported image session floor version");
                    long floor = floorValue.getLong();
                    if (floor < 0 || floor == Long.MAX_VALUE) throw new IOException("Invalid image session floor sequence");
                }
                if (section == Section.STATE && key.length == 16) {
                    if (value.length != 20) throw new IOException("Invalid image applied marker length");
                    var markerValue = ByteBuffer.wrap(value);
                    if (markerValue.getInt() != 1 || markerValue.getLong() != header.position().term()
                            || markerValue.getLong() != header.position().index()) {
                        throw new IOException("Image applied marker differs from header");
                    }
                    marker = true;
                }
            }
            if (previousSection != null) {
                int order = Integer.compare(previousSection.ordinal(), section.ordinal());
                if (order == 0 && section == Section.USER) order = UUID_ORDER.compare(previousColumn, column);
                if (order > 0 || (order == 0 && Arrays.compareUnsigned(previousKey, key) >= 0)) {
                    throw new IOException("Noncanonical image row order");
                }
            }
            previousSection = section;
            previousColumn = column;
            previousKey = key.clone();
            count++;
        }

        HomeContext homeContext(HomeContext required) throws IOException {
            HomeContext context=required;
            for (var credit:homeCredits.values()) {
                var actual=new HomeContext(credit.cluster(),credit.database(),credit.column());
                if (context!=null && !context.equals(actual)) throw new IOException("Qualified HOME image context differs from promises"); context=actual;
            }
            if (context!=null && !header.interval().columns().equals(Set.of(context.column()))) throw new IOException("Qualified HOME image column differs from ownership"); return context;
        }
        void requireHomeUser(HomeContext context,UUID column,byte[] key,byte[] value,Set<UUID> matched) throws IOException {
            if (!context.column().equals(column) || key.length!=16 || value.length>it.cavallium.rockserver.core.common.distributed.TransactionStatus.MAX_BYTES) throw new IOException("Invalid qualified HOME image user row");
            var status=it.cavallium.rockserver.core.common.distributed.TransactionStatus.decode(value); var id=status.transactionId();
            byte[] expected=ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
            if (!status.clusterId().equals(context.cluster()) || !status.databaseId().equals(context.database()) || !Arrays.equals(key,expected)) throw new IOException("Qualified HOME image user identity differs");
            var credit=homeCredits.get(id);
            if (ReplicatedRangeStore.HomeRecoveryCredit.remaining(status)>0 && (credit==null || !credit.current().equals(status)) || ReplicatedRangeStore.HomeRecoveryCredit.remaining(status)==0 && credit!=null) throw new IOException("Unfinished HOME image user row lacks exact promise");
            if (credit!=null) matched.add(id);
        }

        private static long addUsage(long a, long b) throws IOException {
            try { return Math.addExact(a, b); }
            catch (ArithmeticException overflow) { throw new IOException("Image receipt usage overflow", overflow); }
        }

        void finish() throws IOException {
            if (!marker) throw new IOException("Image has no applied marker");
            if (homeRecoveries.size()+homeCreditCount>ReplicatedRangeStore.MAX_HOME_RECOVERIES || homeRecoveryBytes+homeCreditBytes>ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES) throw new IOException("HOME image actual/future capacity exceeded");
            long terminalOnly=0;
            for (var record:prepared) {
                var abort=aborted.get(record.preparation().transactionId());
                if (abort!=null) { try {
                    abort.finalization().requirePrepared(record);
                    if (abort.position().index()<=record.position().index() || abort.position().term()<record.position().term()) throw new IllegalArgumentException("ABORT precedes native preparation");
                } catch (IllegalArgumentException invalid) { throw new IOException("ABORT/preparation binding differs",invalid); } }
                else if (!committed.containsKey(record.preparation().transactionId())) for (var other:prepared) if (other!=record && !committed.containsKey(other.preparation().transactionId()) && !aborted.containsKey(other.preparation().transactionId()) && other.preparation().scope().intersects(record.preparation().scope())) throw new IOException("Active prepared image scopes overlap");
            }
            for (var abort:aborted.values()) {
                if (abort.finalization().participant().prepared()!=null && prepared.stream().noneMatch(p -> p.preparation().transactionId().equals(abort.finalization().transactionId()))) throw new IOException("ABORT image vote lacks native preparation");
                for (var p:prepared) if (!p.preparation().transactionId().equals(abort.finalization().transactionId()) && (abort.ownsOperation(p.preparation().operation()) || abort.ownsOperation(p.preparation().business().operationId()))) throw new IOException("ABORT/preparation image identity reused");
                for (var other:aborted.values()) if (other!=abort && (other.ownsOperation(abort.finalization().operation()) || other.ownsOperation(abort.finalization().decision().operation()))) throw new IOException("ABORT image identity reused");
            }
            for (var commit:committed.values()) {
                var id=commit.finalization().transactionId(); if (aborted.containsKey(id)) throw new IOException("COMMIT and ABORT image decisions coexist");
                var preparation=prepared.stream().filter(p -> p.preparation().transactionId().equals(id)).findFirst().orElseThrow(() -> new IOException("COMMIT image has no retained preparation"));
                try { commit.finalization().requirePrepared(preparation); } catch (IllegalArgumentException invalid) { throw new IOException("COMMIT image preparation differs",invalid); }
                for (var p:prepared) if (p!=preparation && (commit.ownsOperation(p.preparation().operation()) || commit.ownsOperation(p.preparation().business().operationId()))) throw new IOException("COMMIT/preparation image identity reused");
                for (var abort:aborted.values()) if (abort.ownsOperation(commit.finalization().operation()) || abort.ownsOperation(commit.finalization().decision().operation())) throw new IOException("COMMIT/ABORT image identity reused");
                for (var other:committed.values()) if (other!=commit && (other.ownsOperation(commit.finalization().operation()) || other.ownsOperation(commit.finalization().decision().operation()))) throw new IOException("COMMIT image identity reused");
            }
            for (var record:rejected.values()) {
                var id=record.transactionId(); if (committed.containsKey(id)) throw new IOException("COMMIT and rejection image decisions coexist");
                for (var p:prepared) if (p.preparation().transactionId().equals(id) || record.ownsOperation(p.preparation().operation()) || record.ownsOperation(p.preparation().business().operationId())) throw new IOException("Positive/negative image ownership reused");
                for (var other:rejected.values()) if (other!=record && (other.ownsOperation(record.operation()) || other.ownsOperation(record.registration().operation()))) throw new IOException("Rejected image identity reused");
                for (var abort:aborted.values()) {
                    if (abort.finalization().transactionId().equals(id)) {
                        try { record.requireFinalization(abort.finalization()); } catch (IllegalArgumentException invalid) { throw new IOException("ABORT/rejection image binding differs",invalid); }
                    } else if (abort.ownsOperation(record.operation()) || abort.ownsOperation(record.registration().operation())) throw new IOException("ABORT/rejection image identity reused");
                }
                for (var commit:committed.values()) if (commit.ownsOperation(record.operation()) || commit.ownsOperation(record.registration().operation())) throw new IOException("COMMIT/rejection image identity reused");
            }
            var terminalIds=new HashSet<>(aborted.keySet()); terminalIds.addAll(rejected.keySet());
            for (var id:terminalIds) if (prepared.stream().noneMatch(p -> p.preparation().transactionId().equals(id))) terminalOnly++;
            if (prepared.size()+terminalOnly>ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS || preparedBytes+terminalOnly*ReplicatedRangeStore.FINALIZATION_RESERVATION>ReplicatedRangeStore.MAX_PREPARED_BYTES) throw new IOException("Participant image capacity exceeded");

            for (var gate:gates.values()) {
                var participant=gate.fence().participant(gate.participantGroup());
                for (var id:List.of(participant.installOperation(),participant.releaseOperation())) {
                    for (var p:prepared) if (p.preparation().ownsOperation(id)) throw new IOException("Gate/preparation image identity reused");
                    for (var r:rejected.values()) if (r.ownsOperation(id)) throw new IOException("Gate/rejection image identity reused");
                    for (var abort:aborted.values()) if (abort.ownsOperation(id)) throw new IOException("Gate/ABORT image identity reused");
                    for (var commit:committed.values()) if (commit.ownsOperation(id)) throw new IOException("Gate/COMMIT image identity reused");
                    for (var other:gates.values()) if (other!=gate && other.ownsOperation(id)) throw new IOException("Gate image identity reused");
                }
                if (gate.active()) for (var other:gates.values()) if (other!=gate && other.active() && participant.scope().intersects(other.fence().participant(other.participantGroup()).scope())) throw new IOException("Active gate image scopes overlap");
            }

            for (var recovery:homeRecoveries.values()) {
                var id=recovery.recovery().operation();
                for (var p:prepared) if (p.preparation().ownsOperation(id)) throw new IOException("HOME recovery/preparation image identity reused");
                for (var r:rejected.values()) if (r.ownsOperation(id)) throw new IOException("HOME recovery/rejection image identity reused");
                for (var r:aborted.values()) if (r.ownsOperation(id)) throw new IOException("HOME recovery/ABORT image identity reused");
                for (var r:committed.values()) if (r.ownsOperation(id)) throw new IOException("HOME recovery/COMMIT image identity reused");
                for (var r:gates.values()) if (r.ownsOperation(id)) throw new IOException("HOME recovery/gate image identity reused");
            }

            if (sessionAuthority != null && sessionAuthority.minimumEpoch() > epochFloor) {
                throw new IOException("Image range session authority exceeds coarse epoch floor");
            }
            if (!usagePresent || usageCount != bucketCount || usageBytes != bucketBytes
                    || usageCount != receiptCount || usageBytes != receiptBytes) {
                throw new IOException("Image receipt usage differs from retained receipts/buckets");
            }
        }
    }

    private static final class BoundedOutput extends OutputStream {
        private final OutputStream output;
        private final long limit;
        private long count;
        BoundedOutput(OutputStream output, long limit) { this.output = output; this.limit = limit; }
        private void reserve(int length) throws IOException {
            if (length > limit - count) throw new IOException("Image exceeds byte limit");
            count += length;
        }
        @Override public void write(int value) throws IOException { reserve(1); output.write(value); }
        @Override public void write(byte[] bytes, int offset, int length) throws IOException { reserve(length); output.write(bytes, offset, length); }
        @Override public void flush() throws IOException { output.flush(); }
    }

    private static final class BoundedInput extends InputStream {
        private final InputStream input;
        private final long limit;
        private long count;
        BoundedInput(InputStream input, long limit) { this.input = input; this.limit = limit; }
        long remaining() { return limit - count; }
        @Override public int read() throws IOException {
            int value = input.read();
            if (value != -1 && ++count > limit) throw new IOException("Image grew while reading");
            return value;
        }
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = input.read(bytes, offset, length);
            if (read > 0) {
                if (read > limit - count) throw new IOException("Image grew while reading");
                count += read;
            }
            return read;
        }
    }
}
