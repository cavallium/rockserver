package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeRangeSessionImageTest {
    private static final UUID GROUP = new UUID(0, 1), CLUSTER = new UUID(0, 2), DATABASE = new UUID(0, 3);
    private static final UUID DOMAIN = new UUID(0, 4), COLUMN = new UUID(0, 5), SESSION = new UUID(0, 6);
    private static final SessionAuthority ISSUED = new SessionAuthority(CLUSTER, DATABASE, 1, 0, 1);
    private static final SessionAuthority CURRENT = new SessionAuthority(CLUSTER, DATABASE, 2, 20, 1);
    private static final NativeRangeImage.Limits LIMITS = new NativeRangeImage.Limits(256, 1024 * 1024, 1);
    @TempDir(factory = DiskTestDirectory.class) Path root;
    private record Row(byte[] key, byte[] value) {}

    @Test void authorityOnlyAndLogicallyExpiredBindingsRemainValidAboveCoarseFloor() throws Exception {
        assertValid(0, List.of());
        assertValid(1, List.of(authority(CURRENT)));
        var expired = binding(1);
        assertFalse(CURRENT.permits(RangeSessionControl.decode(expired.value()).grant()));
        assertValid(1, List.of(authority(CURRENT), expired));
        // Coarse fences can advance beyond authority.minimumEpoch without a newer authority cut.
        assertValid(2, List.of(authority(CURRENT), binding(2)));
    }

    @Test void malformedOrUnanchoredBindingsAndAuthorityFloorMismatchAreRejected() throws Exception {
        assertInvalid(1, List.of(binding(1)));
        assertInvalid(1, List.of(new Row(key((byte) 0x13, null), binding(1).value())));
        assertInvalid(1, List.of(authority(CURRENT), new Row(key((byte) 0x14, 1L), authority(ISSUED).value())));
        assertInvalid(1, List.of(authority(CURRENT), new Row(key((byte) 0x14, 2L), binding(1).value())));
        assertInvalid(2, List.of(authority(CURRENT), binding(1)));
        assertInvalid(0, List.of(authority(CURRENT)));
        assertInvalid(1, List.of(authority(new SessionAuthority(CLUSTER, DATABASE, 2, 20, 2))));
        for (byte tag : new byte[] {0x13, 0x14}) {
            Row row = tag == 0x13 ? authority(CURRENT) : binding(1);
            assertInvalid(1, List.of(new Row(Arrays.copyOf(row.key(), row.key().length + 1), row.value())));
            byte[] version = row.value().clone(); version[3] = 2;
            assertInvalid(1, tag == 0x13 ? List.of(new Row(row.key(), version))
                    : List.of(authority(CURRENT), new Row(row.key(), version)));
        }
        byte[] window = binding(1).value().clone(); ByteBuffer.wrap(window).putInt(window.length - 4, 0);
        assertInvalid(1, List.of(authority(CURRENT), new Row(key((byte) 0x14, 1L), window)));
    }

    @Test void bindingAuthorityMustBeDominatedAndEpochBindingsRemainUniqueAndBounded() throws Exception {
        var future = new SessionAuthority(CLUSTER, DATABASE, 3, 0, 1);
        var foreign = new SessionAuthority(new UUID(1, 1), DATABASE, 1, 0, 1);
        for (var authority : List.of(future, foreign)) {
            var control = new RangeSessionControl(authority, new SessionEpochLedger.Grant(1, SESSION, 0, 10), 1);
            assertInvalid(1, List.of(authority(CURRENT), new Row(key((byte) 0x14, 1L), control.encode())));
        }
        assertInvalid(1, List.of(authority(CURRENT), binding(1), binding(1)));
        var rows = new ArrayList<Row>(); rows.add(authority(CURRENT));
        for (int epoch = 1; epoch <= SessionEpochLedger.MAX_GRANTS; epoch++) rows.add(binding(epoch));
        assertValid(1, rows);
        rows.add(binding(SessionEpochLedger.MAX_GRANTS + 1));
        assertInvalid(1, rows);
    }

    private static Row acknowledgement(long epoch,int id){return new Row(ByteBuffer.allocate(41).putLong(0).putLong(1).put((byte)2).putLong(epoch).putLong(0).putLong(id).array(),ByteBuffer.allocate(8).putLong(0).array());}
    private static Row acknowledgementCount(long count){return new Row(key((byte)0x15,null),ByteBuffer.allocate(8).putLong(count).array());}
    @Test void orphanReplayFloorsAreLegalButCounterEpochAndFinitePopulationAreRequired() throws Exception {
        assertValid(1,List.of(acknowledgement(1,1),acknowledgementCount(1)));
        assertInvalid(1,List.of(acknowledgement(1,1)));
        assertInvalid(1,List.of(acknowledgement(1,1),acknowledgementCount(0)));
        assertInvalid(2,List.of(acknowledgement(1,1),acknowledgementCount(1)));
        var rows=new ArrayList<Row>();for(int n=0;n<=it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.MAX_SESSION_ACK_NAMESPACES;n++)rows.add(acknowledgement(1,n));rows.add(acknowledgementCount(rows.size()));
        var large=image(1,rows);var bounded=new NativeRangeImage.Limits(256,8L*1024*1024,1);
        var failure=assertThrows(IOException.class,()->NativeRangeImage.inspect(large,bounded));assertTrue(failure.getMessage().contains("session floor"),failure.toString());
        var valid=image(1,List.of(acknowledgement(1,1),acknowledgement(1,2),acknowledgementCount(2)));
        assertThrows(IOException.class,()->NativeRangeImage.inspectSessionMetadata(valid,LIMITS,1));
    }

    private void assertValid(long floor, List<Row> controls) throws Exception {
        Path image = image(floor, controls);
        assertDoesNotThrow(() -> NativeRangeImage.inspect(image, LIMITS));
        assertDoesNotThrow(() -> NativeRangeImage.visit(image, LIMITS, (section, column, key, value) -> {}));
    }
    private void assertInvalid(long floor, List<Row> controls) throws Exception {
        Path image = image(floor, controls);
        assertThrows(IOException.class, () -> NativeRangeImage.inspect(image, LIMITS));
    }
    private static Row authority(SessionAuthority authority) {
        return new Row(key((byte) 0x13, null), new RangeSessionControl(authority, null, 0).encode());
    }
    private static Row binding(long epoch) {
        return new Row(key((byte) 0x14, epoch), new RangeSessionControl(ISSUED,
                new SessionEpochLedger.Grant(epoch, SESSION, 0, 10), 1).encode());
    }
    private static byte[] key(byte tag, Long epoch) {
        var key = ByteBuffer.allocate(epoch == null ? 17 : 25).putLong(0).putLong(1).put(tag);
        if (epoch != null) key.putLong(epoch);
        return key.array();
    }
    private Path image(long floor, List<Row> controls) throws Exception {
        var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes);
        output.writeInt(0x4e52494d); uuid(output, GROUP); output.writeLong(1);
        output.write(new byte[32]); output.writeLong(1); output.writeLong(0); uuid(output, DOMAIN);
        output.writeInt(1); uuid(output, COLUMN); output.writeInt(-1); output.writeInt(-1); output.writeInt(0);
        var rows = new ArrayList<Row>();
        rows.add(new Row(ByteBuffer.allocate(16).putLong(0).putLong(1).array(), ByteBuffer.allocate(16).putLong(1).putLong(0).array()));
        rows.add(new Row(key((byte)0x10,null),ByteBuffer.allocate(32).putLong(0).putLong(0).putLong(1).putLong(0).array()));
        if (floor != 0) rows.add(new Row(key((byte) 3, null), ByteBuffer.allocate(8).putLong(floor).array()));
        rows.addAll(controls); rows.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
        for (Row row : rows) {
            output.writeByte(2); output.writeInt(row.key().length); output.writeInt(row.value().length);
            output.write(row.key()); output.write(row.value());
        }
        output.writeByte(0); output.writeLong(rows.size()); output.flush();
        bytes.write(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        Path image = Files.createTempFile(root, "session-image-", ".image"); Files.write(image, bytes.toByteArray());
        return image;
    }
    private static void uuid(DataOutputStream output, UUID uuid) throws IOException {
        output.writeLong(uuid.getMostSignificantBits()); output.writeLong(uuid.getLeastSignificantBits());
    }
}
