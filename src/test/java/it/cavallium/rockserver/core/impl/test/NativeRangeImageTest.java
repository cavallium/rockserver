package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NativeRangeImageTest {
    static { RocksDB.loadLibrary(); }
    private static final UUID GROUP = new UUID(1, 1);
    private static final UUID OTHER = new UUID(1, 2);
    private static final UUID DOMAIN = new UUID(2, 1);
    private static final UUID A = new UUID(3, 1);
    private static final UUID B = new UUID(3, 2);
    private static final UUID SESSION = new UUID(4, 1);
    private static final Limits LIMITS = new Limits(4096, 1024 * 1024, 4);
    @TempDir(factory = DiskTestDirectory.class) Path directory;

    private record Row(Section section, UUID column, byte[] key, byte[] value) {}
    private byte[] key(int value) { return new byte[]{(byte) value}; }
    private byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private RangeOwnership.Interval interval() {
        return new RangeOwnership.Interval(DOMAIN, Set.of(A, B), key(0x7f), key(0xff));
    }
    private DistributedCommand command(UUID group, long sequence, Mutation... mutations) {
        return new DistributedCommand(group, 7, new byte[32], new OperationId(SESSION, 1, sequence), List.of(mutations));
    }
    private Mutation put(UUID column, int key, String value) {
        return new Mutation(Mutation.Kind.PUT, column, key(key), bytes(value));
    }

    @Test void admittedImageCanSpanManyBurstsAndHashesThroughTheSameGate()throws Exception {
        try(var pool=new Pool(directory.resolve("admitted-pool"))){
            pool.store.applyNoop(new ApplyPosition(1,0));pool.db.put(pool.handles.get(1),key(0x80),new byte[3*1024*1024]);
            var limits=new Limits(4*1024*1024,8*1024*1024,4);var calls=new java.util.concurrent.atomic.AtomicInteger();var charged=new java.util.concurrent.atomic.AtomicLong();
            IoAdmission gate=new IoAdmission(){
                public void beforeBytes(int bytes){assertTrue(bytes>0 && bytes<=1024*1024);calls.incrementAndGet();charged.addAndGet(bytes);}
                public AutoCloseable reserveMetadata(int rawBytes){throw new AssertionError("USER image must not reserve typed metadata");}
            };
            try(var pin=pool.store.capture()){
                var path=directory.resolve("admitted.image");var written=NativeRangeImage.writeAdmitted(path,pin,interval(),limits,gate);
                assertTrue(written.bytes()>3*1024*1024);assertTrue(calls.get()>3);assertTrue(charged.get()>=written.bytes());
                long before=charged.get();var inspected=NativeRangeImage.inspectAdmitted(path,limits,gate);assertArrayEquals(written.sha256(),inspected.sha256());assertTrue(charged.get()-before>=written.bytes());
                before=charged.get();assertArrayEquals(MessageDigest.getInstance("MD5").digest(Files.readAllBytes(path)),NativeRangeImage.md5Admitted(path,gate));assertTrue(charged.get()-before>=written.bytes());
            }
        }
    }
    @Test void deniedImageIoPublishesNothingAndLeavesTheOriginalPinReusable()throws Exception {
        try(var pool=new Pool(directory.resolve("denied-pool"))){pool.store.applyNoop(new ApplyPosition(1,0));
            try(var pin=pool.store.capture()){
                var path=directory.resolve("denied.image");assertThrows(IOException.class,()->NativeRangeImage.writeAdmitted(path,pin,interval(),LIMITS,bytes->{throw new IOException("controlled admission denial");}));assertFalse(Files.exists(path));
                var calls=new java.util.concurrent.atomic.AtomicInteger();assertThrows(IOException.class,()->NativeRangeImage.writeAdmitted(path,pin,interval(),LIMITS,bytes->{if(calls.incrementAndGet()>1)throw new IOException("controlled write denial");}));assertFalse(Files.exists(path));
                var written=NativeRangeImage.writeAdmitted(path,pin,interval(),LIMITS,IoAdmission.UNLIMITED);assertArrayEquals(written.sha256(),NativeRangeImage.inspect(path,LIMITS).sha256());
            }
        }
    }

    @Test void typedMetadataDenialPrecedesWriterDecodeAndReaderValueAllocation()throws Exception {
        int registrationBytes=1024*1024;
        byte[] registrationKey=ByteBuffer.allocate(17).putLong(GROUP.getMostSignificantBits()).putLong(GROUP.getLeastSignificantBits()).put((byte)0x72).array();
        var requests=new java.util.concurrent.atomic.AtomicInteger();
        IoAdmission deny=new IoAdmission(){
            public void beforeBytes(int bytes){}
            public AutoCloseable reserveMetadata(int rawBytes)throws IOException {
                assertEquals(registrationBytes,rawBytes);requests.incrementAndGet();throw new IOException("metadata denied before decode");
            }
        };
        try(var pool=new Pool(directory.resolve("metadata-denied-pool"))){
            pool.store.applyNoop(new ApplyPosition(1,0));
            pool.db.put(pool.handles.get(3),registrationKey,new byte[registrationBytes]);
            try(var pin=pool.store.capture()){
                var path=directory.resolve("metadata-denied.image");
                assertEquals("metadata denied before decode",assertThrows(IOException.class,()->NativeRangeImage.writeAdmitted(path,pin,interval(),new Limits(4096,4*1024*1024,4),deny)).getMessage());
                assertFalse(Files.exists(path));
            }
        }
        // An otherwise valid header followed by a registration payload whose contents must never be decoded.
        byte[] original=imageWithOneUserRow();int end=original.length-41;
        var bytes=new java.io.ByteArrayOutputStream();bytes.write(original,0,end);
        var output=new java.io.DataOutputStream(bytes);output.writeByte(Section.STATE.ordinal()+1);output.writeInt(17);output.writeInt(registrationBytes);output.write(registrationKey);output.write(new byte[registrationBytes]);output.write(original,end,41);
        byte[] forged=bytes.toByteArray();byte[] checksum=MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(forged,forged.length-32));System.arraycopy(checksum,0,forged,forged.length-32,32);
        var path=directory.resolve("metadata-reader-denied.image");Files.write(path,forged);
        assertEquals("metadata denied before decode",assertThrows(IOException.class,()->NativeRangeImage.inspectAdmitted(path,new Limits(4096,4*1024*1024,4),deny)).getMessage());
        assertEquals(2,requests.get());
    }

    @Test
    void exportsOneNativeCutWithExactSortedIntervalsAndOriginalReceipts() throws Exception {
        try (Pool pool = new Pool(directory.resolve("pool"))) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            var seed = command(GROUP, 0, put(A, 0x7f, "old"), put(A, 0x80, "signed"), put(B, 0x80, "paired"));
            pool.store.apply(new ApplyPosition(1, 1), seed, bytes("original-result"));
            pool.db.put(pool.handles.get(1), key(1), bytes("outside-low"));
            pool.db.put(pool.handles.get(2), key(0xff), bytes("outside-high"));
            var other = pool.store(OTHER);
            other.applyNoop(new ApplyPosition(2, 0));
            other.apply(new ApplyPosition(2, 1), command(OTHER, 0, put(A, 2, "other-range")), bytes("other-result"));
            try (var pin = pool.store.capture()) {
                pool.store.apply(new ApplyPosition(1, 2), command(GROUP, 1,
                        new Mutation(Mutation.Kind.MERGE, A, key(0x7f), bytes("later")),
                        new Mutation(Mutation.Kind.DELETE, B, key(0x80), null), put(A, 0x90, "new")));
                Path image = directory.resolve("cut.image");
                Manifest written = NativeRangeImage.write(image, pin, interval(), LIMITS);
                Manifest inspected = NativeRangeImage.inspect(image, LIMITS);
                var rows = new ArrayList<Row>();
                NativeRangeImage.visit(image, LIMITS, (section, column, key, value) -> rows.add(new Row(section, column, key, value)));
                assertDoesNotThrow(() -> NativeRangeImage.visit(image, LIMITS,
                        (section, column, key, value) -> Arrays.fill(key, (byte) 0xff)));
                assertEquals(new RangeOwnership.Owner(GROUP, 7), written.owner());
                assertEquals(interval(), written.interval());
                assertEquals(new ApplyPosition(1, 1), written.position());
                assertEquals(Set.of(A), written.deterministicMergeColumns());
                assertEquals(8, written.rows());
                assertEquals(Files.size(image), written.bytes());
                assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(image)), written.sha256());
                assertArrayEquals(written.sha256(), inspected.sha256());
                assertEquals(A, rows.get(0).column());
                assertArrayEquals(key(0x7f), rows.get(0).key());
                assertArrayEquals(bytes("old"), rows.get(0).value());
                assertArrayEquals(key(0x80), rows.get(1).key());
                assertArrayEquals(bytes("signed"), rows.get(1).value());
                assertEquals(B, rows.get(2).column());
                assertArrayEquals(bytes("paired"), rows.get(2).value());
                assertEquals(Section.STATE, rows.get(3).section());
                assertNull(rows.get(3).column());
                assertEquals(Section.STATE, rows.get(4).section());
                assertEquals(Section.STATE, rows.get(5).section());
                assertEquals(Section.STATE, rows.get(6).section());
                assertEquals(Section.RECEIPTS, rows.get(7).section());
                byte[] receiptKey = ByteBuffer.allocate(48).putLong(GROUP.getMostSignificantBits())
                        .putLong(GROUP.getLeastSignificantBits()).putLong(1).putLong(SESSION.getMostSignificantBits())
                        .putLong(SESSION.getLeastSignificantBits()).putLong(0).array();
                assertArrayEquals(receiptKey, rows.get(7).key());
                assertArrayEquals(bytes("original-result"), Arrays.copyOfRange(rows.get(7).value(), 32, rows.get(7).value().length));
                Path sameCut = directory.resolve("same.image");
                NativeRangeImage.write(sameCut, pin, interval(), LIMITS);
                assertArrayEquals(Files.readAllBytes(image), Files.readAllBytes(sameCut));
                written.schemaDigest()[0] = 99;
                written.sha256()[0] = 99;
                assertArrayEquals(inspected.schemaDigest(), written.schemaDigest());
                assertArrayEquals(inspected.sha256(), written.sha256());
                assertArrayEquals(bytes("old,later"), pool.db.get(pool.handles.get(1), key(0x7f)));
            }
        }
    }

    @Test
    void enforcesBudgetsBeforePublishingAndCleansPrivateTemporaryFiles() throws Exception {
        try (Pool pool = new Pool(directory.resolve("pool"))) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            try (var pin = pool.store.capture()) {
                Path image = directory.resolve("reference.image");
                Manifest manifest = NativeRangeImage.write(image, pin, interval(), LIMITS);
                var exact = new Limits(4096, manifest.bytes(), 2);
                assertEquals(manifest.bytes(), NativeRangeImage.inspect(image, exact).bytes());
                Path exactImage = directory.resolve("exact.image");
                NativeRangeImage.write(exactImage, pin, interval(), exact);
                var tooSmall = new Limits(4096, manifest.bytes() - 1, 2);
                Path refused = directory.resolve("refused.image");
                assertThrows(IOException.class, () -> NativeRangeImage.write(refused, pin, interval(), tooSmall));
                assertFalse(Files.exists(refused));
                assertThrows(IOException.class, () -> NativeRangeImage.inspect(image, tooSmall));
                assertThrows(IOException.class, () -> NativeRangeImage.write(refused, pin, interval(), new Limits(35, 100000, 2)));
                assertThrows(IOException.class, () -> NativeRangeImage.inspect(image, new Limits(35, 100000, 2)));
                assertThrows(IOException.class, () -> NativeRangeImage.inspect(image, new Limits(4096, 100000, 1)));
                try (var files = Files.list(directory)) {
                    assertFalse(files.anyMatch(file -> file.getFileName().toString().startsWith(".native-range-building-")));
                }
            }
        }
    }

    @Test
    void rejectsMissingPositionWrongColumnsAndExistingDestination() throws Exception {
        try (Pool pool = new Pool(directory.resolve("pool"))) {
            Path image = directory.resolve("cut.image");
            try (var empty = pool.store.capture()) {
                assertThrows(IllegalArgumentException.class, () -> NativeRangeImage.write(image, empty, interval(), LIMITS));
            }
            pool.store.applyNoop(new ApplyPosition(1, 0));
            try (var pin = pool.store.capture()) {
                var wrong = new RangeOwnership.Interval(DOMAIN, Set.of(A), null, null);
                assertThrows(IllegalArgumentException.class, () -> NativeRangeImage.write(image, pin, wrong, LIMITS));
                var clipped = new RangeOwnership.Interval(DOMAIN, Set.of(A, B), key(0x80), key(0xf0));
                assertThrows(IllegalArgumentException.class, () -> NativeRangeImage.write(image, pin, clipped, LIMITS));
                Files.write(image, bytes("existing destination"));
                assertThrows(FileAlreadyExistsException.class, () -> NativeRangeImage.write(image, pin, interval(), LIMITS));
                assertArrayEquals(bytes("existing destination"), Files.readAllBytes(image));
            }
        }
    }

    @Test
    void rejectsEveryTruncationCorruptChecksumAndTrailingBytes() throws Exception {
        byte[] valid = imageWithOneUserRow();
        Path damaged = directory.resolve("damaged.image");
        for (int length = 0; length < valid.length; length++) {
            Files.write(damaged, Arrays.copyOf(valid, length));
            assertThrows(IOException.class, () -> NativeRangeImage.inspect(damaged, LIMITS), "length=" + length);
        }
        byte[] corrupted = valid.clone();
        corrupted[32] ^= 1; // Schema byte: structural validation succeeds, checksum must reject it.
        Files.write(damaged, corrupted);
        assertThrows(IOException.class, () -> NativeRangeImage.inspect(damaged, LIMITS));
        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        Files.write(damaged, trailing);
        assertThrows(IOException.class, () -> NativeRangeImage.inspect(damaged, LIMITS));
    }

    @Test
    void rejectsResealedIdentityMarkerBoundsAndLengthForgeries() throws Exception {
        byte[] valid = imageWithOneUserRow();
        int firstRow = headerEnd(valid);
        int userKey = firstRow + 1 + 16 + 4 + 4;
        int userValueLength = ByteBuffer.wrap(valid).getInt(firstRow + 21);
        int stateRow = userKey + 1 + userValueLength;
        int stateKey = stateRow + 9;
        int stateValue = stateKey + 16;
        int[][] badInts = {{0, 2}, {92, Integer.MAX_VALUE}, {firstRow + 17, Integer.MAX_VALUE},
                {firstRow + 21, -1}};
        for (int[] bad : badInts) {
            byte[] changed = valid.clone();
            ByteBuffer.wrap(changed).putInt(bad[0], bad[1]);
            rejectResealed(changed);
        }
        byte[] position = valid.clone();
        ByteBuffer.wrap(position).putLong(68, 9);
        rejectResealed(position);
        byte[] group = valid.clone();
        group[4] ^= 1;
        rejectResealed(group);
        byte[] metadata = valid.clone();
        metadata[stateKey] ^= 1;
        rejectResealed(metadata);
        byte[] outside = valid.clone();
        outside[userKey] = (byte) 0xff;
        rejectResealed(outside);
        byte[] section = valid.clone();
        section[firstRow] = 9;
        rejectResealed(section);
        byte[] marker = valid.clone();
        ByteBuffer.wrap(marker).putLong(stateValue + 8, 9);
        rejectResealed(marker);
        int receiptRow = stateValue + 16;
        byte[] receiptPrefix = valid.clone();
        receiptPrefix[receiptRow + 9] ^= 1;
        rejectResealed(receiptPrefix);
        byte[] count = valid.clone();
        ByteBuffer.wrap(count).putLong(count.length - 40, 99);
        rejectResealed(count);
        // Remove the otherwise valid marker and repair both terminal count and checksum.
        byte[] missingMarker = new byte[valid.length - (receiptRow - stateRow)];
        System.arraycopy(valid, 0, missingMarker, 0, stateRow);
        System.arraycopy(valid, receiptRow, missingMarker, stateRow, valid.length - receiptRow);
        ByteBuffer.wrap(missingMarker).putLong(missingMarker.length - 40, 2);
        rejectResealed(missingMarker);
        // A duplicate user key is noncanonical even with a correct full body checksum.
        int rowLength = stateRow - firstRow;
        byte[] duplicate = new byte[valid.length + rowLength];
        System.arraycopy(valid, 0, duplicate, 0, stateRow);
        System.arraycopy(valid, firstRow, duplicate, stateRow, rowLength);
        System.arraycopy(valid, stateRow, duplicate, stateRow + rowLength, valid.length - stateRow);
        ByteBuffer.wrap(duplicate).putLong(duplicate.length - 40, 4);
        rejectResealed(duplicate);
    }

    @Test
    void visitorRowsAreProvisionalUntilFinalChecksumAndCannotAlterOrderingState() throws Exception {
        byte[] valid = imageWithOneUserRow();
        Path image = directory.resolve("visit.image");
        Files.write(image, valid);
        assertDoesNotThrow(() -> NativeRangeImage.visit(image, LIMITS, (section, column, key, value) -> Arrays.fill(key, (byte) 0xff)));
        valid[valid.length - 1] ^= 1;
        Files.write(image, valid);
        var delivered = new ArrayList<Section>();
        assertThrows(IOException.class, () -> NativeRangeImage.visit(image, LIMITS,
                (section, column, key, value) -> delivered.add(section)));
        assertEquals(List.of(Section.USER, Section.STATE, Section.STATE, Section.STATE, Section.STATE, Section.RECEIPTS), delivered);
    }

    @Test
    void concurrentPublishersCannotOverwriteOneAnother() throws Exception {
        try (Pool pool = new Pool(directory.resolve("pool"))) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            try (var pin = pool.store.capture(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                Path target = directory.resolve("race.image");
                var start = new CountDownLatch(1);
                var task = (java.util.concurrent.Callable<Manifest>) () -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    try {
                        return NativeRangeImage.write(target, pin, interval(), LIMITS);
                    } catch (FileAlreadyExistsException refused) {
                        return null;
                    }
                };
                var first = executor.submit(task);
                var second = executor.submit(task);
                start.countDown();
                Manifest a = first.get(5, TimeUnit.SECONDS);
                Manifest b = second.get(5, TimeUnit.SECONDS);
                assertTrue((a == null) != (b == null));
                assertArrayEquals((a == null ? b : a).sha256(), NativeRangeImage.inspect(target, LIMITS).sha256());
                try (var files = Files.list(directory)) {
                    assertFalse(files.anyMatch(file -> file.getFileName().toString().startsWith(".native-range-building-")));
                }
            }
        }
    }

    private byte[] imageWithOneUserRow() throws Exception {
        try (Pool pool = new Pool(directory.resolve("source"))) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(GROUP, 0, put(A, 0x80, "value")), bytes("result"));
            try (var pin = pool.store.capture()) {
                Path image = directory.resolve("source.image");
                NativeRangeImage.write(image, pin, interval(), LIMITS);
                return Files.readAllBytes(image);
            }
        }
    }

    @Test void malformedSessionFloorMetadataCannotBePublishedAsAnInstallableImage() throws Exception {
        try (Pool pool = new Pool(directory.resolve("floors"))) {
            pool.store.applyNoop(new ApplyPosition(1,0));
            byte[] key = ByteBuffer.allocate(41).putLong(GROUP.getMostSignificantBits()).putLong(GROUP.getLeastSignificantBits())
                    .put((byte)2).putLong(1).putLong(SESSION.getMostSignificantBits()).putLong(SESSION.getLeastSignificantBits()).array();
            var values = List.of(new byte[7], new byte[9],
                    ByteBuffer.allocate(8).putLong(-1).array(),ByteBuffer.allocate(8).putLong(Long.MAX_VALUE).array());
            for (int i=0;i<values.size();i++) {
                pool.db.put(pool.handles.get(3),key,values.get(i));
                Path output = directory.resolve("bad-floor-"+i+".image");
                try (var pin = pool.store.capture()) {
                    assertThrows(IOException.class,() -> NativeRangeImage.write(output,pin,pin.interval(),LIMITS));
                }
                assertFalse(Files.exists(output));
            }
        }
    }

    @Test void importBirthAndIntentTypedBoundsRejectBeforeAllocatingOrDeliveringOversizedValues()throws Exception {
        byte[] header;
        try(var pool=new Pool(directory.resolve("import-bounds"))){pool.store.applyNoop(new ApplyPosition(1,0));Path valid=directory.resolve("import-header.image");try(var pin=pool.store.capture()){NativeRangeImage.write(valid,pin,interval(),LIMITS);}byte[] bytes=Files.readAllBytes(valid);header=Arrays.copyOf(bytes,headerEnd(bytes));}
        for(int tag:new int[]{0x40,0x41}){
            int length=NativeSstImportManifest.MAX_BYTES+(tag==0x41 ? 24 : 0)+1;Path invalid=directory.resolve("oversized-import-"+tag+".image");
            try(var channel=java.nio.channels.FileChannel.open(invalid,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){channel.write(ByteBuffer.wrap(header));var row=ByteBuffer.allocate(26).put((byte)(Section.STATE.ordinal()+1)).putInt(17).putInt(length).putLong(GROUP.getMostSignificantBits()).putLong(GROUP.getLeastSignificantBits()).put((byte)tag);row.flip();channel.write(row);channel.position(channel.position()+length);channel.write(ByteBuffer.allocate(41));}
            var calls=new java.util.concurrent.atomic.AtomicInteger();var failure=assertThrows(IOException.class,()->NativeRangeImage.visit(invalid,new Limits(32<<20,64<<20,4),(section,column,key,value)->calls.incrementAndGet()));assertEquals("Import birth/intent exceeds native metadata bound",failure.getMessage());assertEquals(0,calls.get());
        }
    }

    @Test void recoveryMetadataDoesNotRelaxOrdinaryOrForeignRows()throws Exception {
        byte[] header;
        try(var pool=new Pool(directory.resolve("recovery-row-bounds"))){pool.store.applyNoop(new ApplyPosition(1,0));Path valid=directory.resolve("recovery-header.image");try(var pin=pool.store.capture()){NativeRangeImage.write(valid,pin,interval(),LIMITS);}byte[] bytes=Files.readAllBytes(valid);header=Arrays.copyOf(bytes,headerEnd(bytes));}
        for(int variant=0;variant<5;variant++) {
            boolean user=variant==0;int keyLength=variant==2?18:17;UUID group=variant==1?OTHER:GROUP;
            int length=variant==4?NativeCurrentRangeRecovery.MAX_BYTES+1:4096;
            byte tag=variant==3?(byte)0x66:(byte)0x72;
            Path invalid=directory.resolve("invalid-recovery-row-"+variant+".image");
            try(var channel=java.nio.channels.FileChannel.open(invalid,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(header));var row=ByteBuffer.allocate(9+(user?16:0)+keyLength).put((byte)((user?Section.USER:Section.STATE).ordinal()+1));
                if(user)row.putLong(A.getMostSignificantBits()).putLong(A.getLeastSignificantBits());
                row.putInt(keyLength).putInt(length).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put(tag);if(keyLength==18)row.put((byte)0);row.flip();channel.write(row);channel.position(channel.position()+length);channel.write(ByteBuffer.allocate(41));
            }
            var delivered=new java.util.concurrent.atomic.AtomicInteger();var read=new java.util.concurrent.atomic.AtomicLong();
            assertThrows(IOException.class,()->NativeRangeImage.visitMetadataAdmitted(invalid,new Limits(1024,128L<<20,4),(section,column,key,value)->delivered.incrementAndGet(),read::addAndGet));
            assertEquals(0,delivered.get());assertTrue(read.get()<=8192,"Rejected before reading oversized value");
            read.set(0);
            assertThrows(IOException.class,()->NativeRangeImage.inspectAdmitted(invalid,new Limits(1024,128L<<20,4),read::addAndGet));assertTrue(read.get()<=8192,"Rejected before reading oversized value");
        }
    }

    private int headerEnd(byte[] image) {
        var input = ByteBuffer.wrap(image);
        input.position(92);
        int columns = input.getInt();
        input.position(input.position() + columns * 16);
        for (int i = 0; i < 2; i++) {
            int size = input.getInt();
            if (size >= 0) input.position(input.position() + size);
        }
        int merges = input.getInt();
        input.position(input.position() + merges * 16);
        return input.position();
    }

    private void rejectResealed(byte[] image) throws Exception {
        byte[] checksum = MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(image, image.length - 32));
        System.arraycopy(checksum, 0, image, image.length - 32, 32);
        Path damaged = directory.resolve("forged.image");
        Files.write(damaged, image);
        assertThrows(IOException.class, () -> NativeRangeImage.inspect(damaged, LIMITS));
    }

    private static final class Pool implements AutoCloseable {
        final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions plain = new ColumnFamilyOptions();
        final ColumnFamilyOptions merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","));
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        final RocksDB db;
        final RangeOwnership ownership = new RangeOwnership();
        final ReplicatedRangeStore store;
        Pool(Path path) throws Exception {
            var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                    new ColumnFamilyDescriptor(new byte[]{1}, merged), new ColumnFamilyDescriptor(new byte[]{2}, plain),
                    new ColumnFamilyDescriptor(new byte[]{3}, plain), new ColumnFamilyDescriptor(new byte[]{4}, plain));
            db = RocksDB.open(options, path.toString(), descriptors, handles);
            store = store(GROUP);
        }
        ReplicatedRangeStore store(UUID group) throws Exception {
            var owner = new RangeOwnership.Owner(group, 7);
            var interval = new RangeOwnership.Interval(DOMAIN, Set.of(A, B),
                    group.equals(GROUP) ? new byte[]{0x7f} : null,
                    new byte[]{group.equals(GROUP) ? (byte) 0xff : 0x7f});
            ownership.register(owner, interval, RangeOwnership.State.ACTIVE);
            return new ReplicatedRangeStore(db, ownership, owner, new byte[32], Map.of(A, handles.get(1), B, handles.get(2)),
                    handles.get(3), handles.get(4), Set.of(A));
        }
        @Override public void close() {
            for (var handle : handles) handle.close();
            db.close();
            merged.close();
            plain.close();
            options.close();
        }
    }
}
