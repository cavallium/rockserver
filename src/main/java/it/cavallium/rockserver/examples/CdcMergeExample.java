package it.cavallium.rockserver.examples;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.CdcCommitMode;
import it.cavallium.rockserver.core.common.cdc.CdcStreamOptions;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone example showing how to:
 *  - Ingest an infinite stream of remote inputs using Merge with a custom merge operator
 *  - Demonstrate real serialization/deserialization of values and patches
 *  - Continuously listen to CDC changes, apply patches to a local projection, and acknowledge them reactively
 *
 * The example uses the built-in MyStringAppendOperator as merge operator via a small
 * configuration snippet (applied as fallback column option).
 */
public class CdcMergeExample {

    private static final Logger LOG = LoggerFactory.getLogger(CdcMergeExample.class);

    // --- Example domain model and patch ---
    record Message(int id, String text, boolean pinned, long updatedAt) {}
    record MessagePatch(Integer id, String appendText, Boolean pinnedSet, Long updatedAt) {}

    // Remote input coming from the outside world, here simulated
    record RemoteInput(int id, String textDelta, boolean togglePin) {}

    public static void main(String[] args) throws Exception {
        System.setProperty("rockserver.core.print-config", "false");

        // 1) Prepare a minimal config enabling the sample merge operator
        Path tempConfig = Files.createTempFile("rockserver-demo-config", ".conf");
        Files.writeString(tempConfig, """
            database: {
              global: {
                ingest-behind: true
                // Explicit per-column configuration so it also applies when running in-memory
                column-options: [
                  {
                    name: "messages",
                    // Use the domain-aware merge operator that applies MessagePatch to Message values
                    merge-operator-class: "it.cavallium.rockserver.examples.MessagePatchMergeOperator"
                  }
                ]
                // Fallback remains for any additional columns created later
                fallback-column-options: {
                  merge-operator-class: "it.cavallium.rockserver.examples.MessagePatchMergeOperator"
                }
              }
            }
            """
        );

        // 2) Open an embedded DB (in-memory for the demo).
        // The column is declared via config (column-options) so we only need to resolve its id.
        try (var db = new EmbeddedConnection(null, "cdc-merge-example", tempConfig)) {
            var api = db.getAsyncApi();
            long columnId = db.getSyncApi().getColumnId("messages");
            LOG.info("Using column 'messages' with id={} (from config)", columnId);

            // 3) Create or resume a CDC subscription BEFORE seeding data so we also see initial PUTs
            String subId = "messages-sub";
            long startSeq = db.getSyncApi().cdcCreate(subId, null, List.of(columnId), true); // start from last committed + 1, resolved values
            LOG.info("CDC subscription '{}' created. startSeq={}", subId, startSeq);

            // 4) Seed some initial records (real value serialization)
            var now = System.currentTimeMillis();
            putMessage(db, columnId, new Message(1, "Hello 1", false, now));
            putMessage(db, columnId, new Message(2, "Hello 2", false, now));
            putMessage(db, columnId, new Message(3, "Hello 3", true, now));

            // 5) Simulate an infinite remote input stream (replace with your real source)
            Flux<RemoteInput> remoteInputTypesFlux = Flux
                    .interval(Duration.ofMillis(200))
                    .onBackpressureBuffer(10_000)
                    .map(i -> new RemoteInput(1 + (int)(i % 3), // ids 1..3
                            " +delta-" + i,
                            (i % 5) == 0 // toggle pin sometimes
                    ));

            // 6) Map remote inputs to domain patches
            Flux<MessagePatch> patchers = remoteInputTypesFlux.map(in ->
                    new MessagePatch(in.id(), in.textDelta(), in.togglePin ? null /* handled below */ : null, null)
            ).map(p -> {
                // Occasionally toggle pin and update timestamp
                boolean doToggle = Math.random() < 0.2;
                Boolean newPinned = doToggle ? Boolean.TRUE : null; // example: set pinned to true sometimes
                return new MessagePatch(p.id, p.appendText, newPinned, System.currentTimeMillis());
            });

            // 7) Encode patchers into Merge operands and keys, publish as KV batches (MERGE operands are patches)
            // Stop signal to gracefully end the demo (will complete the merge publisher and let mergeBatchAsync finish)
            Sinks.Empty<Void> stopSignal = Sinks.empty();

            // Capture column id for lambdas
            final long messagesColumnId = columnId;

            Flux<KVBatch> kvBatches = patchers
                    .map(p -> new KV(
                            new Keys(new Buf[]{Buf.wrap(intToBytes(p.id()))}),
                            Buf.wrap(encodePatch(p))
                    ))
                    .bufferTimeout(128, Duration.ofMillis(150)) // small, frequent batches for smoother CDC
                    .filter(batch -> !batch.isEmpty())
                    .map(batch -> {
                        List<Keys> keys = batch.stream().map(KV::keys).collect(Collectors.toList());
                        List<Buf> values = batch.stream().map(KV::value).collect(Collectors.toList());
                        return new KVBatch.KVBatchRef(keys, values);
                    })
                    .map(b -> (KVBatch) b)
                    .doOnNext(b -> {
                        try {
                            int size = ((KVBatch.KVBatchRef) b).keys().size();
                            LOG.debug("MERGE batch emitted size={}", size);
                        } catch (Throwable ignore) { }
                    })
                    .takeUntilOther(stopSignal.asMono());

            // 8) Start merging incoming patches continuously (backpressure-aware)
            CompletableFuture<Void> mergeLoop = api.mergeBatchAsync(
                    columnId,
                    kvBatches.publishOn(Schedulers.boundedElastic()),
                    MergeBatchMode.MERGE_WRITE_BATCH
            ).whenComplete((v, err) -> {
                if (err != null) {
                    LOG.error("mergeBatchAsync terminated with error", err);
                } else {
                    LOG.info("mergeBatchAsync completed (publisher finished)");
                }
            });

            // 9) Local projection for external systems (e.g., to feed Solr)
            Map<Integer, Message> projection = new ConcurrentHashMap<>();

            // 10) Consume CDC events and acknowledge after processing
            var cdcOptions = new CdcStreamOptions(null, 1_000, Duration.ofMillis(100), CdcCommitMode.NONE);

            var doneLatch = new CountDownLatch(1);

            // Simple metrics: track last received and last acked sequences and events/sec
            AtomicLong lastReceivedSeq = new AtomicLong(startSeq - 1);
            AtomicLong lastAckedSeq = new AtomicLong(startSeq - 1);
            AtomicLong receivedCounter = new AtomicLong();
            AtomicLong ackedCounter = new AtomicLong();

            Disposable metricsSampler = Flux.interval(Duration.ofSeconds(2))
                    .doOnNext(tick -> LOG.info("CDC stats: received/sec≈{}, acked/sec≈{}, lastReceivedSeq={}, lastAckedSeq={}",
                            receivedCounter.getAndSet(0) / 2.0,
                            ackedCounter.getAndSet(0) / 2.0,
                            lastReceivedSeq.get(),
                            lastAckedSeq.get()))
                    .subscribe();

            var cdcDisposable = api.cdcStreamAck(subId, cdcOptions)
                    .doOnNext(ack -> {
                        lastReceivedSeq.set(ack.event().seq());
                        receivedCounter.incrementAndGet();
                        try {
                            // With emitLatestValues=true, both PUT and MERGE carry the full resolved value
                            var m = decodeMessage(ack.event().value().toByteArray());
                            LOG.debug("CDC {} id={} pinned={} len(text)={}", ack.event().op(), m.id(), m.pinned(), m.text() != null ? m.text().length() : 0);
                        } catch (Throwable ignore) {}
                    })
                    .concatMap(ack ->
                            // Decode and apply to projection, then index, then ACK
                            processEventAndAck(db, messagesColumnId, projection, ack.event())
                                    .then(ack.ack())
                                    .doOnSuccess(__ -> { lastAckedSeq.set(ack.event().seq()); ackedCounter.incrementAndGet(); })
                                    .thenReturn(ack.event())
                    )
                    .doOnNext(ev -> LOG.trace("Indexed + acked CDC event seq={}", ev.seq()))
                    .doOnError(err -> {
                        LOG.error("CDC stream error", err);
                        doneLatch.countDown();
                    })
                    .doOnComplete(() -> LOG.info("CDC stream completed"))
                    .doOnCancel(() -> doneLatch.countDown())
                    .subscribeOn(Schedulers.parallel())
                    .subscribe();

            // 10) Let the demo run for a while
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutting down CDC/merge example...");
                // Stop the remote input stream and let mergeBatch complete
                stopSignal.tryEmitEmpty();
                try { mergeLoop.get(5, TimeUnit.SECONDS); } catch (Exception e) { LOG.warn("Error waiting for merge loop to stop", e); }
                cdcDisposable.dispose();
                metricsSampler.dispose();
            }));

            LOG.info("CDC Merge example running. Press Ctrl+C to stop.");
            // Run for ~20 seconds then exit (demo)
            doneLatch.await(20, TimeUnit.SECONDS);
            // Stop sources and cleanup (if not stopped by hook)
            stopSignal.tryEmitEmpty();
            cdcDisposable.dispose();
            metricsSampler.dispose();
            try { mergeLoop.get(5, TimeUnit.SECONDS); } catch (Exception e) { LOG.warn("Error waiting for merge loop to stop", e); }
        } finally {
            try { Files.deleteIfExists(tempConfig); } catch (IOException e) { LOG.warn("Failed to delete temp config", e); }
        }
    }

    // === CDC processing: decode, apply patch or value, and simulate indexing ===
    private static Mono<Void> processEventAndAck(EmbeddedConnection db,
                                                 long columnId,
                                                 Map<Integer, Message> projection,
                                                 CDCEvent ev) {
        int id = bytesToInt(ev.key().toByteArray());
        try {
            switch (ev.op()) {
                case PUT -> {
                    Message msg = decodeMessage(ev.value().toByteArray());
                    projection.put(id, msg);
                    LOG.debug("Projection PUT id={} pinned={} text='{}'", msg.id(), msg.pinned(), ellipsize(msg.text(), 64));
                    return indexExternally(msg);
                }
                case MERGE -> {
                    // In resolved-values mode, MERGE carries the full resolved value, not the operand
                    Message msg = decodeMessage(ev.value().toByteArray());
                    projection.put(id, msg);
                    LOG.debug("Projection MERGED(RESOLVED) id={} pinned={} text='{}'", msg.id(), msg.pinned(), ellipsize(msg.text(), 64));
                    return indexExternally(msg);
                }
                case DELETE -> {
                    projection.remove(id);
                    LOG.debug("Projection DELETE id={}", id);
                    return indexExternally(null);
                }
            }
            return Mono.empty();
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    private static Mono<Void> indexExternally(Message message) {
        // Replace with your async indexing call. Here we just simulate a non-blocking op.
        return Mono.delay(Duration.ofMillis(5)).then();
    }

    private static byte[] intToBytes(int x) {
        return ByteBuffer.allocate(4).putInt(x).array();
    }

    private static int bytesToInt(byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }

    // === Serialization for Message ===
    private static byte[] encodeMessage(Message m) {
        byte[] textBytes = m.text() != null ? m.text().getBytes(StandardCharsets.UTF_8) : new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + 8 + 4 + textBytes.length);
        buf.put((byte) 1); // ver
        buf.putInt(m.id());
        buf.put((byte) (m.pinned() ? 1 : 0));
        buf.putLong(m.updatedAt());
        buf.putInt(textBytes.length);
        buf.put(textBytes);
        return buf.array();
    }

    private static Message decodeMessage(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int ver = Byte.toUnsignedInt(buf.get());
        if (ver != 1) throw new IllegalStateException("Unknown Message ver " + ver);
        int id = buf.getInt();
        boolean pinned = buf.get() != 0;
        long updatedAt = buf.getLong();
        int len = buf.getInt();
        byte[] t = new byte[len];
        buf.get(t);
        String text = new String(t, StandardCharsets.UTF_8);
        return new Message(id, text, pinned, updatedAt);
    }

    // === Serialization for MessagePatch (delta) ===
    // flags: bit0 appendText, bit1 pinnedSet, bit2 updatedAt
    private static byte[] encodePatch(MessagePatch p) {
        byte[] append = p.appendText() != null ? p.appendText().getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte flags = 0;
        if (p.appendText() != null) flags |= 0x1;
        if (p.pinnedSet() != null) flags |= 0x2;
        if (p.updatedAt() != null) flags |= 0x4;
        int size = 1 + 4 + 1 // ver, id, flags
                + ((flags & 0x1) != 0 ? 4 + append.length : 0)
                + ((flags & 0x2) != 0 ? 1 : 0)
                + ((flags & 0x4) != 0 ? 8 : 0);
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1); // ver
        buf.putInt(p.id() != null ? p.id() : 0);
        buf.put(flags);
        if ((flags & 0x1) != 0) { buf.putInt(append.length); buf.put(append); }
        if ((flags & 0x2) != 0) { buf.put((byte) (p.pinnedSet() ? 1 : 0)); }
        if ((flags & 0x4) != 0) { buf.putLong(p.updatedAt()); }
        return buf.array();
    }

    private static MessagePatch decodePatch(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int ver = Byte.toUnsignedInt(buf.get());
        if (ver != 1) throw new IllegalStateException("Unknown Patch ver " + ver);
        int id = buf.getInt();
        byte flags = buf.get();
        String append = null;
        Boolean pinned = null;
        Long ts = null;
        if ((flags & 0x1) != 0) { int len = buf.getInt(); byte[] b = new byte[len]; buf.get(b); append = new String(b, StandardCharsets.UTF_8); }
        if ((flags & 0x2) != 0) { pinned = buf.get() != 0; }
        if ((flags & 0x4) != 0) { ts = buf.getLong(); }
        return new MessagePatch(id, append, pinned, ts);
    }

    private static Message applyPatch(Message base, MessagePatch p) {
        String text = base.text();
        if (p.appendText() != null) text = (text != null ? text : "") + p.appendText();
        boolean pinned = p.pinnedSet() != null ? p.pinnedSet() : base.pinned();
        long updatedAt = p.updatedAt() != null ? p.updatedAt() : System.currentTimeMillis();
        return new Message(base.id(), text, pinned, updatedAt);
    }

    private static void putMessage(EmbeddedConnection db, long columnId, Message m) {
        db.getSyncApi().put(0, columnId,
                new Keys(new Buf[]{Buf.wrap(intToBytes(m.id()))}),
                Buf.wrap(encodeMessage(m)),
                RequestType.none());
    }

    private static String ellipsize(String s, int max) {
        if (s == null) return null;
        if (s.length() <= max) return s;
        return s.substring(0, Math.max(0, max - 3)) + "...";
    }

    // End of example
}
