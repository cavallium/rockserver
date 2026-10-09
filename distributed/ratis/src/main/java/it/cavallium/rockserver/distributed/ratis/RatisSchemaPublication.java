package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;

/** Durable one-shot schema publication intent; the caller owns its publication authority and borrowed client. */
public final class RatisSchemaPublication implements AutoCloseable {
    private static final int MAGIC=0x5253504a, MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES+64;
    private final Path journal;
    private final RatisSchemaCatalogue catalogue;
    private final DistributedSchema schema;
    private final byte[] intent;
    private final byte[] allocationWitness;
    private final Map<UUID,RatisColumnAllocation.Reservation> reservations;
    private final Map<UUID,byte[]> columnWitnesses;
    private final Map<Long,byte[]> apiWitnesses;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private DistributedCommand command;
    private CommandResult result;
    private boolean busy,closed,fenced;

    public RatisSchemaPublication(Path journal,RaftClient client,RatisNodePlan plan,DistributedSchema schema) throws IOException {
        this(journal,client,plan,schema,null,Map.of(),Map.of(),Map.of());
    }
    public RatisSchemaPublication(Path journal,RaftClient client,RatisNodePlan plan,DistributedSchema schema,
                                  byte[] allocationWitness,Map<UUID,byte[]> columnWitnesses,Map<Long,byte[]> apiWitnesses,Map<UUID,RatisColumnAllocation.Reservation> reservations) throws IOException {
        this.allocationWitness=allocationWitness==null ? null : allocationWitness.clone(); this.reservations=Map.copyOf(reservations);
        this.columnWitnesses=freeze(columnWitnesses); this.apiWitnesses=freeze(apiWitnesses);
        this.journal=journal.toAbsolutePath(); this.schema=Objects.requireNonNull(schema); Objects.requireNonNull(plan);
        schema.requireMatches(plan.directory()); var c=plan.catalogue();
        catalogue=new RatisSchemaCatalogue(client,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
        byte[] normalized=catalogue.prepare(new OperationId(new UUID(0,0),0,0),schema,this.allocationWitness,this.columnWitnesses,this.apiWitnesses,this.reservations).payloadDigest();
        try {
            intent=MessageDigest.getInstance("SHA-256").digest(ByteBuffer.allocate(48).put(normalized)
                    .putLong(c.sessionsColumn().getMostSignificantBits()).putLong(c.sessionsColumn().getLeastSignificantBits()).array());
        } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        if (!Files.isDirectory(this.journal.getParent())) throw new IOException("Schema publication journal parent is absent");
        lockChannel=FileChannel.open(this.journal.resolveSibling(this.journal.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Schema publication intent already owned",owned); }
            if (acquired==null) throw new IOException("Schema publication intent already owned"); lock=acquired;
            if (Files.exists(this.journal)) {
                var input=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(this.journal,MAX_BYTES)));
                if (input.readInt()!=MAGIC || !Arrays.equals(input.readNBytes(32),intent)) throw new IOException("Schema publication intent changed");
                int size=input.readInt();
                if (size<0 || size>DistributedCommandCodec.MAX_ENCODED_BYTES || size>input.available()-1) throw new IOException("Invalid schema publication command size");
                byte[] bytes=input.readNBytes(size);
                try { command=DistributedCommandCodec.decode(bytes); }
                catch (IllegalArgumentException malformed) { throw new IOException("Invalid journaled schema publication command",malformed); }
                var id=command.operationId();
                if (id.epoch()!=0 || id.sequence()!=0 || !Arrays.equals(bytes,DistributedCommandCodec.encode(prepare(id.sessionId())))) {
                    throw new IOException("Noncanonical schema publication command");
                }
                int status=input.readUnsignedByte();
                if (status>3 || input.available()!=0) throw new IOException("Invalid schema publication completion");
                if (status>0) result=new CommandResult(status==1 ? CommandResult.Status.SUCCESS : status==2 ? CommandResult.Status.CONFLICT : CommandResult.Status.SESSION_EXPIRED,List.of());
                SessionJournalFiles.force(this.journal); SessionJournalFiles.force(this.journal.getParent());
            } else { command=prepare(UUID.randomUUID()); persist(null); }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    public DistributedCommand preparedCommand() { return command; }
    public synchronized CommandResult completedResult() { return result; }
    public CommandResult execute() throws IOException {
        synchronized (this) {
            if (closed || fenced || !lock.isValid()) throw new IOException("Schema publication owner is closed/fenced; reopen its intent");
            if (busy) throw new IllegalStateException("Schema publication execution is active");
            if (result!=null) return result; busy=true;
        }
        try {
            var outcome=catalogue.execute(command);
            synchronized (this) { persist(outcome); result=outcome; }
            return outcome;
        } finally { synchronized (this) { busy=false; } }
    }
    private DistributedCommand prepare(UUID session) { return catalogue.prepare(new OperationId(session,0,0),schema,allocationWitness,columnWitnesses,apiWitnesses,reservations); }
    private static <K> Map<K,byte[]> freeze(Map<K,byte[]> witnesses) {
        var result=new HashMap<K,byte[]>(); Objects.requireNonNull(witnesses).forEach((key,value) -> {
            Objects.requireNonNull(key); if (value!=null) result.put(key,value.clone());
        }); return Collections.unmodifiableMap(result);
    }
    private void persist(CommandResult outcome) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(MAGIC); output.write(intent); byte[] encoded=DistributedCommandCodec.encode(command);
            output.writeInt(encoded.length); output.write(encoded);
            output.writeByte(outcome==null ? 0 : outcome.status()==CommandResult.Status.SUCCESS ? 1 : outcome.status()==CommandResult.Status.CONFLICT ? 2 : 3);
            output.flush(); SessionJournalFiles.write(journal,bytes.toByteArray(),MAX_BYTES);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    @Override public synchronized void close() throws IOException {
        if (closed && !lockChannel.isOpen()) return;
        if (busy) throw new IOException("Schema publication execution is active"); closed=true;
        try { if (lock.isValid()) lock.release(); } finally { lockChannel.close(); }
    }
}
