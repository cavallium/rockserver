package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Explicit initial-domain intent: create the database ledger once or guard its exact existing state.
 * Schema-bearing bootstrap publishes schema content and column issuance before this intent.
 * All replicas require the combined validator. */
public final class RatisCatalogueGenesis implements AutoCloseable {
    private static final int MAGIC=0x5243474a, MAX_BYTES=RatisCatalogue.MAX_DIRECTORY_BYTES+SessionEpochLedger.MAX_BYTES+2*DatabaseDomainRoster.MAX_BYTES+4096;
    private final Path journal; private final RaftClient client; private final RatisNodePlan plan; private final RangeDirectory directory; private final long initialTime;
    private final SessionEpochLedger existingLedger;
    private final DatabaseDomainRoster beforeRoster,afterRoster;
    private final byte[] intent; private final Consumer<DistributedCommand> validator;
    private final FileChannel lockChannel; private final FileLock lock;
    private DistributedCommand command; private CommandResult result;
    private boolean busy,closed,fenced;
    /** Authorized initial placement publication. Null ledger creates it at initialTime; otherwise its exact state is guarded unchanged. */
    public RatisCatalogueGenesis(Path journal,RaftClient client,RatisNodePlan plan,UUID domainId,long initialTime,SessionEpochLedger existingLedger,DatabaseDomainRoster beforeRoster,DatabaseDomainRoster afterRoster) throws IOException {
        this.journal=journal.toAbsolutePath(); this.client=Objects.requireNonNull(client); this.plan=Objects.requireNonNull(plan); this.directory=plan.directory(domainId); this.initialTime=initialTime; this.existingLedger=existingLedger; this.beforeRoster=beforeRoster; this.afterRoster=Objects.requireNonNull(afterRoster);
        var c=plan.catalogue();
        if (initialTime<0 || directory.generation()!=0 || !client.getGroupId().getUuid().equals(c.groupId())) throw new IllegalArgumentException("Invalid catalogue genesis binding");
        if(existingLedger!=null && (!existingLedger.clusterId().equals(directory.clusterId()) || !existingLedger.databaseId().equals(directory.databaseId())))throw new IllegalArgumentException("Domain publication changed database ledger identity");
        RatisDatabaseDomainRoster.requireNamespace(afterRoster,directory.clusterId(),directory.databaseId());
        if(beforeRoster==null) { if(afterRoster.generation()!=0) throw new IllegalArgumentException("Initial roster generation differs"); } else { RatisDatabaseDomainRoster.requireNamespace(beforeRoster,directory.clusterId(),directory.databaseId()); if(!beforeRoster.equals(afterRoster)) afterRoster.requireSuccessorOf(beforeRoster); }
        if(!afterRoster.domainIds().contains(directory.domainId())) throw new IllegalArgumentException("Genesis domain is not advertised");
        validator=RatisCatalogue.validator(directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),c.sessionsColumn());
        intent=prepare(new UUID(0,0)).payloadDigest();
        if (!Files.isDirectory(this.journal.getParent())) throw new IOException("Genesis journal parent is absent");
        lockChannel=FileChannel.open(this.journal.resolveSibling(this.journal.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Genesis intent already owned",owned); }
            if (acquired==null) throw new IOException("Genesis intent already owned"); lock=acquired;
            if (Files.exists(this.journal)) {
                var input=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(this.journal,MAX_BYTES)));
                if (input.readInt()!=MAGIC || !Arrays.equals(input.readNBytes(32),intent)) throw new IOException("Genesis intent changed");
                int size=input.readInt(); if (size<0 || size>MAX_BYTES || size>input.available()-1) throw new IOException("Invalid genesis command size");
                byte[] bytes=input.readNBytes(size); command=DistributedCommandCodec.decode(bytes); var id=command.operationId();
                if (id.epoch()!=0 || id.sequence()!=0 || !Arrays.equals(bytes,DistributedCommandCodec.encode(prepare(id.sessionId())))) throw new IOException("Noncanonical genesis command");
                int status=input.readUnsignedByte(); if (status>3 || input.available()!=0) throw new IOException("Invalid genesis completion");
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
            if (closed || fenced || !lock.isValid()) throw new IOException("Genesis owner is closed/fenced; reopen its intent");
            if (busy) throw new IllegalStateException("Genesis execution is active"); if (result!=null) return result; busy=true;
        }
        try {
            var reply=RatisRoutedClient.rpc(() -> client.io().send(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))));
            if (reply==null || !reply.isSuccess() || reply.getMessage()==null) throw new IOException("Genesis request failed",reply==null ? null : reply.getException());
            var bytes=reply.getMessage().getContent(); if (bytes==null || bytes.size()>CommandResult.MAX_BYTES) throw new IOException("Invalid genesis reply size");
            final CommandResult outcome;
            try { outcome=CommandResult.decode(bytes.toByteArray()); } catch (IllegalArgumentException malformed) { throw new IOException("Invalid genesis result",malformed); }
            if (outcome.status()==CommandResult.Status.RESULT_TOO_LARGE || outcome.valueCount()!=0) throw new IOException("Unexpected genesis outcome");
            synchronized (this) { persist(outcome); result=outcome; }
            return outcome;
        } finally { synchronized (this) { busy=false; } }
    }
    private DistributedCommand prepare(UUID session) {
        var d=directory; var c=plan.catalogue(); byte[] directory=RangeDirectoryCodec.encode(d);
        var mutations=new ArrayList<Mutation>();mutations.add(new Mutation(Mutation.Kind.PUT,c.directoryColumn(),uuid(d.domainId()),directory));
        if(existingLedger==null)mutations.add(new Mutation(Mutation.Kind.PUT,c.sessionsColumn(),uuid(d.databaseId()),SessionEpochLedger.initial(d.clusterId(),d.databaseId(),initialTime).encode()));
        if(!Objects.equals(beforeRoster,afterRoster)) mutations.add(new Mutation(Mutation.Kind.PUT,c.directoryColumn(),RatisDatabaseDomainRoster.key(d.databaseId()),afterRoster.encode()));
        var prepared=new DistributedCommand(c.groupId(),c.epoch(),c.schemaDigest(),new OperationId(session,0,0),mutations,
                List.of(new KeyPredicate(c.directoryColumn(),uuid(d.domainId()),null),new KeyPredicate(c.sessionsColumn(),uuid(d.databaseId()),existingLedger==null ? null : existingLedger.encode()),new KeyPredicate(c.directoryColumn(),RatisSchemaAdministration.headKey(d.domainId()),null),new KeyPredicate(c.directoryColumn(),RatisRangeImportCatalogue.headKey(d.domainId()),null),new KeyPredicate(c.directoryColumn(),RatisTopologyCatalogue.headKey(d.domainId()),null),new KeyPredicate(c.directoryColumn(),RatisDatabaseDomainRoster.key(d.databaseId()),beforeRoster==null ? null : beforeRoster.encode())));
        validator.accept(prepared); return prepared;
    }
    private void persist(CommandResult outcome) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(MAGIC); output.write(intent); byte[] encoded=DistributedCommandCodec.encode(command);
            output.writeInt(encoded.length); output.write(encoded); output.writeByte(outcome==null ? 0 : outcome.status()==CommandResult.Status.SUCCESS ? 1 : outcome.status()==CommandResult.Status.CONFLICT ? 2 : 3);
            output.flush(); SessionJournalFiles.write(journal,bytes.toByteArray(),MAX_BYTES);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    @Override public synchronized void close() throws IOException {
        if (closed && !lockChannel.isOpen()) return; if (busy) throw new IOException("Genesis execution is active"); closed=true;
        try { if (lock.isValid()) lock.release(); } finally { lockChannel.close(); }
    }
    private static byte[] uuid(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
}
