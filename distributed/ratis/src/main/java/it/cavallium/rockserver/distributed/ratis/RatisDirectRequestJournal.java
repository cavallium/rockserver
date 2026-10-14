package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** One bounded direct request, including ordered acknowledged results; caller serializes mutations. */
final class RatisDirectRequestJournal implements AutoCloseable {
    static final int MAX_BYTES=40*1024*1024;
    static final long MAX_INPUT=16L*1024*1024,MAX_RESULTS=16L*1024*1024;
    private static final int MAGIC=0x52445251;
    static final class Request {
        final RatisRocksDBDirect.WriteKind kind; final RequestType.RequestTypeId type; final boolean multi; final List<DistributedCommand> items; final List<CommandResult> results=new ArrayList<>();
        RemoteMutationToken token; boolean resultAcknowledged; OperationId pending; int pendingIndex=-1; boolean overflow; CommandResult.Status unsubmittedStatus;
        Request(RatisRocksDBDirect.WriteKind kind,RequestType.RequestTypeId type,boolean multi,List<DistributedCommand> items) { this.kind=Objects.requireNonNull(kind); this.type=type; this.multi=multi; this.items=List.copyOf(items); }
        int cursor() { return results.size(); }
        boolean terminal() { return unsubmittedStatus!=null || overflow || !results.isEmpty() && results.getLast().status()!=CommandResult.Status.SUCCESS || cursor()==items.size(); }
        boolean same(Request other) {
            if (!Objects.equals(token,other.token) || kind!=other.kind || type!=other.type || multi!=other.multi || items.size()!=other.items.size()) return false;
            for (int n=0;n<items.size();n++) if (!Arrays.equals(DistributedCommandCodec.encode(items.get(n)),DistributedCommandCodec.encode(other.items.get(n)))) return false;
            return true;
        }
        DistributedCommand pendingItem() { return items.get(pendingIndex); }
    }
    private final Path path; private final byte[] binding; private final FileChannel channel; private final FileLock lock;
    private boolean closed,fenced; private Request request;
    RatisDirectRequestJournal(Path path,byte[] binding) throws IOException {
        this.path=path.toAbsolutePath(); this.binding=binding.clone(); if (binding.length!=32 || !Files.isDirectory(this.path.getParent())) throw new IOException("Direct journal binding/parent invalid");
        channel=FileChannel.open(this.path.resolveSibling(this.path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Direct journal already owned",owned); }
            if (acquired==null) throw new IOException("Direct journal already owned"); lock=acquired;
            if (Files.exists(this.path)) { byte[] body=SessionJournalFiles.read(this.path,MAX_BYTES); request=decode(body); if (!Arrays.equals(body,encode())) throw new IOException("Noncanonical direct journal"); SessionJournalFiles.force(this.path); SessionJournalFiles.force(this.path.getParent()); }
        } catch (IOException | RuntimeException | Error failure) { try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    void retire() throws IOException {requireOpen();if(request==null)return;try{SessionJournalFiles.remove(path,encode(),MAX_BYTES);request=null;}catch(IOException failure){fenced=true;throw failure;}}
    Request request() throws IOException { requireOpen(); return request; }
    void begin(Request next) throws IOException { requireOpen(); validate(next); request=next; persist(); }
    void bind(OperationId operation) throws IOException { requireOpen(); if (request.pending!=null || request.terminal()) throw new IOException("Direct pending binding changed"); request.pending=operation; request.pendingIndex=request.cursor(); persist(); }
    void result(CommandResult result) throws IOException {
        requireOpen(); if (request.pending==null || request.pendingIndex!=request.cursor() || request.terminal()) throw new IOException("Direct result lacks original pending identity");
        request.results.add(result); long bytes=0; for (var saved:request.results) bytes+=saved.encode().length;
        request.overflow=bytes>MAX_RESULTS; persist();
    }
    void acknowledged() throws IOException { requireOpen(); request.pending=null; request.pendingIndex=-1; persist(); }
    void rejectUnsubmitted(CommandResult.Status status) throws IOException {
        requireOpen(); if (status!=CommandResult.Status.STALE_SCHEMA || request.pending!=null || request.terminal()) throw new IOException("No-effects rejection requires an unsubmitted remaining item");
        request.unsubmittedStatus=status; persist();
    }
    static CommandResult.Status terminalOutcome(Request request) {if(!request.terminal())throw new IllegalArgumentException("Direct outcome is not terminal");if(request.unsubmittedStatus!=null)return request.unsubmittedStatus;if(request.overflow)return CommandResult.Status.RESULT_TOO_LARGE;return request.results.isEmpty()?CommandResult.Status.SUCCESS:request.results.getLast().status();}
    static byte[] resultDigest(Request request) { var out=new java.io.ByteArrayOutputStream(); for(var result:request.results)out.writeBytes(result.encode()); out.write(request.overflow?1:0); out.write(request.unsubmittedStatus==null?0:request.unsubmittedStatus.ordinal()+1); return RemoteMutationToken.digest(out.toByteArray()); }
    private void persist() throws IOException { try { SessionJournalFiles.write(path,encode(),MAX_BYTES); } catch (IOException | RuntimeException failure) { fenced=true; if (failure instanceof IOException io) throw io; throw new IOException("Direct journal failed; reopen it",failure); } }
    private byte[] encode() throws IOException {
        if (request==null) throw new IOException("Direct request absent"); validate(request);
        long size=56L+RemoteMutationToken.BYTES+(request.pending==null ? 0 : 36);
        for (var item:request.items) size+=4L+DistributedCommandCodec.encodedSize(item);
        for (var result:request.results) size+=4L+result.encode().length;
        if (size+32>MAX_BYTES) throw new IOException("Direct request journal exceeds bound");
        var out=ByteBuffer.allocate((int)size).putInt(MAGIC).put(binding).putInt(request.kind.ordinal()).putInt(request.type.ordinal()).put((byte)(request.multi ? 1 : 0)).putInt(request.items.size());
        for (var item:request.items) { byte[] encoded=DistributedCommandCodec.encode(item); out.putInt(encoded.length).put(encoded); }
        out.putInt(request.results.size());
        for (var result:request.results) { byte[] encoded=result.encode(); out.putInt(encoded.length).put(encoded); }
        out.put((byte)(request.overflow ? 1 : 0)).put((byte)(request.pending==null ? 0 : 1));
        if (request.pending!=null) out.putInt(request.pendingIndex).putLong(request.pending.sessionId().getMostSignificantBits()).putLong(request.pending.sessionId().getLeastSignificantBits()).putLong(request.pending.epoch()).putLong(request.pending.sequence());
        out.put((byte)(request.unsubmittedStatus==null ? 0 : 1));
        out.put(request.token.encode());
        if (out.hasRemaining()) throw new IOException("Direct journal size calculation changed"); return out.array();
    }
    private Request decode(byte[] body) throws IOException {
        try {
            var in=ByteBuffer.wrap(body); if (in.getInt()!=MAGIC) throw new IOException("Invalid direct journal header"); byte[] actual=new byte[32]; in.get(actual); if (!Arrays.equals(binding,actual)) throw new IOException("Direct journal client/grant/schema binding changed");
            int effect=in.getInt(); if (effect<0 || effect>=RatisRocksDBDirect.WriteKind.values().length) throw new IOException("Invalid direct write kind");
            int kind=in.getInt(); if (kind<0 || kind>=RequestType.RequestTypeId.values().length) throw new IOException("Invalid direct callback"); boolean multi=flag(in); int count=in.getInt(); if (count<1 || count>256 || count*4L>in.remaining()) throw new IOException("Invalid direct item count");
            var items=new ArrayList<DistributedCommand>(); for (int n=0;n<count;n++) items.add(DistributedCommandCodec.decode(blob(in,DistributedCommandCodec.MAX_ENCODED_BYTES)));
            var result=new Request(RatisRocksDBDirect.WriteKind.values()[effect],RequestType.RequestTypeId.values()[kind],multi,items); count=in.getInt(); if (count<0 || count>items.size()) throw new IOException("Invalid direct progress"); for (int n=0;n<count;n++) result.results.add(CommandResult.decode(blob(in,CommandResult.MAX_BYTES)));
            result.overflow=flag(in); if (flag(in)) { result.pendingIndex=in.getInt(); result.pending=new OperationId(new UUID(in.getLong(),in.getLong()),in.getLong(),in.getLong()); } if (flag(in)) result.unsubmittedStatus=CommandResult.Status.STALE_SCHEMA; byte[] token=new byte[RemoteMutationToken.BYTES]; in.get(token); result.token=RemoteMutationToken.decode(token); if (in.hasRemaining()) throw new IOException("Trailing direct journal bytes"); validate(result); return result;
        } catch (RuntimeException invalid) { throw new IOException("Invalid direct journal",invalid); }
    }
    private static void validate(Request request) throws IOException {
        if (request.token==null || request.token.kind()!=RemoteMutationToken.Kind.POINT)throw new IOException("Direct request lacks caller ownership");
        if (request.items.isEmpty() || request.items.size()>256 || !request.multi && request.items.size()!=1 || request.results.size()>request.items.size()) throw new IOException("Invalid direct request count");
        boolean returns=request.type==RequestType.RequestTypeId.MERGED || request.type==RequestType.RequestTypeId.PREVIOUS || request.type==RequestType.RequestTypeId.PREVIOUS_PRESENCE || request.type==RequestType.RequestTypeId.DELTA || request.type==RequestType.RequestTypeId.CHANGED;
        if (request.kind==RatisRocksDBDirect.WriteKind.MERGE ? request.type!=RequestType.RequestTypeId.MERGED && request.type!=RequestType.RequestTypeId.NOTHING
                : !(returns && request.type!=RequestType.RequestTypeId.MERGED || request.type==RequestType.RequestTypeId.NOTHING || request.kind==RatisRocksDBDirect.WriteKind.PUT && request.type==RequestType.RequestTypeId.ENSURE)) throw new IOException("Invalid retained direct callback");
        long input=0,resultBytes=0;
        for (var item:request.items) { var mutation=item.mutations().isEmpty() ? null : item.mutations().getFirst();
            Mutation.ReturnMode mode=!returns ? Mutation.ReturnMode.NONE : request.kind==RatisRocksDBDirect.WriteKind.MERGE ? Mutation.ReturnMode.CURRENT : Mutation.ReturnMode.PREVIOUS;
            boolean effect=mutation!=null && switch (request.kind) {
                case PUT -> mutation.kind()==Mutation.Kind.PUT || mutation.kind()==Mutation.Kind.BUCKET_PUT;
                case DELETE -> mutation.kind()==Mutation.Kind.DELETE || mutation.kind()==Mutation.Kind.BUCKET_DELETE;
                case MERGE -> mutation.kind()==Mutation.Kind.MERGE || mutation.kind()==Mutation.Kind.BUCKET_MERGE;
            };
            if (!effect || mutation.returnMode()!=mode) throw new IOException("Direct callback/effect shape changed");
            if (request.kind==RatisRocksDBDirect.WriteKind.DELETE && (request.type==RequestType.RequestTypeId.DELTA || request.type==RequestType.RequestTypeId.CHANGED)) throw new IOException("Invalid direct delete callback");
            if (item.kind()!=DistributedCommand.Kind.DATA || item.acknowledgedThrough()!=-1 || item.mutations().size()!=1 || !item.predicates().isEmpty()) throw new IOException("Direct input is not one frozen physical item");
            input+=DistributedCommandCodec.encodedSize(item);
        }
        for (int n=0;n<request.results.size();n++) {
            var result=request.results.get(n); resultBytes+=result.encode().length; if (result.status()!=CommandResult.Status.SUCCESS && n!=request.results.size()-1) throw new IOException("Direct progress follows a terminal no-effects result");
            int expectedReturns=request.items.get(n).mutations().getFirst().returnMode()==Mutation.ReturnMode.NONE ? 0 : 1;
            if (result.status()==CommandResult.Status.SUCCESS && result.valueCount()!=expectedReturns) throw new IOException("Direct result shape changed");
        }
        if (input>MAX_INPUT || resultBytes>MAX_RESULTS+CommandResult.MAX_BYTES || request.overflow!=(resultBytes>MAX_RESULTS)) throw new IOException("Direct aggregate input/result bound exceeded");
        if (request.unsubmittedStatus!=null && (request.unsubmittedStatus!=CommandResult.Status.STALE_SCHEMA || request.pending!=null || request.cursor()>=request.items.size() || request.overflow || request.results.stream().anyMatch(result -> result.status()!=CommandResult.Status.SUCCESS))) throw new IOException("Invalid retained unsubmitted schema rejection");
        if (request.pending!=null) { if (request.pendingIndex<0 || request.pendingIndex>=request.items.size() || request.pendingIndex!=request.cursor() && request.pendingIndex!=request.cursor()-1) throw new IOException("Invalid direct pending cursor"); var item=request.items.get(request.pendingIndex); if (!request.pending.sessionId().equals(item.operationId().sessionId()) || request.pending.epoch()!=item.operationId().epoch()) throw new IOException("Direct pending grant changed"); }
    }
    private static byte[] blob(ByteBuffer in,int maximum) throws IOException { int size=in.getInt(); if (size<0 || size>maximum || size>in.remaining()) throw new IOException("Invalid direct journal blob"); byte[] value=new byte[size]; in.get(value); return value; }
    private static boolean flag(ByteBuffer in) throws IOException { int flag=in.get() & 255; if (flag>1) throw new IOException("Invalid direct journal flag"); return flag==1; }
    private void requireOpen() throws IOException { if (closed || fenced || !lock.isValid()) throw new IOException("Direct journal closed/fenced; reopen it"); }
    public void close() throws IOException { if (closed && !channel.isOpen()) return; closed=true; request=null; try { if (lock.isValid()) lock.release(); } finally { channel.close(); } }
}
