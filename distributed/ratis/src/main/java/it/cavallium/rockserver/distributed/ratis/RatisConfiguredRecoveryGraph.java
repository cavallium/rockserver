package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** One node-owned frozen discovery graph. No removal API and no recovery-host authority. */
final class RatisConfiguredRecoveryGraph implements AutoCloseable {
    static final int MAX_BYTES=64<<20;
    private static final int MAGIC=0x52434752,MAX_DIRECTORIES=RatisNodePlan.MAX_DOMAINS+3*RatisConfiguredRecoveryDiscovery.MAX_DEFINITIONS+RatisNodePlan.MAX_LOCAL_GROUPS;
    @FunctionalInterface interface Admission {void beforeBytes(long retainedEncodedBytes,int incomingEncodedBytes)throws IOException;}
    record Snapshot(RatisConfiguredRecoveryDiscovery.Plan plan,List<TopologyChange.Definition> retainedDefinitions,List<Request> frozenRequests,Map<UUID,Request> activeRequests) {
        Snapshot {retainedDefinitions=List.copyOf(retainedDefinitions);frozenRequests=List.copyOf(frozenRequests);activeRequests=Map.copyOf(activeRequests);}
    }
    private final RatisNodePlan node;private final RatisJournalAccess files;private final Admission heap;private final Path file;
    private final FileChannel channel;private final FileLock lock;
    private Snapshot current,dirty;private byte[] dirtyBody;private long currentBytes,dirtyBytes;private boolean closed;
    RatisConfiguredRecoveryGraph(Path directory,RatisNodePlan node,RatisJournalAccess files,Admission heap)throws IOException {
        this.node=Objects.requireNonNull(node);this.files=Objects.requireNonNull(files);this.heap=Objects.requireNonNull(heap);
        var root=directory.toAbsolutePath().normalize();if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS))throw new IOException("Graph requires an existing owned directory");file=root.resolve("graph");
        channel=FileChannel.open(root.resolve("graph.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try {
            try{acquired=channel.tryLock();}catch(OverlappingFileLockException busy){throw new IOException("Recovery graph already owned",busy);}if(acquired==null)throw new IOException("Recovery graph already owned");lock=acquired;
            files.register(file,MAX_BYTES);int count=0;try(var entries=Files.newDirectoryStream(root)){for(var entry:entries){String name=entry.getFileName().toString();if(++count>3 || !Set.of("graph","graph.pending","graph.lock").contains(name) || !Files.isRegularFile(entry,LinkOption.NOFOLLOW_LINKS) || Files.size(entry)>(name.equals("graph.lock")?4096:MAX_BYTES))throw new IOException("Foreign or oversized recovery graph file");}}
            if(Files.exists(file,LinkOption.NOFOLLOW_LINKS)){
                byte[] body=read(file);var restored=decode(body);
                // A prior rename can be visible after a lost directory-force acknowledgement.
                // Establish its durability after full validation before publishing any authority.
                SessionJournalFiles.force(file);SessionJournalFiles.force(root);
                current=restored;currentBytes=new Tables(current).retainedSize();
            }
            var pending=root.resolve("graph.pending");if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)){
                heap.beforeBytes(retainedEncodedBytes(),65536);
                if(!validChecksum(pending)){Files.delete(pending);SessionJournalFiles.force(root);files.deleted(pending);}
                else {byte[] body=read(pending);var candidate=decode(body);requireProgress(current,candidate);dirty=candidate;dirtyBody=body;dirtyBytes=new Tables(candidate).retainedSize();promotePending(pending);}
            }
        }catch(IOException|RuntimeException|Error failure){if(acquired!=null)acquired.release();channel.close();throw failure;}
    }
    synchronized Snapshot snapshot(){return current;}
    /** Raw retained graph and uncertain encoded candidate; caller keeps its admission until close. */
    synchronized long retainedEncodedBytes(){return currentBytes+(dirtyBody==null?0:dirtyBytes+dirtyBody.length+32);}
    synchronized void force(RatisConfiguredRecoveryDiscovery.Plan plan,List<Request> selected)throws IOException {
        requireOpen();Objects.requireNonNull(plan);Objects.requireNonNull(selected);
        if(selected.size()>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IOException("Active recovery requests exceed node bound");
        long entries=selected.size()+plan.roots().size()+3L*plan.definitions().size();if(current!=null)entries=Math.addExact(entries,current.frozenRequests().size()+3L*current.retainedDefinitions().size());
        heap.beforeBytes(retainedEncodedBytes(),Math.toIntExact(Math.addExact(4096L,Math.multiplyExact(64L,entries))));
        var active=selection(plan,selected);
        if(dirty!=null){if(!samePlan(plan,dirty.plan()) || !sameRequests(mergeRequests(current,selected),dirty.frozenRequests()) || !sameSelection(active,dirty.activeRequests()))throw new IOException("Unknown graph force requires exact original candidate retry");settle();return;}
        var retained=mergeDefinitions(current,plan.definitions());var requests=mergeRequests(current,selected);var candidate=new Snapshot(plan,retained,requests,active);requireProgress(current,candidate);
        var tables=new Tables(candidate);int size=tables.size();heap.beforeBytes(retainedEncodedBytes(),Math.toIntExact(tables.retainedSize()+size+32));validate(candidate);byte[] body=tables.encode();
        dirty=candidate;dirtyBody=body;dirtyBytes=new Tables(candidate).retainedSize();settle();
    }
    synchronized boolean retryPending()throws IOException {requireOpen();if(dirty==null)return false;settle();return true;}
    private void promotePending(Path pending)throws IOException {
        long peak=(Files.exists(file,LinkOption.NOFOLLOW_LINKS)?Files.size(file):0)+Files.size(pending);
        var reservation=new RatisJournalAccess.Reservation(file,MAX_BYTES,peak);files.reserve(reservation);
        // The exact validated sidecar already owns its disk bytes. Publish by metadata rename,
        // including an initial journal with no canonical predecessor, without a second body copy.
        SessionJournalFiles.force(pending);Files.move(pending,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
        SessionJournalFiles.force(file);SessionJournalFiles.force(file.getParent());files.fulfilled(reservation);
        current=dirty;currentBytes=dirtyBytes;dirty=null;dirtyBody=null;dirtyBytes=0;
    }
    private void settle()throws IOException {heap.beforeBytes(retainedEncodedBytes(),0);files.write(file,dirtyBody,MAX_BYTES);current=dirty;currentBytes=dirtyBytes;dirty=null;dirtyBody=null;dirtyBytes=0;}
    private byte[] read(Path path)throws IOException {long size=Files.size(path);if(size<32 || size>MAX_BYTES)throw new IOException("Recovery graph exceeds frame bound");heap.beforeBytes(retainedEncodedBytes(),Math.toIntExact(size));return files.read(path,MAX_BYTES);}
    private void requireOpen()throws IOException{if(closed)throw new IOException("Recovery graph closed");}
    @Override public synchronized void close()throws IOException {if(closed)return;closed=true;try{lock.release();}finally{channel.close();}}

    private static List<TopologyChange.Definition> mergeDefinitions(Snapshot previous,List<TopologyChange.Definition> proposed)throws IOException {
        var result=new TreeMap<UUID,TopologyChange.Definition>();if(previous!=null)for(var d:previous.retainedDefinitions())result.put(d.intentId(),d);
        for(var d:proposed){var old=result.putIfAbsent(d.intentId(),d);if(old!=null && !Arrays.equals(old.digest(),d.digest()))throw new IOException("Frozen recovery definition changed");}
        if(result.size()>RatisConfiguredRecoveryDiscovery.MAX_DEFINITIONS || result.values().stream().mapToLong(RatisConfiguredRecoveryGraph::definitionBytes).sum()>RatisConfiguredRecoveryDiscovery.MAX_DEFINITION_BYTES)throw new IOException("Retained recovery definitions exhausted");return List.copyOf(result.values());
    }
    private static List<Request> mergeRequests(Snapshot previous,List<Request> proposed)throws IOException {
        var result=new TreeMap<UUID,Request>();if(previous!=null)for(var request:previous.frozenRequests())result.put(request.transferId(),request);
        for(var request:proposed){var old=result.putIfAbsent(request.transferId(),request);if(old!=null && !sameRequest(old,request))throw new IOException("Frozen recovery request UUID changed body");}
        if(result.size()>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IOException("Retained recovery attempts exhausted");return List.copyOf(result.values());
    }
    private static boolean sameRequest(Request a,Request b){return a.transferId().equals(b.transferId()) && a.catalogue().equals(b.catalogue()) && a.receiver().equals(b.receiver()) && a.purpose()==b.purpose() && a.sourceGroup().equals(b.sourceGroup()) && a.sourceDirectory()!=null && b.sourceDirectory()!=null && Arrays.equals(a.sourceDirectory().payloadDigest(),b.sourceDirectory().payloadDigest()) && (a.header()==null?b.header()==null:b.header()!=null && Arrays.equals(a.header().definitionDigest(),b.header().definitionDigest()));}
    private static boolean sameRequests(List<Request> a,List<Request> b){if(a.size()!=b.size())return false;for(int i=0;i<a.size();i++)if(!sameRequest(a.get(i),b.get(i)))return false;return true;}
    private static Set<String> rootKeys(RatisConfiguredRecoveryDiscovery.Plan plan){var result=new HashSet<String>();for(var r:plan.roots())result.add(r.directory().domainId()+":"+HexFormat.of().formatHex(r.directory().payloadDigest())+":"+r.range().rangeId()+":"+r.range().groupId()+":"+r.range().rangeEpoch());return result;}
    private static Set<String> definitions(List<TopologyChange.Definition> definitions){var result=new HashSet<String>();for(var d:definitions)result.add(HexFormat.of().formatHex(d.digest()));return result;}
    private static Set<String> edges(RatisConfiguredRecoveryDiscovery.Plan plan){var result=new HashSet<String>();for(var e:plan.edges())result.add(e.parent()+":"+e.child()+":"+HexFormat.of().formatHex(e.consuming().definitionDigest()));return result;}
    private static Set<String> obligations(RatisConfiguredRecoveryDiscovery.Plan plan){var result=new HashSet<String>();for(var o:plan.localObligations())result.add(o.purpose()+":"+o.range().groupId()+":"+HexFormat.of().formatHex(o.sourceDirectory().payloadDigest())+":"+o.localReplica()+":"+(o.header()==null?"":HexFormat.of().formatHex(o.header().definitionDigest())));return result;}
    private static boolean samePlan(RatisConfiguredRecoveryDiscovery.Plan a,RatisConfiguredRecoveryDiscovery.Plan b){return a.catalogueCut().equals(b.catalogueCut()) && a.roots().size()==b.roots().size() && a.definitions().size()==b.definitions().size() && a.edges().size()==b.edges().size() && a.localObligations().size()==b.localObligations().size() && rootKeys(a).equals(rootKeys(b)) && definitions(a.definitions()).equals(definitions(b.definitions())) && edges(a).equals(edges(b)) && obligations(a).equals(obligations(b));}
    private static Map<UUID,RangeDirectory> rootDirectories(RatisConfiguredRecoveryDiscovery.Plan plan)throws IOException {
        var result=new TreeMap<UUID,RangeDirectory>();for(var root:plan.roots()){var old=result.putIfAbsent(root.directory().domainId(),root.directory());if(old!=null && !Arrays.equals(old.payloadDigest(),root.directory().payloadDigest()))throw new IOException("Conflicting recovery root directories");}return result;
    }
    private void requireProgress(Snapshot previous,Snapshot next)throws IOException {
        if(previous==null)return;if(!RatisRelocationOwnerRpc.covers(next.plan().catalogueCut(),previous.plan().catalogueCut()))throw new IOException("Recovery graph catalogue cut regressed");
        if(!definitions(next.retainedDefinitions()).containsAll(definitions(previous.retainedDefinitions())))throw new IOException("Recovery graph discarded immutable definitions");
        var before=rootDirectories(previous.plan());var after=rootDirectories(next.plan());if(!before.keySet().equals(after.keySet()))throw new IOException("Recovery graph domain set changed");
        for(var entry:before.entrySet()){var a=entry.getValue();var b=after.get(entry.getKey());if(b.generation()<a.generation() || b.schemaEpoch()<a.schemaEpoch() || b.generation()==a.generation() && !Arrays.equals(a.payloadDigest(),b.payloadDigest()) || b.schemaEpoch()==a.schemaEpoch() && !Arrays.equals(a.schemaDigest(),b.schemaDigest()))throw new IOException("Recovery graph directory regressed or changed at same generation");}
        var requests=new HashMap<UUID,Request>();for(var r:next.frozenRequests())requests.put(r.transferId(),r);for(var r:previous.frozenRequests())if(requests.get(r.transferId())==null || !sameRequest(r,requests.get(r.transferId())))throw new IOException("Recovery graph discarded/changed frozen attempt");
        var oldIds=new HashSet<UUID>();for(var request:previous.frozenRequests())oldIds.add(request.transferId());
        for(var entry:next.activeRequests().entrySet()){var beforeActive=previous.activeRequests().get(entry.getKey());if(oldIds.contains(entry.getValue().transferId()) && (beforeActive==null || !beforeActive.transferId().equals(entry.getValue().transferId())))throw new IOException("Inactive historical recovery attempt cannot become active again");}
        if(next.plan().catalogueCut().equals(previous.plan().catalogueCut()) && !rootKeys(next.plan()).equals(rootKeys(previous.plan())))throw new IOException("Recovery roots changed at the same catalogue cut");
    }
    private void validate(Snapshot snapshot)throws IOException {
        var roots=rootDirectories(snapshot.plan());if(!roots.keySet().equals(node.domains().stream().map(RatisNodePlan.Domain::domainId).collect(java.util.stream.Collectors.toSet())))throw new IOException("Recovery graph domains differ from node plan");
        var births=new HashMap<UUID,TopologyChange.Definition>();var byDigest=new HashMap<String,TopologyChange.Definition>();
        for(var d:snapshot.retainedDefinitions()){if(d.kind()==TopologyChange.Kind.RELOCATE || !d.sourceDirectory().clusterId().equals(node.clusterId()) || !d.sourceDirectory().databaseId().equals(node.databaseId()) || !roots.containsKey(d.sourceDirectory().domainId()))throw new IOException("Recovery graph definition crossed scope");byDigest.put(HexFormat.of().formatHex(d.digest()),d);for(var r:d.targets()){var old=births.putIfAbsent(r.groupId(),d);if(old!=null)throw new IOException("Recovery graph has conflicting group births");}}
        requireRetainedGraph(snapshot.retainedDefinitions());
        long graphBytes=new Tables(snapshot).retainedSize();var cut=snapshot.plan().catalogueCut();var source=new RatisConfiguredRecoveryDiscovery.Source(){
            public RatisCatalogueViewRpc.View view(RatisCatalogueViewRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException {var d=roots.get(request.domain());admission.beforeReply(Math.toIntExact(ScopedTransactionRpc.HEADER_BYTES+104+RangeDirectoryCodec.encodedSize(d)));return new RatisCatalogueViewRpc.View(request,cut,d,null);}
            public RatisRecoveryLineageRpc.Reply birth(RatisRecoveryLineageRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException {var d=births.get(request.group());admission.beforeReply(Math.toIntExact(ScopedTransactionRpc.HEADER_BYTES+156+(d==null?0:32+definitionBytes(d))));return new RatisRecoveryLineageRpc.Reply(request,cut,d);}
        };
        var actual=RatisConfiguredRecoveryDiscovery.discover(node,source,(retained,incoming)->heap.beforeBytes(retainedEncodedBytes()+graphBytes+retained,incoming));if(!samePlan(actual,snapshot.plan()))throw new IOException("Recovery graph is not its canonical discovered lineage");
        var active=selection(snapshot.plan(),snapshot.activeRequests().values());if(!sameSelection(active,snapshot.activeRequests()))throw new IOException("Active recovery group key differs from request");var frozen=new HashMap<UUID,Request>();for(var request:snapshot.frozenRequests())frozen.put(request.transferId(),request);
        for(var entry:active.entrySet()){var retained=frozen.get(entry.getValue().transferId());if(!entry.getKey().equals(entry.getValue().sourceGroup()) || retained==null || !sameRequest(entry.getValue(),retained))throw new IOException("Active recovery selection lost frozen request");}
        for(var request:snapshot.frozenRequests()) {
            if(request.relocation() || !request.catalogue().equals(node.catalogue().groupId()) || !request.clusterId().equals(node.clusterId()) || !request.databaseId().equals(node.databaseId()) || !roots.containsKey(request.domainId()) || !request.receiver().nodeId().equals(node.nodeId()) || !request.receiver().nodeIncarnation().equals(node.nodeIncarnation()) || !request.receiver().peerId().equals(node.peerId()) || !request.receiver().address().equals(node.address()))throw new IOException("Frozen recovery attempt crossed node/scope");
            if(request.metadataOnly()){var definition=byDigest.get(HexFormat.of().formatHex(request.header().definitionDigest()));if(definition==null || !Arrays.equals(definition.sourceDirectory().payloadDigest(),request.sourceDirectory().payloadDigest()))throw new IOException("Frozen metadata attempt lacks retained consuming definition");if(!Arrays.equals(request.header().encode(),definition.header().encode()))throw new IOException("Frozen metadata header changed its immutable definition");}
            else if(request.header()!=null)throw new IOException("CURRENT attempt must use ordinary current scope");
        }
    }
    private static void requireRetainedGraph(List<TopologyChange.Definition> definitions)throws IOException {
        var ranges=new HashMap<UUID,RangeDescriptor>();var domains=new HashMap<UUID,UUID>();var consumers=new HashMap<UUID,UUID>();var parents=new HashMap<UUID,List<UUID>>();
        for(var definition:definitions) {
            for(var directory:List.of(definition.sourceDirectory(),definition.targetDirectory()))for(var range:directory.ranges()) {
                var previous=ranges.putIfAbsent(range.groupId(),range);var domain=domains.putIfAbsent(range.groupId(),directory.domainId());
                if(previous!=null && (!previous.rangeId().equals(range.rangeId()) || previous.rangeEpoch()!=range.rangeEpoch() || !Arrays.equals(previous.lowerInclusive(),range.lowerInclusive()) || !Arrays.equals(previous.upperExclusive(),range.upperExclusive())) || domain!=null && !domain.equals(directory.domainId()))throw new IOException("Retained graph reused conflicting group geometry");
            }
            for(var parent:definition.parents()){var old=consumers.putIfAbsent(parent.groupId(),definition.intentId());if(old!=null && !old.equals(definition.intentId()))throw new IOException("Retained ancestor has conflicting consumers");}
            for(var target:definition.targets())parents.put(target.groupId(),definition.parents().stream().map(RangeDescriptor::groupId).toList());
        }
        var visiting=new HashSet<UUID>();var visited=new HashSet<UUID>();for(var group:parents.keySet())visitRetained(group,parents,visiting,visited);
    }
    private static void visitRetained(UUID group,Map<UUID,List<UUID>> parents,Set<UUID> visiting,Set<UUID> visited)throws IOException {
        if(visited.contains(group))return;if(!visiting.add(group))throw new IOException("Retained recovery graph cycle");for(var parent:parents.getOrDefault(group,List.of()))visitRetained(parent,parents,visiting,visited);visiting.remove(group);visited.add(group);
    }
    private static Map<UUID,Request> selection(RatisConfiguredRecoveryDiscovery.Plan plan,Collection<Request> selected)throws IOException {
        if(selected.size()!=plan.localObligations().size() || selected.size()>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IOException("Active recovery requests must cover every local obligation exactly once");
        var result=new HashMap<UUID,Request>();for(var request:selected)if(result.putIfAbsent(request.sourceGroup(),request)!=null)throw new IOException("Duplicate active recovery group");
        var seen=new HashSet<UUID>();for(var obligation:plan.localObligations()) {
            var request=result.get(obligation.range().groupId());
            if(!seen.add(obligation.range().groupId()) || request==null || request.purpose()!=obligation.purpose() || !request.receiver().equals(obligation.localReplica()) || request.sourceDirectory()==null || !Arrays.equals(request.sourceDirectory().payloadDigest(),obligation.sourceDirectory().payloadDigest()) || (request.header()==null?obligation.header()!=null:obligation.header()==null || !Arrays.equals(request.header().definitionDigest(),obligation.header().definitionDigest())))throw new IOException("Active recovery request differs from current discovered obligation");
        }
        return Map.copyOf(result);
    }
    private static boolean sameSelection(Map<UUID,Request> a,Map<UUID,Request> b){if(!a.keySet().equals(b.keySet()))return false;for(var entry:a.entrySet())if(!sameRequest(entry.getValue(),b.get(entry.getKey())))return false;return true;}
    /** An incomplete unpublished sidecar cannot have authorized host work. Valid but foreign data is never discarded. */
    private static boolean validChecksum(Path path)throws IOException {
        long size=Files.size(path);if(size<32)return false;if(size>MAX_BYTES)throw new IOException("Recovery pending frame exceeds bound");
        try(var channel=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            java.security.MessageDigest digest;try{digest=java.security.MessageDigest.getInstance("SHA-256");}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
            var buffer=java.nio.ByteBuffer.allocate(65536);long remaining=size-32;
            while(remaining>0){buffer.clear().limit((int)Math.min(buffer.capacity(),remaining));int read=channel.read(buffer);if(read<0)return false;if(read==0)continue;digest.update(buffer.array(),0,read);remaining-=read;}
            byte[] checksum=new byte[32];var tail=java.nio.ByteBuffer.wrap(checksum);while(tail.hasRemaining())if(channel.read(tail)<0)return false;
            return channel.size()==size && java.security.MessageDigest.isEqual(digest.digest(),checksum);
        }
    }
    private static long definitionBytes(TopologyChange.Definition d){return 29+RangeDirectoryCodec.encodedSize(d.sourceDirectory())+RangeDirectoryCodec.encodedSize(d.targetDirectory());}
    private final class Tables {
        final Snapshot snapshot;final List<RangeDirectory> directories;final List<TopologyChange.Definition> defs;final List<Request> requests;final Map<String,Integer> indices=new HashMap<>();final Map<UUID,Integer> requestIndices=new HashMap<>();
        Tables(Snapshot snapshot)throws IOException {this.snapshot=snapshot;var all=new TreeMap<String,RangeDirectory>();for(var root:snapshot.plan().roots())add(all,root.directory());for(var d:snapshot.retainedDefinitions()){add(all,d.sourceDirectory());add(all,d.targetDirectory());}for(var r:snapshot.frozenRequests())add(all,r.sourceDirectory());if(all.size()>MAX_DIRECTORIES)throw new IOException("Recovery graph directory table exhausted");directories=List.copyOf(all.values());for(int i=0;i<directories.size();i++)indices.put(key(directories.get(i)),i);defs=snapshot.retainedDefinitions().stream().sorted(Comparator.comparing(TopologyChange.Definition::intentId)).toList();requests=snapshot.frozenRequests().stream().sorted(Comparator.comparing(Request::transferId)).toList();for(int i=0;i<requests.size();i++)requestIndices.put(requests.get(i).transferId(),i);}
        void add(Map<String,RangeDirectory> all,RangeDirectory d)throws IOException {if(d==null)throw new IOException("Recovery graph has no exact directory");if(RangeDirectoryCodec.encodedSize(d)>RatisCatalogue.MAX_DIRECTORY_BYTES)throw new IOException("Recovery graph directory exceeds catalogue bound");all.putIfAbsent(key(d),d);}
        String key(RangeDirectory d){return HexFormat.of().formatHex(d.payloadDigest());}int index(RangeDirectory d){return indices.get(key(d));}
        int size()throws IOException {long size=4+32+16+4+4+4+4+4L*node.domains().size()+57L*defs.size()+45L*requests.size()+4+4L*snapshot.activeRequests().size();for(var d:directories)size+=4+RangeDirectoryCodec.encodedSize(d);if(size+32>MAX_BYTES)throw new IOException("Recovery graph exceeds frame bound");return Math.toIntExact(size);}
        long retainedSize()throws IOException {return size()+defs.stream().mapToLong(RatisConfiguredRecoveryGraph::definitionBytes).sum();}
        byte[] encode()throws IOException {var bytes=new ByteArrayOutputStream(size());var out=new DataOutputStream(bytes);out.writeInt(MAGIC);out.write(node.identityDigest());out.writeLong(snapshot.plan().catalogueCut().term());out.writeLong(snapshot.plan().catalogueCut().index());out.writeInt(directories.size());for(var d:directories){byte[] encoded=RangeDirectoryCodec.encode(d);out.writeInt(encoded.length);out.write(encoded);}var roots=rootDirectories(snapshot.plan());out.writeInt(roots.size());for(var d:roots.values())out.writeInt(index(d));out.writeInt(defs.size());for(var d:defs){uuid(out,d.intentId());out.writeByte(d.kind().ordinal());out.writeInt(index(d.sourceDirectory()));out.writeInt(index(d.targetDirectory()));out.write(d.digest());}out.writeInt(requests.size());for(var r:requests){uuid(out,r.transferId());out.writeByte(r.purpose().ordinal());out.writeInt(index(r.sourceDirectory()));uuid(out,r.sourceGroup());out.writeInt(r.range().replicas().indexOf(r.receiver()));int definition=-1;if(r.header()!=null)for(int i=0;i<defs.size();i++)if(Arrays.equals(r.header().definitionDigest(),defs.get(i).digest()))definition=i;out.writeInt(definition);}out.writeInt(snapshot.activeRequests().size());for(var entry:new TreeMap<>(snapshot.activeRequests()).entrySet())out.writeInt(requestIndices.get(entry.getValue().transferId()));out.flush();if(bytes.size()!=size())throw new IOException("Recovery graph sizing mismatch");return bytes.toByteArray();}
    }
    private Snapshot decode(byte[] body)throws IOException {
        try {
            var in=new DataInputStream(new ByteArrayInputStream(body));if(in.readInt()!=MAGIC || !Arrays.equals(in.readNBytes(32),node.identityDigest()))throw new IOException("Recovery graph node binding differs");var cut=new ApplyPosition(in.readLong(),in.readLong());
            int count=count(in,MAX_DIRECTORIES);var directories=new ArrayList<RangeDirectory>();for(int i=0;i<count;i++){int n=in.readInt();if(n<1 || n>RatisCatalogue.MAX_DIRECTORY_BYTES || n>in.available())throw new IOException("Recovery graph directory framing invalid");directories.add(RangeDirectoryCodec.decode(in.readNBytes(n)));}
            int rootsCount=count(in,RatisNodePlan.MAX_DOMAINS);var roots=new TreeMap<UUID,RangeDirectory>();for(int i=0;i<rootsCount;i++){var d=directories.get(in.readInt());if(roots.put(d.domainId(),d)!=null)throw new IOException("Duplicate recovery root domain");}
            count=count(in,RatisConfiguredRecoveryDiscovery.MAX_DEFINITIONS);var definitions=new ArrayList<TopologyChange.Definition>();var definitionIds=new HashSet<UUID>();long definitionBytes=0;for(int i=0;i<count;i++){UUID id=uuid(in);if(!definitionIds.add(id))throw new IOException("Duplicate recovery definition intent ID");int kind=in.readUnsignedByte();var source=directories.get(in.readInt());var target=directories.get(in.readInt());byte[] digest=in.readNBytes(32);long incoming=29+RangeDirectoryCodec.encodedSize(source)+RangeDirectoryCodec.encodedSize(target);if(definitionBytes+incoming>RatisConfiguredRecoveryDiscovery.MAX_DEFINITION_BYTES)throw new IOException("Retained recovery definition bytes exhausted");heap.beforeBytes(retainedEncodedBytes()+body.length+definitionBytes,Math.toIntExact(incoming));definitionBytes+=incoming;var definition=new TopologyChange.Definition(id,TopologyChange.Kind.values()[kind],source,target);if(!Arrays.equals(digest,definition.digest()))throw new IOException("Frozen definition digest changed");definitions.add(definition);}
            count=count(in,RatisNodePlan.MAX_LOCAL_GROUPS);var requests=new ArrayList<Request>();var transferIds=new HashSet<UUID>();for(int i=0;i<count;i++){UUID id=uuid(in);if(!transferIds.add(id))throw new IOException("Duplicate frozen recovery transfer UUID");int purpose=in.readUnsignedByte();var directory=directories.get(in.readInt());UUID group=uuid(in);int receiver=in.readInt(),definition=in.readInt();var range=directory.ranges().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow();requests.add(new Request(id,definition<0?null:definitions.get(definition).header(),node.catalogue().groupId(),range.replicas().get(receiver),Purpose.values()[purpose],group,directory));}
            count=count(in,RatisNodePlan.MAX_LOCAL_GROUPS);var active=new HashMap<UUID,Request>();for(int i=0;i<count;i++){var request=requests.get(in.readInt());if(active.putIfAbsent(request.sourceGroup(),request)!=null)throw new IOException("Duplicate active recovery group");}
            if(in.available()!=0)throw new IOException("Trailing recovery graph bytes");
            var births=new HashMap<UUID,TopologyChange.Definition>();for(var d:definitions)for(var r:d.targets())if(births.put(r.groupId(),d)!=null)throw new IOException("Conflicting recovery graph births");
            var source=new RatisConfiguredRecoveryDiscovery.Source(){public RatisCatalogueViewRpc.View view(RatisCatalogueViewRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException{var d=roots.get(request.domain());if(d==null)throw new IOException("Missing recovery root domain");admission.beforeReply(Math.toIntExact(ScopedTransactionRpc.HEADER_BYTES+104+RangeDirectoryCodec.encodedSize(d)));return new RatisCatalogueViewRpc.View(request,cut,d,null);}public RatisRecoveryLineageRpc.Reply birth(RatisRecoveryLineageRpc.Request request,RatisConfiguredRecoveryDiscovery.ReplyAdmission admission)throws IOException{var d=births.get(request.group());admission.beforeReply(Math.toIntExact(ScopedTransactionRpc.HEADER_BYTES+156+(d==null?0:32+definitionBytes(d))));return new RatisRecoveryLineageRpc.Reply(request,cut,d);}};
            var plan=RatisConfiguredRecoveryDiscovery.discover(node,source,(retained,incoming)->heap.beforeBytes(retainedEncodedBytes()+body.length+retained,incoming));var snapshot=new Snapshot(plan,definitions,requests,active);validate(snapshot);var tables=new Tables(snapshot);heap.beforeBytes(retainedEncodedBytes()+body.length+tables.retainedSize(),tables.size());if(!Arrays.equals(body,tables.encode()))throw new IOException("Noncanonical recovery graph frame");return snapshot;
        }catch(IllegalArgumentException|IndexOutOfBoundsException malformed){throw new IOException("Invalid recovery graph",malformed);}
    }
    private static int count(DataInputStream in,int maximum)throws IOException {int count=in.readInt();if(count<0 || count>maximum || count>in.available()/4)throw new IOException("Recovery graph count exceeds bound");return count;}
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
}
