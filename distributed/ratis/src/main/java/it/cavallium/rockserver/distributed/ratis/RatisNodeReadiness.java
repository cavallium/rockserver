package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.util.LifeCycle;

/** Bounded local operational evidence. A report neither grants admission nor certifies quorum availability. */
public final class RatisNodeReadiness {
    private RatisNodeReadiness() {}
    public enum Reason { NODE_NOT_RUNNING, NODE_INSTALLING, NODE_CHANGED, MAINTENANCE_FAILED, RECOVERY_NOT_SCANNED, RECOVERY_PENDING, RECOVERY_FAILED, DIVISION_UNAVAILABLE, DIVISION_NOT_RUNNING, MACHINE_NOT_RUNNING, OWNER_CHANGED, STORAGE_FAILED, OBSERVER_FAILED, CONFIGURATION_PENDING, DISCOVERY_PENDING, CONFIGURATION_MISSING, CONFIGURATION_CHANGED, CONFIGURATION_UNQUALIFIED, CONFIGURATION_NOT_APPLIED, NATIVE_PREFIX_MISSING, CURRENT_TERM_NOT_APPLIED, APPLY_LAG, NATIVE_CUT_CHANGED, NATIVE_IDENTITY_CHANGED, NATIVE_ADMISSION_FENCED, IMPORT_FENCED, IMAGE_INSTALLING, TOPOLOGY_FENCED, SCHEMA_FENCED, SCHEMA_UNQUALIFIED, OPERATOR_UNQUALIFIED, PREPARED_RECOVERY_PENDING, SAMPLE_FAILED }
    public record Issue(Reason reason,String detail) {
        public Issue {Objects.requireNonNull(reason);detail=detail==null ? "" : detail.substring(0,Math.min(detail.length(),256));}
    }
    public record Recovery(boolean scanned,int unresolvedDecisions,int activeWorkers,Throwable failure) {
        public Recovery {if(unresolvedDecisions<0 || activeWorkers<0)throw new IllegalArgumentException("Negative recovery readiness count");}
    }
    public record NodeStamp(RatisNodeRuntime.State state,boolean installing,Object placementStamp,Recovery recovery,Throwable maintenanceFailure) {
        public NodeStamp {Objects.requireNonNull(state);Objects.requireNonNull(placementStamp);Objects.requireNonNull(recovery);}
    }
    public enum Discovery { SETTLED, PENDING, NONE_AT_CUT }
    public record MachineStamp(long admissionGeneration,long configurationStamp,boolean configurationPending,Discovery discovery,Throwable storageFailure,Throwable observerFailure) {
        public MachineStamp {Objects.requireNonNull(discovery);if(admissionGeneration<0 || configurationStamp<0)throw new IllegalArgumentException("Negative readiness owner stamp");}
    }
    @FunctionalInterface public interface NodeReader {NodeStamp read()throws Exception;}
    @FunctionalInterface public interface DivisionReader {RaftServer.Division read()throws Exception;}
    @FunctionalInterface public interface MachineReader {MachineStamp read()throws Exception;}
    public record GroupSource(RaftGroup expectedGroup,RangeDescriptor descriptor,UUID domain,byte[] schemaDigest,RatisNodePlan.Role role,RaftPeerId localPeer,boolean servingRequired,ReplicatedRangeStore store,RatisRangeStateMachine machine,DivisionReader division,MachineReader stamp) {
        public GroupSource {Objects.requireNonNull(expectedGroup);Objects.requireNonNull(role);Objects.requireNonNull(localPeer);Objects.requireNonNull(store);Objects.requireNonNull(machine);Objects.requireNonNull(division);Objects.requireNonNull(stamp);if(schemaDigest==null || schemaDigest.length!=32)throw new IllegalArgumentException("Invalid readiness schema identity");schemaDigest=schemaDigest.clone();if(descriptor!=null && !descriptor.groupId().equals(expectedGroup.getGroupId().getUuid()))throw new IllegalArgumentException("Readiness descriptor group differs");}
        @Override public byte[] schemaDigest(){return schemaDigest.clone();}
    }
    public record GroupStatus(UUID group,RatisNodePlan.Role role,boolean servingRequired,Discovery discovery,ApplyPosition nativeCut,ApplyPosition raftApplied,long committedIndex,long raftTerm,long configurationTerm,long configurationIndex,byte[] configurationDigest,int unresolvedPrepared,boolean recoverySampled,List<Issue> issues) {
        public GroupStatus {Objects.requireNonNull(group);Objects.requireNonNull(role);Objects.requireNonNull(discovery);configurationDigest=configurationDigest==null ? null : configurationDigest.clone();issues=List.copyOf(issues);if(unresolvedPrepared<0)throw new IllegalArgumentException("Negative prepared count");}
        @Override public byte[] configurationDigest(){return configurationDigest==null ? null : configurationDigest.clone();}
        public boolean servingReady(){return discovery!=Discovery.NONE_AT_CUT && issues.stream().noneMatch(issue->issue.reason()!=Reason.PREPARED_RECOVERY_PENDING);}
        public boolean recoverySettled(){return recoverySampled && unresolvedPrepared==0;}
    }
    public record Report(UUID node,UUID incarnation,RatisNodeRuntime.State state,List<GroupStatus> groups,boolean recoveryScanned,int unresolvedDecisions,int activeRecoveryWorkers,List<Issue> issues) {
        public Report {Objects.requireNonNull(node);Objects.requireNonNull(incarnation);Objects.requireNonNull(state);groups=List.copyOf(groups);issues=List.copyOf(issues);}
        public boolean localServingReady(){return issues.stream().noneMatch(issue->issue.reason()!=Reason.RECOVERY_NOT_SCANNED && issue.reason()!=Reason.RECOVERY_PENDING && issue.reason()!=Reason.RECOVERY_FAILED) && groups.stream().filter(GroupStatus::servingRequired).allMatch(GroupStatus::servingReady);}
        public boolean recoverySettled(){return recoveryScanned && unresolvedDecisions==0 && activeRecoveryWorkers==0 && issues.stream().noneMatch(issue->switch(issue.reason()){case RECOVERY_FAILED,SAMPLE_FAILED,NODE_CHANGED,NODE_INSTALLING,NODE_NOT_RUNNING -> true;default -> false;}) && groups.stream().allMatch(GroupStatus::recoverySettled);}
        public boolean ready(){return localServingReady() && recoverySettled();}
    }
    public static Report evaluate(UUID node,UUID incarnation,NodeReader nodeReader,Collection<GroupSource> requested) {
        Objects.requireNonNull(nodeReader);Objects.requireNonNull(requested);if(requested.isEmpty() || requested.size()>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IllegalArgumentException("Invalid bounded readiness group count");var sources=List.copyOf(requested);var unique=new HashSet<UUID>();for(var source:sources)if(!unique.add(source.expectedGroup().getGroupId().getUuid()))throw new IllegalArgumentException("Duplicate readiness group");var issues=new ArrayList<Issue>();NodeStamp before;
        try {before=Objects.requireNonNull(nodeReader.read());}catch(Exception failure){return new Report(node,incarnation,RatisNodeRuntime.State.OPEN,List.of(),false,0,0,List.of(failed(Reason.SAMPLE_FAILED,failure)));}
        if(before.state()!=RatisNodeRuntime.State.RUNNING)add(issues,Reason.NODE_NOT_RUNNING,before.state().name());if(before.installing())add(issues,Reason.NODE_INSTALLING,"");if(before.maintenanceFailure()!=null)issues.add(failed(Reason.MAINTENANCE_FAILED,before.maintenanceFailure()));
        var groups=new ArrayList<GroupStatus>(sources.size());sources.stream().sorted(Comparator.comparing(source->source.expectedGroup().getGroupId().getUuid())).forEach(source->groups.add(group(source)));
        var recovery=before.recovery();if(!recovery.scanned())add(issues,Reason.RECOVERY_NOT_SCANNED,"");if(recovery.unresolvedDecisions()!=0 || recovery.activeWorkers()!=0)add(issues,Reason.RECOVERY_PENDING,"decisions="+recovery.unresolvedDecisions()+", workers="+recovery.activeWorkers());if(recovery.failure()!=null)issues.add(failed(Reason.RECOVERY_FAILED,recovery.failure()));
        try {var after=Objects.requireNonNull(nodeReader.read());if(before.state()!=after.state() || before.installing()!=after.installing() || !before.placementStamp().equals(after.placementStamp()) || !before.recovery().equals(after.recovery()) || before.maintenanceFailure()!=after.maintenanceFailure())add(issues,Reason.NODE_CHANGED,"");}catch(Exception failure){issues.add(failed(Reason.SAMPLE_FAILED,failure));}
        return new Report(node,incarnation,before.state(),groups,recovery.scanned(),recovery.unresolvedDecisions(),recovery.activeWorkers(),issues);
    }
    private static GroupStatus group(GroupSource source) {
        UUID id=source.expectedGroup().getGroupId().getUuid();var issues=new ArrayList<Issue>();ApplyPosition nativeCut=null,raftApplied=null;long commit=-1,term=0,configurationTerm=0,configurationIndex=-1;byte[] configurationDigest=null;int prepared=0;boolean recoverySampled=false;
        try {
            var firstStamp=Objects.requireNonNull(source.stamp().read());stampIssues(firstStamp,issues);if(firstStamp.discovery()==Discovery.NONE_AT_CUT)return inert(source,firstStamp,issues);var division=source.division().read();if(division==null){add(issues,Reason.DIVISION_UNAVAILABLE,"");return status(source,nativeCut,raftApplied,commit,term,configurationTerm,configurationIndex,configurationDigest,prepared,false,issues);}
            var info=division.getInfo();var actual=division.getRaftConf();term=info.getCurrentTerm();if(info.getLifeCycleState()!=LifeCycle.State.RUNNING || !info.isAlive())add(issues,Reason.DIVISION_NOT_RUNNING,info.getLifeCycleState().name());if(source.machine().getLifeCycleState()!=LifeCycle.State.RUNNING)add(issues,Reason.MACHINE_NOT_RUNNING,source.machine().getLifeCycleState().name());if(division.getStateMachine()!=source.machine() || !division.getGroup().getGroupId().equals(source.expectedGroup().getGroupId()) || !division.getId().equals(source.localPeer()))add(issues,Reason.OWNER_CHANGED,"");var applied=source.machine().getLastAppliedTermIndex();if(applied!=null && applied.getIndex()>=0 && applied.getTerm()>=0)raftApplied=new ApplyPosition(applied.getTerm(),applied.getIndex());var log=division.getRaftLog();if(log!=null)commit=log.getLastCommittedIndex();else add(issues,Reason.APPLY_LAG,"Raft log unavailable");
            var store=source.store();var view=store.metadataOutcomeState();nativeCut=view.applied();var cfg=store.configuration();if(!view.groupId().equals(id) || source.domain()!=null && !source.domain().equals(view.domainId()) || !Arrays.equals(source.schemaDigest(),view.schemaDigest()) || source.descriptor()!=null && (view.rangeEpoch()!=source.descriptor().rangeEpoch() || !Arrays.equals(view.lowerInclusive(),source.descriptor().lowerInclusive()) || !Arrays.equals(view.upperExclusive(),source.descriptor().upperExclusive())))add(issues,Reason.NATIVE_IDENTITY_CHANGED,"");
            if(nativeCut==null || nativeCut.term()<1)add(issues,Reason.NATIVE_PREFIX_MISSING,"");else {if(nativeCut.term()!=term)add(issues,Reason.CURRENT_TERM_NOT_APPLIED,"");if(!nativeCut.equals(raftApplied) || commit>nativeCut.index() || info.getLastAppliedIndex()>nativeCut.index())add(issues,Reason.APPLY_LAG,"");}
            if(cfg==null)add(issues,Reason.CONFIGURATION_MISSING,"");else {configurationTerm=cfg.term();configurationIndex=cfg.index();configurationDigest=OperatorUpload.sha256(cfg.data());if(nativeCut==null || cfg.term()>nativeCut.term() || cfg.index()>nativeCut.index() || cfg.index()==nativeCut.index() && cfg.term()!=nativeCut.term())add(issues,Reason.CONFIGURATION_NOT_APPLIED,"");try {requireMembership(source,cfg,actual);}catch(IOException | RuntimeException invalid){issues.add(failed(Reason.CONFIGURATION_UNQUALIFIED,invalid));}}
            if(!store.admissionOpen())add(issues,Reason.NATIVE_ADMISSION_FENCED,store.ownership().state().name());if(store.importPending())add(issues,Reason.IMPORT_FENCED,"");if(store.pendingInstallation())add(issues,Reason.IMAGE_INSTALLING,"");if(store.topologyBirthPending() || store.topologyRetired() || store.topologyFreeze()!=null)add(issues,Reason.TOPOLOGY_FENCED,"");if(store.schemaFence()!=null)add(issues,Reason.SCHEMA_FENCED,"");
            if(source.role()==RatisNodePlan.Role.DATA){var schema=store.installedSchema();if(schema==null)add(issues,Reason.SCHEMA_UNQUALIFIED,"");else {var needed=new HashSet<UUID>();schema.columns().stream().filter(column->column.operator()!=null).forEach(column->needed.add(column.columnId()));if(!needed.equals(store.deterministicMergeColumns()))add(issues,Reason.OPERATOR_UNQUALIFIED,"");}prepared=store.topologyFreezeState().preparedParticipants();if(prepared!=0)add(issues,Reason.PREPARED_RECOVERY_PENDING,"participants="+prepared);}
            var last=store.metadataOutcomeState();var currentCfg=store.configuration();if(!view.equals(last))add(issues,Reason.NATIVE_CUT_CHANGED,"");if(!same(cfg,currentCfg))add(issues,Reason.CONFIGURATION_CHANGED,"");var finalStamp=Objects.requireNonNull(source.stamp().read());if(!firstStamp.equals(finalStamp) || division!=source.division().read() || division.getRaftConf()!=actual || division.getInfo().getCurrentTerm()!=term || division.getInfo().getLifeCycleState()!=LifeCycle.State.RUNNING || !division.getInfo().isAlive())add(issues,Reason.OWNER_CHANGED,"");
            recoverySampled=issues.stream().noneMatch(issue->switch(issue.reason()){case STORAGE_FAILED,OBSERVER_FAILED,DISCOVERY_PENDING,CONFIGURATION_PENDING,CONFIGURATION_MISSING,CONFIGURATION_CHANGED,CONFIGURATION_UNQUALIFIED,CONFIGURATION_NOT_APPLIED,NATIVE_PREFIX_MISSING,CURRENT_TERM_NOT_APPLIED,APPLY_LAG,NATIVE_IDENTITY_CHANGED,NATIVE_CUT_CHANGED,OWNER_CHANGED,DIVISION_NOT_RUNNING,MACHINE_NOT_RUNNING,SAMPLE_FAILED -> true;default -> false;});
        }catch(Exception failure){issues.add(failed(Reason.SAMPLE_FAILED,failure));}
        return status(source,nativeCut,raftApplied,commit,term,configurationTerm,configurationIndex,configurationDigest,prepared,recoverySampled,issues);
    }
    private static GroupStatus inert(GroupSource source,MachineStamp before,List<Issue> issues)throws Exception {
        if(source.role()!=RatisNodePlan.Role.DATA || source.servingRequired() || !source.machine().recoveryEligibilityGate().discoveryPending() || source.store().admissionOpen())add(issues,Reason.OWNER_CHANGED,"Invalid inert discovery owner");
        var division=source.division().read();
        if(division==null)add(issues,Reason.DIVISION_UNAVAILABLE,"");
        else {
            if(division.getStateMachine()!=source.machine() || !division.getGroup().getGroupId().equals(source.expectedGroup().getGroupId()) || !division.getId().equals(source.localPeer()))add(issues,Reason.OWNER_CHANGED,"");
            if(division.getInfo().getLifeCycleState()!=LifeCycle.State.RUNNING || !division.getInfo().isAlive())add(issues,Reason.DIVISION_NOT_RUNNING,"");
            if(source.machine().getLifeCycleState()!=LifeCycle.State.RUNNING)add(issues,Reason.MACHINE_NOT_RUNNING,"");
        }
        if(!before.equals(source.stamp().read()) || division!=source.division().read())add(issues,Reason.OWNER_CHANGED,"");
        return new GroupStatus(source.expectedGroup().getGroupId().getUuid(),source.role(),source.servingRequired(),Discovery.NONE_AT_CUT,null,null,-1,0,0,-1,null,0,issues.isEmpty(),issues);
    }
    private static GroupStatus status(GroupSource source,ApplyPosition nativeCut,ApplyPosition raftApplied,long commit,long term,long cfgTerm,long cfgIndex,byte[] digest,int prepared,boolean sampled,List<Issue> issues){return new GroupStatus(source.expectedGroup().getGroupId().getUuid(),source.role(),source.servingRequired(),issues.stream().anyMatch(issue->issue.reason()==Reason.DISCOVERY_PENDING)?Discovery.PENDING:Discovery.SETTLED,nativeCut,raftApplied,commit,term,cfgTerm,cfgIndex,digest,prepared,sampled,issues);}
    private static void stampIssues(MachineStamp stamp,List<Issue> issues){if(stamp.storageFailure()!=null)issues.add(failed(Reason.STORAGE_FAILED,stamp.storageFailure()));if(stamp.observerFailure()!=null)issues.add(failed(Reason.OBSERVER_FAILED,stamp.observerFailure()));if(stamp.configurationPending())add(issues,Reason.CONFIGURATION_PENDING,"");if(stamp.discovery()==Discovery.PENDING)add(issues,Reason.DISCOVERY_PENDING,"");}
    private static boolean same(ReplicatedRangeStore.Configuration a,ReplicatedRangeStore.Configuration b){return a==null ? b==null : b!=null && a.term()==b.term() && a.index()==b.index() && Arrays.equals(a.data(),b.data());}
    private static void requireMembership(GroupSource source,ReplicatedRangeStore.Configuration cfg,RaftConfiguration actual)throws IOException {
        if(cfg.data().length>65536 || actual==null)throw new IOException("Configuration unavailable/oversized");if(source.descriptor()!=null){RatisRangeStateMachine.requireSchemaMembership(source.descriptor(),cfg.data());RatisRangeStateMachine.requireActualMembership(actual,source.descriptor());}else {var nativeCfg=RaftConfigurationProto.parseFrom(cfg.data());if(nativeCfg.getOldPeersCount()!=0 || nativeCfg.getOldListenersCount()!=0)throw new IOException("Joint native configuration");var expected=new TreeMap<String,String>();for(var peer:source.expectedGroup().getPeers())expected.put(peer.getId().toString(),peer.getAddress()+"|"+(peer.getStartupRole()==RaftPeerRole.LISTENER ? "LISTENER" : "VOTER"));var nativePeers=new TreeMap<String,String>();for(var peer:nativeCfg.getPeersList())nativePeers.put(peer.getId().toStringUtf8(),peer.getAddress()+"|VOTER");for(var peer:nativeCfg.getListenersList())nativePeers.put(peer.getId().toStringUtf8(),peer.getAddress()+"|LISTENER");if(!nativePeers.equals(expected))throw new IOException("Native expected membership differs");var actualPeers=new TreeMap<String,String>();for(var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER)){if(!actual.getPreviousPeers(role).isEmpty())throw new IOException("Joint actual configuration");for(var peer:actual.getCurrentPeers(role))actualPeers.put(peer.getId().toString(),peer.getAddress()+"|"+(role==RaftPeerRole.LISTENER ? "LISTENER" : "VOTER"));}if(!actualPeers.equals(expected))throw new IOException("Actual expected membership differs");}
        if(actual.getLogEntryIndex()!=cfg.index() && !(cfg.term()==0 && cfg.index()==-1 && actual.getLogEntryIndex()<=0))throw new IOException("Actual/native configuration positions differ");
    }
    private static void add(List<Issue> issues,Reason reason,String detail){issues.add(new Issue(reason,detail));}
    private static Issue failed(Reason reason,Throwable failure){return new Issue(reason,failure.getClass().getSimpleName()+": "+Objects.toString(failure.getMessage(),""));}
}
