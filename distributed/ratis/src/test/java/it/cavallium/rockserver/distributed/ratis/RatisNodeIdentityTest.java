package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisNodeIdentityTest {
    @TempDir Path root;
    private static UUID id(long n){return new UUID(12030,n);}
    private static final RatisNativeExtentPolicy POLICY=RatisNativeExtentPolicy.defaults();
    private static ReplicaDescriptor local(){return new ReplicaDescriptor(id(1),id(2),id(3),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);}
    private static ReplicaDescriptor remote(){return new ReplicaDescriptor(id(4),id(5),id(6),"remote","127.0.0.1:9002",ReplicaDescriptor.Role.VOTER);}
    private static RatisNodePlan plan(long generation,long configuration,int domains) {
        var placements=new ArrayList<RatisNodePlan.Domain>();
        for(int i=0;i<domains;i++) {
            UUID column=id(30+i);var range=new RangeDescriptor(id(400+i),id(500+i),1,configuration,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local(),remote()));
            var directory=new RangeDirectory(id(10),id(11),id(20+i),1,new byte[32],generation,Set.of(column),0,List.of(range));placements.add(new RatisNodePlan.Domain(directory,Map.of(column,"data"+i),Set.of()));
        }
        var catalogue=new RatisNodePlan.Catalogue(id(6000),1,new byte[32],id(6001),id(6002),List.of(remote()));
        return new RatisNodePlan(id(1),id(2),placements,catalogue,List.of(),List.of(),Map.of(),domains,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(placements),RatisPoolFixtures.groups(catalogue,List.of()));
    }
    @Test void exactCurrentPurposesAndMonotonicExposureRoundTripWithoutGenesisByDefault() {
        var plan=plan(7,0,1);
        for(var purpose:List.of(RatisNodeIdentity.Purpose.INITIAL_PLACEMENT,RatisNodeIdentity.Purpose.RESTORE_TARGET)) {
            var identity=RatisNodeIdentity.create(plan,POLICY,purpose);assertEquals(0,identity.phase());assertFalse(identity.genesisOpen(id(20)));assertTrue(identity.genesisDomains().isEmpty());
            var exposed=identity.withPhase(1).withPhase(2);var decoded=RatisNodeIdentity.decode(exposed.encode(),plan,POLICY);
            assertEquals(purpose,decoded.purpose());assertEquals(2,decoded.phase());assertEquals(decoded.encodedSize(),decoded.encode().length);assertArrayEquals(exposed.encode(),decoded.encode());
            assertThrows(IllegalArgumentException.class,()->decoded.withPhase(1));assertThrows(IllegalArgumentException.class,()->decoded.closeGenesis(List.of(id(20))));
        }
    }
    @Test void genesisBindsExactZeroGenerationAndLocalCohortDespiteNormalizedPlanDigest() {
        var plan=plan(0,0,1);var identity=RatisNodeIdentity.create(plan,POLICY,RatisNodeIdentity.Purpose.GENESIS);var directory=plan.domains().getFirst().directory();var range=directory.ranges().getFirst();
        identity.requireGenesisScope(directory,range,local());assertThrows(IllegalArgumentException.class,()->identity.requireGenesisScope(directory,range,remote()));
        var progressed=plan(1,0,1);assertArrayEquals(plan.identityDigest(),progressed.identityDigest());
        assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.decode(identity.encode(),progressed,POLICY));
        assertThrows(IllegalArgumentException.class,()->identity.requireGenesisScope(progressed.domains().getFirst().directory(),range,local()));
        assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.create(progressed,POLICY,RatisNodeIdentity.Purpose.GENESIS));
        assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.create(plan(0,1,1),POLICY,RatisNodeIdentity.Purpose.GENESIS));
    }
    @Test void forcedPerDomainClosureSurvivesColdReadAndPreservesExposureAndOtherScope()throws Exception {
        var plan=plan(0,0,2);var identity=RatisNodeIdentity.create(plan,POLICY,RatisNodeIdentity.Purpose.GENESIS).withPhase(2);var path=root.resolve("node.identity");
        SessionJournalFiles.write(path,identity.encode(),RatisNodeIdentity.MAX_BYTES);var closed=identity.closeGenesis(List.of(id(20)));SessionJournalFiles.write(path,closed.encode(),RatisNodeIdentity.MAX_BYTES);
        var cold=RatisNodeIdentity.decode(SessionJournalFiles.read(path,RatisNodeIdentity.MAX_BYTES),plan,POLICY);assertEquals(2,cold.phase());assertEquals(RatisNodeIdentity.Purpose.GENESIS,cold.purpose());assertFalse(cold.genesisOpen(id(20)));assertTrue(cold.genesisOpen(id(21)));assertSame(cold,cold.closeGenesis(List.of(id(20))));
        var first=plan.directory(id(20));assertThrows(IllegalArgumentException.class,()->cold.requireGenesisScope(first,first.ranges().getFirst(),local()));
        var second=plan.directory(id(21));cold.requireGenesisScope(second,second.ranges().getFirst(),local());assertEquals(Set.of(id(20),id(21)),cold.genesisDomains());assertThrows(UnsupportedOperationException.class,()->cold.genesisDomains().clear());
    }
    @Test void maximumDomainInventoryFitsTheExactAdmittedFileCeiling()throws Exception {
        var plan=plan(0,0,RatisNodePlan.MAX_DOMAINS);var identity=RatisNodeIdentity.create(plan,POLICY,RatisNodeIdentity.Purpose.GENESIS);byte[] encoded=identity.encode();
        assertEquals(RatisNodeIdentity.MAX_BYTES,encoded.length+32);var path=root.resolve("maximum.identity");SessionJournalFiles.write(path,encoded,RatisNodeIdentity.MAX_BYTES);
        var cold=RatisNodeIdentity.decode(SessionJournalFiles.read(path,RatisNodeIdentity.MAX_BYTES),plan,POLICY);assertEquals(RatisNodePlan.MAX_DOMAINS,cold.genesisDomains().size());
        encoded[0]=2;assertEquals(0,cold.phase());assertEquals(0,identity.phase());
    }
    @Test void malformedForeignAndOversizedIdentitiesFailBeforeScopeUse()throws Exception {
        var plan=plan(0,0,2);byte[] exact=RatisNodeIdentity.create(plan,POLICY,RatisNodeIdentity.Purpose.GENESIS).encode();
        for(int length:List.of(0,149,exact.length-1,exact.length+1,RatisNodeIdentity.MAX_BYTES))assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.decode(Arrays.copyOf(exact,length),plan,POLICY));
        for(int offset:List.of(0,1,33,1+32+RatisNativeExtentPolicy.BYTES,150+48)) {
            byte[] malformed=exact.clone();malformed[offset]=(byte)255;assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.decode(malformed,plan,POLICY));
        }
        byte[] duplicate=exact.clone();System.arraycopy(duplicate,150,duplicate,150+49,16);assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.decode(duplicate,plan,POLICY));
        byte[] wrongCount=exact.clone();ByteBuffer.wrap(wrongCount).putInt(146,RatisNodePlan.MAX_DOMAINS+1);assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.decode(wrongCount,plan,POLICY));
        assertThrows(IllegalArgumentException.class,()->RatisNodeIdentity.decode(exact,plan(0,0,1),POLICY));
        var oversized=root.resolve("oversized.identity");Files.write(oversized,new byte[RatisNodeIdentity.MAX_BYTES+1]);assertThrows(java.io.IOException.class,()->SessionJournalFiles.read(oversized,RatisNodeIdentity.MAX_BYTES));
    }
}
