package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue, client and dispatcher with controlled Raft transport. */
class RatisRecoveryLineageRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void freshRemoteBirthSurvivesSchemaProgressAndRejectsWrongAuthority()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)) {
            var binding=fixture.plan.catalogue();var schema=fixture.first;
            var catalogue=new RatisCatalogue(fixture.catalogue,schema.clusterId(),schema.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
            var original=fixture.firstDirectory.ranges().getFirst();var initial=catalogue.readView(schema.domainId());
            assertNull(catalogue.readRecoveryBirth(schema.domainId(),original.rangeId(),original.groupId(),original.rangeEpoch(),initial.cut()).birth(),"Absence is evidence only; no fabricated bootstrap header");
            var split=fixture.split();var leaf=split.targets().getFirst();var view=catalogue.readView(schema.domainId());
            var birth=catalogue.readRecoveryBirth(schema.domainId(),leaf.rangeId(),leaf.groupId(),leaf.rangeEpoch(),view.cut());
            assertArrayEquals(split.encode(),birth.birth().encode());assertEquals(leaf,birth.target());assertTrue(NativeMetadataRecoveryRegistration.covers(birth.catalogueCut(),view.cut()));
            fixture.createColumn(schema.domainId(),"after-birth");var later=catalogue.readView(schema.domainId());assertTrue(later.directory().schemaEpoch()>split.targetDirectory().schemaEpoch());
            var retained=catalogue.readRecoveryBirth(schema.domainId(),leaf.rangeId(),leaf.groupId(),leaf.rangeEpoch(),later.cut());
            assertArrayEquals(split.encode(),retained.birth().encode());assertEquals(split.parents(),retained.birth().parents());
            var request=retained.request();var message=Message.valueOf(ByteString.copyFrom(RatisRecoveryLineageRpc.request(request)));
            assertThrows(ExecutionException.class,()->fixture.catalogueMachine.query(message).get(10,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,()->fixture.catalogueMachine.queryStale(message,0).get(10,TimeUnit.SECONDS));
            var wrong=new RatisRecoveryLineageRpc.Request(new UUID(981,1),request.database(),request.catalogue(),request.column(),request.domain(),request.range(),request.group(),request.epoch(),request.minimumCatalogueCut());
            assertThrows(ExecutionException.class,()->fixture.catalogueMachine.queryLinearizable(message(wrong)).get(10,TimeUnit.SECONDS));
            var ahead=new RatisRecoveryLineageRpc.Request(request.cluster(),request.database(),request.catalogue(),request.column(),request.domain(),request.range(),request.group(),request.epoch(),new ApplyPosition(retained.catalogueCut().term(),retained.catalogueCut().index()+1));
            assertThrows(ExecutionException.class,()->fixture.catalogueMachine.queryLinearizable(message(ahead)).get(10,TimeUnit.SECONDS));
            fixture.catalogueLeadership=new RatisRangeStateMachine.Leadership(1,false);
            assertThrows(ExecutionException.class,()->fixture.catalogueMachine.queryLinearizable(message).get(10,TimeUnit.SECONDS));
            fixture.catalogueLeadership=new RatisRangeStateMachine.Leadership(1,true);
            // Exhausted protected proof admission rejects before invoking the native lookup.
            var queries=RatisRangeStateMachine.class.getDeclaredField("schemaAuthorityQueries");queries.setAccessible(true);var prior=queries.get(fixture.catalogueMachine);var attempted=new java.util.concurrent.atomic.AtomicInteger();
            var denied=RatisProofQueryService.authorityLane((group,bytes)->{attempted.incrementAndGet();throw new IOException("Controlled lineage workspace denial");});
            try {
                queries.set(fixture.catalogueMachine,denied);
                assertThrows(ExecutionException.class,()->fixture.catalogueMachine.queryLinearizable(message).get(10,TimeUnit.SECONDS));assertEquals(1,attempted.get());assertEquals(0,denied.stats().jobs());assertEquals(0,denied.stats().bytes());
            }finally{queries.set(fixture.catalogueMachine,prior);denied.stop().get(10,TimeUnit.SECONDS);}
            assertArrayEquals(split.encode(),catalogue.readRecoveryBirth(schema.domainId(),leaf.rangeId(),leaf.groupId(),leaf.rangeEpoch(),later.cut()).birth().encode());
        }
    }
    private static Message message(RatisRecoveryLineageRpc.Request request){return Message.valueOf(ByteString.copyFrom(RatisRecoveryLineageRpc.request(request)));}
}
