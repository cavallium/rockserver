package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisSessionBindingPlanTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static final UUID CLUSTER=new UUID(1,1),DATABASE=new UUID(1,2);
    private static RangeDirectory directory(int domain,byte[] boundary) {
        var replica=new ReplicaDescriptor(new UUID(2,1),new UUID(2,2),new UUID(2,3),"node","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(CLUSTER,DATABASE,new UUID(3,domain),1,new byte[32],0,Set.of(new UUID(4,domain)),0,List.of(
                new RangeDescriptor(new UUID(5,2L*domain),new UUID(6,2L*domain),1,0,null,boundary,RangeDescriptor.TopologyState.ACTIVE,List.of(replica)),
                new RangeDescriptor(new UUID(5,2L*domain+1),new UUID(6,2L*domain+1),1,0,boundary,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
    }
    private static RatisRoutedClient route(RangeDirectory directory,AtomicInteger calls) {
        return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
            public byte[] read(RangeDescriptor range,UUID column,byte[] key){calls.incrementAndGet();throw new AssertionError("Binding preflight must not dispatch");}
            public CommandResult command(RangeDescriptor range,DistributedCommand command){calls.incrementAndGet();throw new AssertionError("Binding preflight must not dispatch");}
        });
    }
    @Test void fourNamespacesKeepOnlySelectedGroupsAndRestoreFrozenDirectories()throws Exception {
        var calls=new AtomicInteger();var routes=new ArrayList<RatisRoutedClient>();var selections=new ArrayList<RatisSessionBindingPlan.Selection>();
        try {
            for(int domain=1;domain<=4;domain++) {
                var directory=directory(domain,new byte[]{10});routes.add(route(directory,calls));
                selections.add(new RatisSessionBindingPlan.Selection(directory,Set.of(directory.ranges().get(domain%2).groupId())));
            }
            var factory=new RatisRocksDBConnection.SessionFactory(routes,(path,schema,plan)->{throw new AssertionError("No grant before binding preflight");});
            var input=factory.bindings(selections);var plan=input.freeze();assertEquals(4,plan.targets().size());
            assertEquals(input.reservation(),plan.metadataBytes());
            var reversed=new ArrayList<>(selections);Collections.reverse(reversed);assertArrayEquals(plan.encode(),factory.bindings(reversed).freeze().encode());
            for(var routed:routes) {
                var old=routed.directory();routed.refresh(new RangeDirectory(old.clusterId(),old.databaseId(),old.domainId(),old.schemaEpoch(),old.schemaDigest(),1,old.columns(),old.routingPrefixBytes(),old.ranges()));
            }
            var restored=factory.restore(plan.encode());assertArrayEquals(plan.digest(),restored.digest());
            for(var selection:selections)assertArrayEquals(selection.directory().payloadDigest(),restored.directory(selection.directory().domainId()).payloadDigest());
            assertEquals(0,calls.get());
        } finally {for(var routed:routes)routed.close();}
    }
    @Test void foreignMissingAndDuplicateTargetsFailBeforeDispatch()throws Exception {
        var one=directory(1,new byte[]{10});var two=directory(2,new byte[]{10});var calls=new AtomicInteger();
        try(var first=route(one,calls);var second=route(two,calls)) {
            var selection=RatisSessionBindingPlan.Selection.all(one);
            assertThrows(IllegalArgumentException.class,()->new RatisSessionBindingPlan.Selection(one,Set.of(UUID.randomUUID())));
            assertThrows(IllegalArgumentException.class,()->RatisSessionBindingPlan.borrow(List.of(selection,selection),List.of(first)));
            assertThrows(IllegalArgumentException.class,()->RatisSessionBindingPlan.borrow(List.of(selection),List.of(second)));
            var duplicate=new RangeDirectory(CLUSTER,DATABASE,two.domainId(),1,new byte[32],0,two.columns(),0,one.ranges());
            try(var duplicated=route(duplicate,calls)) {
                assertThrows(IllegalArgumentException.class,()->RatisSessionBindingPlan.borrow(List.of(selection,RatisSessionBindingPlan.Selection.all(duplicate)),List.of(first,duplicated)));
            }
            var foreign=new RangeDirectory(UUID.randomUUID(),DATABASE,two.domainId(),1,new byte[32],0,two.columns(),0,two.ranges());
            try(var foreignRoute=route(foreign,calls)) {
                assertThrows(IllegalArgumentException.class,()->RatisSessionBindingPlan.borrow(List.of(selection,RatisSessionBindingPlan.Selection.all(foreign)),List.of(first,foreignRoute)));
            }
            assertEquals(0,calls.get());
        }
    }
    @Test void aggregateEncodingCeilingDoesNotMultiplyWithNamespaces()throws Exception {
        var selections=new ArrayList<RatisSessionBindingPlan.Selection>();var routes=new ArrayList<RatisRoutedClient>();var calls=new AtomicInteger();
        try {
            for(int domain=1;domain<=4;domain++){var directory=directory(domain,new byte[450_000]);selections.add(RatisSessionBindingPlan.Selection.all(directory));routes.add(route(directory,calls));}
            assertThrows(IllegalArgumentException.class,()->RatisSessionBindingPlan.borrow(selections,routes));assertEquals(0,calls.get());
        } finally {for(var routed:routes)routed.close();}
    }
    @Test void decoderRejectsNoncanonicalOrUnknownSelectedGroups()throws Exception {
        var directory=directory(1,new byte[]{10});
        try(var routed=route(directory,new AtomicInteger())) {
            byte[] encoded=RatisSessionBindingPlan.borrow(List.of(RatisSessionBindingPlan.Selection.all(directory)),List.of(routed)).freeze().encode();
            int groupsOffset=Math.toIntExact(12+RangeDirectoryCodec.encodedSize(directory));var swapped=encoded.clone();
            System.arraycopy(encoded,groupsOffset+16,swapped,groupsOffset,16);System.arraycopy(encoded,groupsOffset,swapped,groupsOffset+16,16);
            assertThrows(IOException.class,()->RatisSessionBindingPlan.restore(swapped,List.of(routed)));
            var unknown=encoded.clone();ByteBuffer.wrap(unknown).putLong(groupsOffset,99);assertThrows(IOException.class,()->RatisSessionBindingPlan.restore(unknown,List.of(routed)));
            assertThrows(IOException.class,()->RatisSessionBindingPlan.restore(Arrays.copyOf(encoded,encoded.length+1),List.of(routed)));
        }
    }
    @Test void nativeGrantBindsSelectedDataAndHomesAndRetainsLostReplyIdentity()throws Exception {
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("native"))) {
            var selections=new LinkedHashMap<UUID,RatisSessionBindingPlan.Selection>();var routes=new LinkedHashMap<UUID,RatisRoutedClient>();
            for(var target:RatisBackendSessionsTest.targets(fixture)) {
                selections.putIfAbsent(target.directory().domainId(),new RatisSessionBindingPlan.Selection(target.directory(),Set.of(target.group())));
                routes.putIfAbsent(target.directory().domainId(),target.routed());
            }
            assertEquals(4,selections.size());var plan=RatisSessionBindingPlan.borrow(List.copyOf(selections.values()),List.copyOf(routes.values())).freeze();
            var beforePositions=new HashMap<UUID,ApplyPosition>();for(var home:fixture.homes)home.data.stores.forEach((group,store)->beforePositions.put(group,store.applied()));
            long before=fixture.catalogue.epochs.read().nextEpoch();var data=fixture.access.values().iterator().next().routed();Path window=root.resolve("selected-window");
            fixture.loseDataBinding=true;assertThrows(IOException.class,()->RatisBackendSessions.window(window,data,fixture.catalogue.epochs,60000,RatisSessionClient.Limits.defaults(),plan.targets(),RatisJournalAccess.standalone()));
            assertEquals(before+1,fixture.catalogue.epochs.read().nextEpoch());
            var restored=RatisSessionBindingPlan.restore(plan.encode(),List.copyOf(routes.values()));
            try(var client=RatisBackendSessions.window(window,data,fixture.catalogue.epochs,60000,RatisSessionClient.Limits.defaults(),restored.targets(),RatisJournalAccess.standalone())) {
                assertTrue(client.pristine());assertEquals(before+1,fixture.catalogue.epochs.read().nextEpoch());
                try(var files=java.nio.file.Files.list(window)){assertEquals(plan.targets().size(),files.filter(file->file.getFileName().toString().endsWith(".identity")).count());}
            }
            var selectedData=plan.targets().stream().filter(target->fixture.access.containsKey(target.directory().domainId())).map(RatisBackendSessions.Target::group).collect(java.util.stream.Collectors.toSet());
            for(var home:fixture.homes)home.data.stores.forEach((group,store)->{if(selectedData.contains(group))assertTrue(store.applied().index()>beforePositions.get(group).index());else assertEquals(beforePositions.get(group),store.applied(),"Unselected DATA group must not receive a binding");});
        }
    }
}
