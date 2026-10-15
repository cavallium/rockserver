package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native foreground fencing remains independent of adapter/core eligibility publication. */
class NativeDiscoveryActivationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    @Test void captureAndHeavyActivationStayClosedUntilExactPublication()throws Exception {
        var nativeTest=new NativeTopologyCdcTest();var helper=nativeTest.helper;
        try(var f=nativeTest.new Fixture(root)) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));var cut=f.parent.applied();f.parent.deferDiscovery();
            assertFalse(f.parent.admissionOpen());assertThrows(IllegalStateException.class,f.parent::captureForRecovery);assertThrows(IllegalStateException.class,()->f.parent.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));
            try(var pin=f.parent.captureCurrentForDiscovery()) {
                assertEquals(cut,pin.position());long size=NativeRangeImage.encodedBytes(pin,nativeTest.limits);var image=NativeRangeImage.write(root.resolve("current.group"),pin,pin.interval(),nativeTest.limits);assertEquals(size,image.bytes());
            }
            f.parent.activate();assertFalse(f.parent.admissionOpen(),"Ordinary activation cannot clear the discovery fence");
            var activation=f.parent.prepareCurrentDiscoveryActivation(true,RangeMutationHistory.Limits.defaults());
            assertEquals(RangeOwnership.State.ACTIVE,f.parent.ownership().state());assertFalse(f.parent.admissionOpen());assertThrows(IllegalStateException.class,()->f.parent.apply(new ApplyPosition(1,2),f.write(f.parent,1,8)));assertEquals(cut,f.parent.applied());
            activation.publish();assertTrue(f.parent.admissionOpen());assertArrayEquals(helper.helper.key(9),f.parent.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertThrows(IllegalStateException.class,activation::publish);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.apply(new ApplyPosition(1,2),f.write(f.parent,1,8))).status());
            f.parent.deferDiscovery();assertThrows(java.io.IOException.class,()->f.parent.installCurrentDiscoverySnapshotAdmitted(root.resolve("current.group"),nativeTest.limits,batches->{},NativeRangeImage.IoAdmission.UNLIMITED));assertEquals(new ApplyPosition(1,2),f.parent.applied());var stale=f.parent.prepareCurrentDiscoveryActivation(true,RangeMutationHistory.Limits.defaults());f.parent.fence();assertThrows(IllegalStateException.class,stale::publish);assertFalse(f.parent.admissionOpen());
        }
    }
    @Test void emptyOwnerCannotAcquireCurrentCreationAuthority()throws Exception {
        var helper=new NativeTopologySeedTest();var definition=helper.topology.split(helper.outer.schema(),100);
        try(var pool=helper.pool(root)) {
            var parent=helper.open(pool,definition.parents().getFirst());parent.initializeConfiguration(new byte[]{1});assertNull(parent.applied());parent.deferDiscovery();assertThrows(IllegalStateException.class,parent::captureCurrentForDiscovery);assertThrows(IllegalStateException.class,()->parent.prepareCurrentDiscoveryActivation(true,RangeMutationHistory.Limits.defaults()));assertFalse(parent.admissionOpen());
        }
    }
    @Test void bornCurrentWithAbsentARequiresTypedImageRestorationAndOriginalSeed()throws Exception {
        var nativeTest=new NativeTopologyCdcTest();var helper=nativeTest.helper;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {
            source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));source.freeze();Path frozen=root.resolve("parent.group");NativeRangeImage.write(frozen,source.parent,nativeTest.limits);source.seed();Path image=root.resolve("child.group");var manifest=NativeRangeImage.write(image,source.child,nativeTest.limits);
            try(var pool=helper.pool(root.resolve("target"))) {
                var parent=helper.open(pool,source.definition.parents().getFirst());parent.initializeConfiguration(new byte[]{1});parent.fence();parent.installSnapshot(frozen,nativeTest.limits);parent.activate();
                var authority=TopologyChange.State.begin(source.definition.header()).beginFencing().frozen(source.freeze.proof(helper.topology.replica.replicaId()));var seed=pool.seedTopology(source.definition,authority,java.util.List.of(parent),helper.topology.replica);var target=source.definition.targets().getFirst();var child=pool.reopenTopologyRange(seed.birth(target.groupId()),helper.outer.schema());child.initializeConfiguration(new byte[]{1});child.deferDiscovery();assertNull(child.applied());
                assertThrows(java.io.IOException.class,()->child.installSnapshot(image,nativeTest.limits));assertNull(child.applied());child.validateCurrentDiscoveryImageOrigin(manifest.topologyOrigin());
                child.installCurrentDiscoverySnapshotAdmitted(image,nativeTest.limits,batches->{},NativeRangeImage.IoAdmission.UNLIMITED);assertEquals(manifest.position(),child.applied());assertFalse(child.admissionOpen());var activation=child.prepareCurrentDiscoveryActivation(true,RangeMutationHistory.Limits.defaults());assertFalse(child.admissionOpen());activation.publish();assertArrayEquals(helper.helper.key(9),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));
            }
        }
    }
    @Test void retiredOwnerKeepsMetadataCaptureWithoutCurrentForegroundAuthority()throws Exception {
        var nativeTest=new NativeTopologyCdcTest();
        try(var f=nativeTest.new Fixture(root)) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();f.seed();f.parent.deferDiscovery();
            assertThrows(IllegalStateException.class,f.parent::captureCurrentForDiscovery);assertThrows(IllegalStateException.class,()->f.parent.prepareCurrentDiscoveryActivation(true,RangeMutationHistory.Limits.defaults()));
            try(var pin=f.parent.captureMetadataForRecovery()){assertEquals(f.parent.applied(),pin.position());var image=NativeRangeImage.writeMetadataAdmitted(root.resolve("sealed.meta"),pin,pin.interval(),nativeTest.limits,NativeRangeImage.IoAdmission.UNLIMITED);assertEquals(pin.position(),image.position());}
            assertFalse(f.parent.admissionOpen());
        }
    }
}
