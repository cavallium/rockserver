package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Codec/scope tests: captured facts grant no installed-image or serving authority. */
class NativeRecoverySupersessionTest {
    final NativeTopologyFreezeTest helper=new NativeTopologyFreezeTest();
    final TopologyChange.Definition definition=helper.split(helper.outer.schema(),21701);
    final RangeDirectory directory=definition.sourceDirectory();
    final UUID group=definition.parents().getFirst().groupId(),catalogue=new UUID(0,21702);
    byte[] digest(int value){var result=new byte[32];Arrays.fill(result,(byte)value);return result;}
    byte[] receipt(long index,int hash){return ByteBuffer.allocate(48).putLong(1).putLong(index).put(digest(hash)).array();}
    NativeRecoverySupersession.Scope scope(){return new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,20),definition.header(),directory,group,helper.replica,digest(1),List.of(digest(2),digest(3)));}
    NativeRecoverySupersession.CapturedState captured(){return new NativeRecoverySupersession.CapturedState(digest(4),new ApplyPosition(1,3),digest(5),receipt(7,6),receipt(8,7),new ApplyPosition(1,9),new NativeRecoverySupersession.ConfigurationReference(1,4,RangeImportAdmission.sha256(new byte[]{9})));}
    NativeMetadataRecoveryRegistration metadata(UUID transfer,ApplyPosition image,byte[] config,NativeMetadataRecoveryRegistration.CurrentPredecessor predecessor){
        var freeze=new NativeTopologyFreeze(definition.header(),group,new ApplyPosition(1,10),NativeTopologyFreeze.boundsDigest(null,null),digest(8));
        return new NativeMetadataRecoveryRegistration(transfer,catalogue,new ApplyPosition(1,21),directory,freeze,helper.replica,helper.replica,1,4,config,image,digest(9),Set.of(),predecessor);
    }
    NativeMetadataRecoveryRegistration.CurrentPredecessor previous(){return new NativeMetadataRecoveryRegistration.CurrentPredecessor(digest(4),new ApplyPosition(1,3),digest(5));}

    NativeCurrentRangeRecovery current(ReplicaDescriptor receiver,UUID catalogue,ApplyPosition catCut,long generation,long epoch,UUID rangeId){
        var schema=helper.outer.schema();var voter=receiver.role()==ReplicaDescriptor.Role.VOTER?receiver:new ReplicaDescriptor(new UUID(0,500),new UUID(0,501),new UUID(0,502),"source-voter","127.0.0.1:9998",ReplicaDescriptor.Role.VOTER);
        var members=receiver.equals(voter)?List.of(receiver):List.of(receiver,voter);
        var range=new RangeDescriptor(rangeId,group,epoch,1,null,null,RangeDescriptor.TopologyState.ACTIVE,members);
        var directory=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),generation,schema.columnIds(),schema.routingPrefixBytes(),List.of(range));
        return new NativeCurrentRangeRecovery(new UUID(0,503),catalogue,catCut,directory,schema,group,receiver,voter,new ApplyPosition(1,3),digest(5),0,-1,new byte[]{9},null,helper.outer.names,Set.of(),Map.of());
    }
    @Test void currentPreflightAllowsRoleChangeButRequiresEveryReceiverIdentityField(){
        var r=helper.replica;var rangeId=definition.parents().getFirst().rangeId();
        var listener=new ReplicaDescriptor(r.nodeId(),r.nodeIncarnation(),r.replicaId(),r.peerId(),r.address(),ReplicaDescriptor.Role.LISTENER);
        var original=current(listener,catalogue,new ApplyPosition(1,10),0,scope().range().rangeEpoch(),rangeId);
        scope().requireCurrent(original);new NativeRecoverySupersession(scope(),captured()).requireCurrent(original);
        var changed=List.of(
                new ReplicaDescriptor(new UUID(0,600),r.nodeIncarnation(),r.replicaId(),r.peerId(),r.address(),r.role()),
                new ReplicaDescriptor(r.nodeId(),new UUID(0,601),r.replicaId(),r.peerId(),r.address(),r.role()),
                new ReplicaDescriptor(r.nodeId(),r.nodeIncarnation(),new UUID(0,602),r.peerId(),r.address(),r.role()),
                new ReplicaDescriptor(r.nodeId(),r.nodeIncarnation(),r.replicaId(),"different-peer",r.address(),r.role()),
                new ReplicaDescriptor(r.nodeId(),r.nodeIncarnation(),r.replicaId(),r.peerId(),"127.0.0.1:9997",r.role()));
        for(var receiver:changed)assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(receiver,catalogue,new ApplyPosition(1,10),0,scope().range().rangeEpoch(),rangeId)));
    }
    @Test void currentPreflightRejectsRegressedCatalogueDirectoryAndChangedRangeAuthority(){
        var r=helper.replica;var range=scope().range();
        assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(r,new UUID(0,700),new ApplyPosition(1,10),1,range.rangeEpoch(),range.rangeId())));
        assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(r,catalogue,new ApplyPosition(1,21),1,range.rangeEpoch(),range.rangeId())));
        assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(r,catalogue,new ApplyPosition(2,19),1,range.rangeEpoch(),range.rangeId())));
        assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(r,catalogue,new ApplyPosition(1,10),2,range.rangeEpoch(),range.rangeId())));
        assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(r,catalogue,new ApplyPosition(1,10),1,range.rangeEpoch()+1,range.rangeId())));
        assertThrows(IllegalArgumentException.class,()->scope().requireCurrent(current(r,catalogue,new ApplyPosition(1,10),1,range.rangeEpoch(),new UUID(0,701))));
        assertDoesNotThrow(()->scope().requireCurrent(current(r,catalogue,scope().catalogueCut(),directory.generation(),range.rangeEpoch(),range.rangeId())));
    }
    @Test void threeCurrentAliasesAreOneCanonicalImmutableColdFenceIdentity(){
        byte[] first=digest(255),second=digest(128),third=digest(0);var input=new ArrayList<>(List.of(first,third,second));var original=scope();
        var aliases=new NativeRecoverySupersession.Scope(original.catalogue(),original.catalogueCut(),original.header(),original.sourceDirectory(),original.groupId(),original.receiver(),original.selectedMetadataRequestDigest(),input);
        assertEquals(aliases.encodedSize(),aliases.encode().length);var ordered=aliases.currentRequestDigests();assertArrayEquals(digest(0),ordered.get(0));assertArrayEquals(digest(128),ordered.get(1));assertArrayEquals(digest(255),ordered.get(2));
        first[0]=1;input.clear();ordered.getFirst()[0]=1;
        var value=new NativeRecoverySupersession(aliases,new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,null));var cold=NativeRecoverySupersession.decode(value.encode());
        assertArrayEquals(value.digest(),cold.digest());assertArrayEquals(value.scopeDigest(),cold.scopeDigest());
        for(int id:new int[]{0,128,255})assertTrue(cold.scope().authorizesCurrentRequestDigest(digest(id)));
        assertFalse(cold.scope().authorizesCurrentRequestDigest(digest(127)));assertFalse(cold.scope().authorizesCurrentRequestDigest(new byte[31]));assertFalse(cold.scope().authorizesCurrentRequestDigest(null));
        assertThrows(UnsupportedOperationException.class,()->cold.scope().currentRequestDigests().clear());
        var changed=new NativeRecoverySupersession.Scope(original.catalogue(),original.catalogueCut(),original.header(),original.sourceDirectory(),original.groupId(),original.receiver(),original.selectedMetadataRequestDigest(),List.of(digest(0),digest(128)));
        assertFalse(Arrays.equals(aliases.digest(),changed.digest()));
    }
    @Test void aliasCountDuplicatesAndNoncanonicalWireOrderAreRejected(){
        var base=scope();
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.Scope(base.catalogue(),base.catalogueCut(),base.header(),base.sourceDirectory(),base.groupId(),base.receiver(),base.selectedMetadataRequestDigest(),List.of()));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.Scope(base.catalogue(),base.catalogueCut(),base.header(),base.sourceDirectory(),base.groupId(),base.receiver(),base.selectedMetadataRequestDigest(),List.of(digest(2),digest(2))));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.Scope(base.catalogue(),base.catalogueCut(),base.header(),base.sourceDirectory(),base.groupId(),base.receiver(),base.selectedMetadataRequestDigest(),Collections.nCopies(NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS+1,digest(2))));
        var maximum=new ArrayList<byte[]>();for(int n=0;n<NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS;n++)maximum.add(java.nio.ByteBuffer.allocate(32).putInt(n).array());
        var bounded=new NativeRecoverySupersession.Scope(base.catalogue(),base.catalogueCut(),base.header(),base.sourceDirectory(),base.groupId(),base.receiver(),base.selectedMetadataRequestDigest(),maximum);
        assertEquals(NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS,NativeRecoverySupersession.Scope.decode(bounded.encode()).currentRequestDigests().size());assertEquals(bounded.encodedSize(),bounded.encode().length);assertTrue(bounded.encodedSize()<=NativeRecoverySupersession.MAX_SCOPE_BYTES);
        var encoded=base.encode();int start=encoded.length-64;
        var reversed=encoded.clone();System.arraycopy(encoded,start+32,reversed,start,32);System.arraycopy(encoded,start,reversed,start+32,32);
        assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.Scope.decode(reversed));
        var duplicate=encoded.clone();System.arraycopy(encoded,start,duplicate,start+32,32);assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.Scope.decode(duplicate));
        var zero=encoded.clone();java.nio.ByteBuffer.wrap(zero).putInt(start-4,0);assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.Scope.decode(zero));
        var oversized=encoded.clone();java.nio.ByteBuffer.wrap(oversized).putInt(start-4,NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS+1);assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.Scope.decode(oversized));
    }
    @Test void canonicalRoundTripIsBoundedDefensiveAndRetainsIndependentFacts(){
        var scope=scope();var value=new NativeRecoverySupersession(scope,captured());
        assertEquals(scope.encode().length,scope.encodedSize());assertEquals(scope.header().encode().length,scope.header().encodedSize());assertEquals(scope,NativeRecoverySupersession.Scope.decode(scope.encode()));
        assertEquals(value,NativeRecoverySupersession.decode(value.encode()));
        assertEquals(value.encodedSize(),value.encode().length);assertTrue(value.encodedSize()<=NativeRecoverySupersession.MAX_BYTES);
        assertArrayEquals(RangeImportAdmission.sha256(value.encode()),value.digest());
        byte[] encoded=value.encode(),hash=value.digest(),current=value.captured().currentRegistrationDigest(),intent=value.captured().pendingIntent();
        encoded[0]^=1;hash[0]^=1;current[0]^=1;intent[0]^=1;
        assertEquals(value,NativeRecoverySupersession.decode(value.encode()));assertArrayEquals(digest(4),value.captured().currentRegistrationDigest());assertArrayEquals(receipt(8,7),value.captured().pendingIntent());
        byte[] selected=digest(1);var copied=new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,20),definition.header(),directory,group,helper.replica,selected,List.of(digest(2),digest(3)));selected[0]^=1;
        assertEquals(scope,copied);copied.selectedMetadataRequestDigest()[0]^=1;assertEquals(scope,copied);
    }
    @Test void rejectsMalformedTrailingUnknownAndUncoupledFields(){
        var value=new NativeRecoverySupersession(scope(),captured());var bytes=value.encode();
        assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.decode(Arrays.copyOf(bytes,bytes.length-1)));
        assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.decode(Arrays.copyOf(bytes,bytes.length+1)));
        var wrong=bytes.clone();wrong[0]^=1;assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.decode(wrong));
        var flags=bytes.clone();flags[8+scope().encode().length]=(byte)0x80;assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.decode(flags));
        assertThrows(IllegalArgumentException.class,()->NativeRecoverySupersession.Scope.decode(Arrays.copyOf(scope().encode(),scope().encode().length+1)));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.CapturedState(digest(1),null,digest(2),null,null,null,null));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.CapturedState(null,null,null,new byte[47],null,null,null));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.ConfigurationReference(1,-1,digest(1)));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.ConfigurationReference(1,4,new byte[31]));
    }
    @Test void exactOriginalScopeRejectsForeignReceiverDirectoryAndParent(){
        var foreign=new ReplicaDescriptor(new UUID(0,999),helper.replica.nodeIncarnation(),helper.replica.replicaId(),helper.replica.peerId(),helper.replica.address(),helper.replica.role());
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,20),definition.header(),directory,group,foreign,digest(1),List.of(digest(2),digest(3))));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,20),definition.header(),definition.targetDirectory(),group,helper.replica,digest(1),List.of(digest(2),digest(3))));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,20),definition.header(),directory,new UUID(0,998),helper.replica,digest(1),List.of(digest(2),digest(3))));
    }
    @Test void metadataRequiresImmutableAuthorityAndCapturedFloorsButNotSelectedRequestUuid(){
        var value=new NativeRecoverySupersession(scope(),captured());
        value.requireMetadata(metadata(new UUID(0,30),new ApplyPosition(1,12),new byte[]{9},previous()));
        value.requireMetadata(metadata(new UUID(0,31),new ApplyPosition(1,12),new byte[]{9},previous()));
        assertThrows(IllegalArgumentException.class,()->value.requireMetadata(metadata(new UUID(0,32),new ApplyPosition(1,12),new byte[]{8},previous())));
        assertThrows(IllegalArgumentException.class,()->value.requireMetadata(metadata(new UUID(0,32),new ApplyPosition(1,12),new byte[]{9},null)));
        var changed=new NativeMetadataRecoveryRegistration.CurrentPredecessor(digest(40),new ApplyPosition(1,3),digest(5));
        assertThrows(IllegalArgumentException.class,()->value.requireMetadata(metadata(new UUID(0,32),new ApplyPosition(1,12),new byte[]{9},changed)));
        var high=new NativeRecoverySupersession(scope(),new NativeRecoverySupersession.CapturedState(null,null,null,null,receipt(13,1),null,null));
        assertThrows(IllegalArgumentException.class,()->high.requireMetadata(metadata(new UUID(0,32),new ApplyPosition(1,12),new byte[]{9},null)));
        var foreignScope=new NativeRecoverySupersession.Scope(new UUID(0,400),scope().catalogueCut(),definition.header(),directory,group,helper.replica,digest(1),List.of(digest(2),digest(3)));
        assertThrows(IllegalArgumentException.class,()->new NativeRecoverySupersession(foreignScope,captured()).requireMetadata(metadata(new UUID(0,32),new ApplyPosition(1,12),new byte[]{9},previous())));
    }
    @Test void retainedMetadataScopeDoesNotDemandOriginalImageCoverLaterNativeTailOrCatalogueCut(){
        var scope=new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,30),definition.header(),directory,group,helper.replica,digest(1),List.of(digest(2),digest(3)));
        var captured=new NativeRecoverySupersession.CapturedState(digest(4),new ApplyPosition(1,3),digest(5),null,null,new ApplyPosition(1,15),new NativeRecoverySupersession.ConfigurationReference(1,15,digest(8)));
        var value=new NativeRecoverySupersession(scope,captured);var retained=metadata(new UUID(0,70),new ApplyPosition(1,12),new byte[]{9},previous());
        value.requireMetadataScope(retained);
        assertThrows(IllegalArgumentException.class,()->value.requireMetadata(retained));
        assertThrows(IllegalArgumentException.class,()->value.requireMetadataScope(metadata(new UUID(0,71),new ApplyPosition(1,12),new byte[]{9},null)));
    }
    @Test void positiveConfigurationWithoutMarkerOrPendingImageDoesNotManufactureAReadyPrefix(){
        var captured=new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,new NativeRecoverySupersession.ConfigurationReference(1,11,digest(7)));
        var restored=NativeRecoverySupersession.decode(new NativeRecoverySupersession(scope(),captured).encode());
        assertNull(restored.captured().marker());assertNull(restored.captured().pendingIntent());assertNull(restored.captured().localImageReceipt());assertNull(restored.captured().registeredImageCut());
        restored.requireMetadata(metadata(new UUID(0,60),new ApplyPosition(1,12),new byte[]{9},null));
        var beyond=new NativeRecoverySupersession(scope(),new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,new NativeRecoverySupersession.ConfigurationReference(2,11,digest(7))));
        assertThrows(IllegalArgumentException.class,()->beyond.requireMetadata(metadata(new UUID(0,61),new ApplyPosition(1,12),new byte[]{9},null)));
    }
}
