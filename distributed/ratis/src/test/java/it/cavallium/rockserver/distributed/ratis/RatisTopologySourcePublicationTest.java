package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Genuine source cuts and files; full receiver/controller acceptance is a separate fixture. */
class RatisTopologySourcePublicationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,8L<<20,16);
    RatisTopologySourceSnapshotArtifacts open(Path owned)throws IOException{return new RatisTopologySourceSnapshotArtifacts(owned,limits,16L<<20,128L<<20,1);}
    @Test void partialUnpublishedSidecarPreservesValidatedCommittedCapture()throws Exception{
        var owned=Files.createDirectory(root.resolve("partial"));try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"))){var request=source.request(60);try(var artifacts=open(owned);var pin=source.store.capture()){artifacts.reserve(request);artifacts.capture(request,source.definition,pin,source.schema,source.donor);}var main=owned.resolve(request.transferId()+".publication");byte[] committed=Files.readAllBytes(main);Files.write(main.resolveSibling(main.getFileName()+".pending"),new byte[]{1,2,3});try(var artifacts=open(owned)){assertArrayEquals(committed,Files.readAllBytes(main));assertFalse(Files.exists(main.resolveSibling(main.getFileName()+".pending")));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING,artifacts.publicationPhase(request));assertEquals(16L<<20,artifacts.reservedImageBytes());}}
    }
    @Test void completeReadyAndAbandonedSidecarsRecoverExactTerminalAuthority()throws Exception{
        try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"))){
            for(boolean ready:new boolean[]{true,false}){var owned=Files.createDirectory(root.resolve(ready?"ready-sidecar":"abandoned-sidecar"));var request=source.request(ready?61:62);byte[] capturing,terminal;UUID successor=null;
                try(var artifacts=open(owned);var pin=source.store.capture()){artifacts.reserve(request);artifacts.capture(request,source.definition,pin,source.schema,source.donor);capturing=Files.readAllBytes(owned.resolve(request.transferId()+".publication"));if(ready){var file=artifacts.originalImagePath(request,pin.position());var image=NativeRangeImage.write(file,pin,pin.interval(),limits);var prepared=new RatisTopologySourceSnapshotRpc.Prepared(request,image,source.schema,source.donor,pin.configuration(),Map.of());artifacts.retain(prepared,new SingleFileSnapshotInfo(new FileInfo(file,MD5FileUtil.computeMd5ForFile(file.toFile())),pin.position().term(),pin.position().index()));}else successor=artifacts.abandon(request,source.store.configuration(),source.store.applied()).successorId();terminal=Files.readAllBytes(owned.resolve(request.transferId()+".publication"));}
                var main=owned.resolve(request.transferId()+".publication");Files.write(main,capturing);Files.write(main.resolveSibling(main.getFileName()+".pending"),terminal);
                try(var artifacts=open(owned)){assertArrayEquals(terminal,Files.readAllBytes(main));assertFalse(Files.exists(main.resolveSibling(main.getFileName()+".pending")));if(ready){assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(request));assertNotNull(artifacts.lookup(request));assertThrows(IOException.class,()->artifacts.abandon(request,source.store.configuration(),source.store.applied()));}else{assertEquals(successor,artifacts.abandoned(request).successorId());assertEquals(0,artifacts.reservedImageBytes());}}
                try(var artifacts=open(owned)){if(!ready)assertEquals(successor,artifacts.abandoned(request).successorId());}
            }
        }
    }
    RatisTopologySourceSnapshotArtifacts openIdentityBoundary(Path owned)throws IOException{return new RatisTopologySourceSnapshotArtifacts(owned,limits,32L<<20,512L<<20,2);}
    @Test void reservedOnlyIdentityKeepsPublicationCreditAtTerminalTableBoundary()throws Exception{
        var owned=Files.createDirectory(root.resolve("identity-bound"));try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"))){var seed=source.request(63);byte[] template;try(var artifacts=openIdentityBoundary(owned);var pin=source.store.capture()){artifacts.reserve(seed);artifacts.capture(seed,source.definition,pin,source.schema,source.donor);artifacts.abandon(seed,source.store.configuration(),source.store.applied());template=SessionJournalFiles.read(owned.resolve(seed.transferId()+".publication"),RatisTopologySourceSnapshotRpc.MAX_REQUEST_BYTES+RatisTopologySourceSnapshotRpc.MAX_OWNER_BYTES+177);}
            // Leave capacity for two captures so only the identity ceiling rejects the second request.
            for(int i=1;i<1023;i++){var request=source.request(1000+i);byte[] body=template.clone();byte[] framed=RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request);assertEquals(framed.length,java.nio.ByteBuffer.wrap(body).getInt(5));System.arraycopy(framed,0,body,9,framed.length);byte[] checksum=OperatorUpload.sha256(body);Files.write(owned.resolve(request.transferId()+".publication"),java.nio.ByteBuffer.allocate(body.length+32).put(body).put(checksum).array());SessionJournalFiles.write(owned.resolve(request.transferId()+".abandoned-settled"),checksum,64);}
            var a=source.request(3000);var b=source.request(3001);try(var artifacts=openIdentityBoundary(owned)){artifacts.reserve(a);artifacts.reserve(a);assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->artifacts.reserve(b));assertFalse(Files.exists(owned.resolve(b.transferId()+".reserved")));}
            try(var artifacts=openIdentityBoundary(owned);var pin=source.store.capture()){artifacts.reserve(a);artifacts.capture(a,source.definition,pin,source.schema,source.donor);assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING,artifacts.publicationPhase(a));assertThrows(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,()->artifacts.reserve(b));}
            try(var artifacts=openIdentityBoundary(owned)){assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING,artifacts.publicationPhase(a));}
        }
    }
    @Test void coldUnpublishedCaptureAcknowledgesOneSuccessorAndPreservesOldTerminalIdentity()throws Exception{
        var owned=Files.createDirectory(root.resolve("owned"));try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"))){var a=source.request(50);RatisTopologySourceSnapshotRpc.Abandoned receipt;
            try(var artifacts=open(owned)){artifacts.reserve(a);artifacts.publicationForceObserver(phase->{if(phase==RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING)throw new IOException("Unknown capturing acknowledgement before image");});try(var pin=source.store.capture()){assertThrows(IOException.class,()->artifacts.capture(a,source.definition,pin,source.schema,source.donor));}assertEquals(0,source.store.activeSnapshotPins());assertEquals(16L<<20,artifacts.reservedImageBytes());}
            source.store.applyNoop(new ApplyPosition(1,source.store.applied().index()+1));
            try(var artifacts=open(owned)){assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING,artifacts.publicationPhase(a));try(var pin=source.store.capture()){assertThrows(RatisTopologySourceSnapshotArtifacts.OriginalCutUnavailable.class,()->artifacts.capture(a,source.definition,pin,source.schema,source.donor));}receipt=artifacts.abandon(a,source.store.configuration(),source.store.applied());assertEquals(0,artifacts.reservedImageBytes());assertArrayEquals(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,receipt),RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,artifacts.abandoned(a)));assertThrows(RatisTopologySourceSnapshotRpc.AbandonedException.class,()->artifacts.lookup(a));}
            try(var artifacts=open(owned)){assertEquals(receipt.successorId(),artifacts.abandoned(a).successorId());var b=receipt.successorRequest();assertNotEquals(a.transferId(),b.transferId());artifacts.reserve(b);try(var pin=source.store.capture()){artifacts.capture(b,source.definition,pin,source.schema,source.donor);var path=artifacts.originalImagePath(b,pin.position());var image=NativeRangeImage.write(path,pin,pin.interval(),limits);var prepared=new RatisTopologySourceSnapshotRpc.Prepared(b,image,source.schema,source.donor,pin.configuration(),Map.of());artifacts.retain(prepared,new SingleFileSnapshotInfo(new FileInfo(path,MD5FileUtil.computeMd5ForFile(path.toFile())),pin.position().term(),pin.position().index()));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(b));assertEquals(b.transferId(),artifacts.lookup(b).prepared().request().transferId());}assertEquals(receipt.successorId(),artifacts.abandoned(a).successorId());}
        }
    }
    @Test void uncertainReadyAndMissingImagesNeverPermitAbandonment()throws Exception{
        var owned=Files.createDirectory(root.resolve("ready"));try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"));var lease=source.capture(source.request(51),NativeRangeImage.IoAdmission.UNLIMITED)){var request=lease.prepared().request();try(var artifacts=open(owned)){artifacts.publicationForceObserver(phase->{if(phase==RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY)throw new IOException("Lost READY acknowledgement");});assertThrows(IOException.class,()->artifacts.retain(lease.prepared(),lease.snapshot()));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(request));}
            try(var artifacts=open(owned)){var saved=artifacts.lookup(request);Files.delete(saved.image());Files.delete(saved.snapshot().getFiles().getFirst().getPath());assertThrows(IOException.class,()->artifacts.abandon(request,source.store.configuration(),source.store.applied()));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(request));assertEquals(16L<<20,artifacts.reservedImageBytes());}
        }
    }
}
