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
    @Test void coldUnpublishedCaptureAcknowledgesOneSuccessorAndPreservesOldTerminalIdentity()throws Exception{
        var owned=Files.createDirectory(root.resolve("owned"));try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"))){var a=source.request(50);RatisTopologySourceSnapshotRpc.Abandoned receipt;
            try(var artifacts=open(owned)){artifacts.reserve(a);artifacts.publicationForceObserver(phase->{if(phase==RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING)throw new IOException("Unknown capturing acknowledgement before image");});try(var pin=source.store.capture()){assertThrows(IOException.class,()->artifacts.capture(a,source.definition,pin,source.schema,source.donor));}assertEquals(0,source.store.activeSnapshotPins());assertEquals(16L<<20,artifacts.reservedImageBytes());}
            source.store.applyNoop(new ApplyPosition(1,source.store.applied().index()+1));
            try(var artifacts=open(owned)){assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING,artifacts.publicationPhase(a));try(var pin=source.store.capture()){assertThrows(RatisTopologySourceSnapshotArtifacts.OriginalCutUnavailable.class,()->artifacts.capture(a,source.definition,pin,source.schema,source.donor));}receipt=artifacts.abandon(a,source.store.configuration(),source.store.applied());assertEquals(0,artifacts.reservedImageBytes());assertArrayEquals(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,receipt),RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,artifacts.abandoned(a)));assertThrows(RatisTopologySourceSnapshotRpc.AbandonedException.class,()->artifacts.lookup(a));}
            try(var artifacts=open(owned)){assertEquals(receipt.successorId(),artifacts.abandoned(a).successorId());var b=receipt.successorRequest();assertNotEquals(a.transferId(),b.transferId());artifacts.reserve(b);try(var pin=source.store.capture()){artifacts.capture(b,source.definition,pin,source.schema,source.donor);var path=artifacts.originalImagePath(b,pin.position());var image=NativeRangeImage.write(path,pin,pin.interval(),limits);var prepared=new RatisTopologySourceSnapshotRpc.Prepared(b,image,source.schema,source.donor,pin.configuration(),Map.of());artifacts.retain(prepared,new SingleFileSnapshotInfo(new FileInfo(path,MD5FileUtil.computeMd5ForFile(path.toFile())),pin.position().term(),pin.position().index()));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(b));assertEquals(b.transferId(),artifacts.lookup(b).prepared().request().transferId());}assertEquals(receipt.successorId(),artifacts.abandoned(a).successorId());}
        }
    }
    @Test void uncertainReadyAndMissingImagesNeverPermitAbandonment()throws Exception{
        var owned=Files.createDirectory(root.resolve("ready"));try(var source=new RatisTopologySourceSnapshotFixture(root.resolve("native"));var lease=source.capture(source.request(51))){var request=lease.prepared().request();try(var artifacts=open(owned)){artifacts.publicationForceObserver(phase->{if(phase==RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY)throw new IOException("Lost READY acknowledgement");});assertThrows(IOException.class,()->artifacts.retain(lease.prepared(),lease.snapshot()));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(request));}
            try(var artifacts=open(owned)){var saved=artifacts.lookup(request);Files.delete(saved.image());Files.delete(saved.snapshot().getFiles().getFirst().getPath());assertThrows(IOException.class,()->artifacts.abandon(request,source.store.configuration(),source.store.applied()));assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,artifacts.publicationPhase(request));assertEquals(16L<<20,artifacts.reservedImageBytes());}
        }
    }
}
