package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** Explicit file authority. Backend journals use their one shared budget; independent tools choose standalone IO. */
interface RatisJournalAccess {
    record Reservation(Path path,int maximum,long peakBytes) {
        public Reservation { if(maximum<32 || peakBytes<0 || peakBytes>2L*maximum)throw new IllegalArgumentException("Invalid journal reservation"); }
    }
    interface RootLease extends AutoCloseable {@Override void close()throws IOException;}
    RootLease backendRoot(Path root)throws IOException;
    void register(Path path,int maximum)throws IOException;
    byte[] read(Path path,int maximum)throws IOException;
    void write(Path path,byte[] body,int maximum)throws IOException;
    void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException;
    void deleted(Path path)throws IOException;
    void reclaimedTree(Path directory)throws IOException;
    void reserve(Reservation reservation)throws IOException;
    void fulfilled(Reservation reservation)throws IOException;
    default void preflight(List<Reservation> paths)throws IOException {for(var path:paths)reserve(path);}
    default void fulfilled(List<Reservation> paths)throws IOException {for(var path:paths)fulfilled(path);}
    static RatisJournalAccess standalone(){return Standalone.INSTANCE;}
    enum Standalone implements RatisJournalAccess {
        INSTANCE;
        public RootLease backendRoot(Path root)throws IOException{return new RatisBackendRootOwnership(root.toAbsolutePath().normalize(),this).retain(root.toAbsolutePath().normalize());}
        public void register(Path path,int maximum){}
        public byte[] read(Path path,int maximum)throws IOException{return SessionJournalFiles.read(path,maximum);}
        public void write(Path path,byte[] body,int maximum)throws IOException{SessionJournalFiles.write(path,body,maximum);}
        public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException{SessionJournalFiles.remove(path,body,maximum,removal);}
        public void deleted(Path path){}
        public void reclaimedTree(Path directory){}
        public void reserve(Reservation reservation){}
        public void fulfilled(Reservation reservation){}
    }
}
