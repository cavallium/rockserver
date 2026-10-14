package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Only a forced verified-DONE manifest authorizes removal of this private job graph. */
final class RatisRerouteReclamation {
    private static final int MAGIC=0x5252434d,MAX_FILES=4*TransactionStatus.MAX_PARTICIPANTS+16;
    static final int MAX_BYTES=128+MAX_FILES*160;
    @FunctionalInterface interface Verify { void done(Path job)throws IOException; }
    @FunctionalInterface interface BeforeDelete { void run(Path job)throws IOException; }
    private record FileImage(String name,long bytes,byte[] digest) {}
    private record Manifest(OperationId operation,List<FileImage> files) {}
    private final RatisJournalAccess access;
    private final Path root;private final byte[] binding;private final SessionEpochLedger.Grant business;private final int stateMaximum;
    RatisRerouteReclamation(Path root,byte[] binding,SessionEpochLedger.Grant business,int stateMaximum,RatisJournalAccess access){this.access=Objects.requireNonNull(access);this.root=root;this.binding=binding.clone();this.business=business;this.stateMaximum=stateMaximum;}
    private Path marker(long sequence){return root.resolve("reclaim-"+sequence+".bin");}
    private static boolean markerName(String name){return name.matches("reclaim-(0|[1-9][0-9]*)\\.bin(\\.pending)?");}
    List<Path> jobs(int maximum)throws IOException {
        var jobs=new ArrayList<Path>();int entries=0;
        try(var paths=Files.newDirectoryStream(root)){for(var path:paths){if(++entries>3*maximum+1)throw new IOException("Reroute root entry budget exhausted");var name=path.getFileName().toString();
            if(name.equals("coordinator.lock")){if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)!=0)throw new IOException("Invalid coordinator ownership file");}
            else if(markerName(name)){if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)>MAX_BYTES)throw new IOException("Invalid reclamation manifest");}
            else if(name.matches("0|[1-9][0-9]*") && Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS)){try{Long.parseLong(name);}catch(NumberFormatException invalid){throw new IOException("Invalid original sequence directory",invalid);}jobs.add(path);}
            else throw new IOException("Foreign reroute root entry");
        }}
        if(jobs.size()>maximum)throw new IOException("Retained reroute job budget exhausted");return jobs.stream().sorted(Comparator.comparingLong(p->Long.parseLong(p.getFileName().toString()))).toList();
    }
    boolean hasMarkers()throws IOException {try(var files=Files.newDirectoryStream(root)){for(var file:files)if(markerName(file.getFileName().toString()))return true;}return false;}
    void recover(int maximum,Verify verify,SessionJournalFiles.Removal removal,BeforeDelete before)throws IOException {
        jobs(maximum);var markers=new TreeSet<Long>();try(var paths=Files.newDirectoryStream(root)){for(var path:paths){var name=path.getFileName().toString();if(markerName(name)){try{markers.add(Long.parseLong(name.substring(8,name.indexOf(".bin"))));}catch(NumberFormatException invalid){throw new IOException("Invalid reclamation sequence",invalid);}}}}
        for(long sequence:markers)resume(sequence,verify,removal,before);
    }
    void prepare(Path job,RatisOperationRerouteJournal input)throws IOException {
        var operation=input.originalCommand().operationId();requireOperation(operation);if(!job.equals(root.resolve(Long.toString(operation.sequence()))))throw new IOException("Reclamation crossed original job");
        var groups=new HashSet<UUID>();for(var route:input.targetDirectory().ranges())groups.add(route.groupId());var files=new ArrayList<FileImage>();
        collect(job,job,groups,files);if(files.stream().noneMatch(f->f.name.equals("input.bin")) || files.stream().noneMatch(f->f.name.equals("state.bin")))throw new IOException("Verified DONE journal graph is incomplete");
        files.sort(Comparator.comparing(FileImage::name));access.write(marker(operation.sequence()),encode(new Manifest(operation,List.copyOf(files))),MAX_BYTES);
    }
    private void collect(Path job,Path directory,Set<UUID> groups,List<FileImage> images)throws IOException {
        try(var paths=Files.newDirectoryStream(directory)){for(var path:paths){String name=job.relativize(path).toString();
            if(name.equals("bindings")){if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid bindings directory");collect(job,path,groups,images);continue;}
            if(!allowed(name,groups) || !Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign or uncertain reroute journal entry");long bytes=Files.size(path);if(bytes>bound(name) || images.size()>=MAX_FILES)throw new IOException("Owned journal graph exceeds reclamation budget");images.add(new FileImage(name,bytes,hash(path)));
        }}
    }
    private boolean allowed(String name,Set<UUID> groups){
        if(name.endsWith(".pending"))return false;
        if(!name.startsWith("bindings/"))return Set.of("owner.lock","input.bin","input.bin.lock","input.bin.atomic","input.bin.atomic.lock","issuer.bin","issuer.bin.lock","state.bin").contains(name);
        var leaf=name.substring(9);if(leaf.endsWith(".lock"))leaf=leaf.substring(0,leaf.length()-5);if(leaf.equals("home-business.bin") || leaf.equals("home-control.bin"))return true;
        if(!leaf.endsWith("-true.bin") && !leaf.endsWith("-false.bin"))return false;int suffix=leaf.endsWith("-true.bin")?9:10;try{return groups.contains(UUID.fromString(leaf.substring(0,leaf.length()-suffix)));}catch(IllegalArgumentException malformed){return false;}
    }
    private long bound(String name){if(name.endsWith(".lock"))return 0;if(name.startsWith("bindings/"))return RangeDirectoryCodec.MAX_ENCODED_BYTES+16384L;if(name.equals("input.bin"))return RatisOperationRerouteJournal.MAX_BYTES;if(name.equals("input.bin.atomic"))return RatisDurableTransaction.MAX_BYTES;if(name.equals("issuer.bin"))return 2L*SessionEpochLedger.MAX_BYTES+4096;return stateMaximum;}
    private void requireOperation(OperationId operation)throws IOException {if(!operation.sessionId().equals(business.sessionId()) || operation.epoch()!=business.epoch())throw new IOException("Reclamation changed original grant");}
    private void resume(long sequence,Verify verify,SessionJournalFiles.Removal removal,BeforeDelete before)throws IOException {
        Path marker=marker(sequence),pending=marker.resolveSibling(marker.getFileName()+".pending"),job=root.resolve(Long.toString(sequence));byte[] body;
        if(!Files.exists(marker,LinkOption.NOFOLLOW_LINKS)){
            body=access.read(pending,MAX_BYTES);var manifest=decode(body);if(manifest.operation.sequence()!=sequence)throw new IOException("Pending reclamation changed sequence");verify.done(job);verifyImages(job,manifest,false);SessionJournalFiles.force(pending);Files.move(pending,marker,StandardCopyOption.ATOMIC_MOVE);SessionJournalFiles.force(root);
        }else{body=access.read(marker,MAX_BYTES);if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)){if(!Arrays.equals(body,access.read(pending,MAX_BYTES)))throw new IOException("Foreign pending reclamation");removal.delete(pending);removal.force(root);}}
        var manifest=decode(body);if(manifest.operation.sequence()!=sequence)throw new IOException("Reclamation changed sequence");verifyImages(job,manifest,true);
        before.run(job);var channels=new ArrayList<FileChannel>();var locks=new ArrayList<FileLock>();
        try{
            for(var image:manifest.files)if(image.name.endsWith(".lock")){var path=job.resolve(image.name);if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS))continue;var channel=FileChannel.open(path,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);channels.add(channel);FileLock lock;try{lock=channel.tryLock();}catch(OverlappingFileLockException owned){throw new IOException("Reclamation journal remains owned",owned);}if(lock==null)throw new IOException("Reclamation journal remains owned");locks.add(lock);}
            verifyImages(job,manifest,true);
            for(var image:manifest.files)if(!image.name.endsWith(".lock")){var path=job.resolve(image.name);if(Files.exists(path,LinkOption.NOFOLLOW_LINKS)){checkImage(path,image);removal.delete(path);removal.force(path.getParent());access.deleted(path);}}
        }finally{IOException failure=null;for(var channel:channels)try{channel.close();}catch(IOException cleanup){if(failure==null)failure=cleanup;else failure.addSuppressed(cleanup);}if(failure!=null)throw failure;}
        for(var image:manifest.files)if(image.name.endsWith(".lock")){var path=job.resolve(image.name);if(Files.exists(path,LinkOption.NOFOLLOW_LINKS)){checkImage(path,image);removal.delete(path);removal.force(path.getParent());access.deleted(path);}}
        var bindings=job.resolve("bindings");if(Files.exists(bindings,LinkOption.NOFOLLOW_LINKS)){removal.delete(bindings);removal.force(job);}if(Files.exists(job,LinkOption.NOFOLLOW_LINKS)){removal.delete(job);removal.force(root);}access.reclaimedTree(job);access.remove(marker,body,MAX_BYTES,removal);
    }
    private void verifyImages(Path job,Manifest manifest,boolean missing)throws IOException {
        var expected=new HashMap<String,FileImage>();for(var image:manifest.files)expected.put(image.name,image);
        if(!Files.exists(job,LinkOption.NOFOLLOW_LINKS)){if(missing)return;throw new IOException("Pending reclamation lost its intact job");}
        if(!Files.isDirectory(job,LinkOption.NOFOLLOW_LINKS))throw new IOException("Reclamation job changed type");
        try(var paths=Files.newDirectoryStream(job)){for(var path:paths){var name=path.getFileName().toString();if(name.equals("bindings")){if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Reclamation bindings changed type");try(var entries=Files.newDirectoryStream(path)){for(var entry:entries)verifyImage(job,entry,expected);}}else verifyImage(job,path,expected);}}
        if(!missing && !expected.isEmpty())throw new IOException("Pending reclamation lost journal files");
    }
    private void verifyImage(Path job,Path path,Map<String,FileImage> expected)throws IOException {var image=expected.remove(job.relativize(path).toString());if(image==null)throw new IOException("Foreign file appeared during reclamation");checkImage(path,image);}
    private static void checkImage(Path path,FileImage image)throws IOException {if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)!=image.bytes || !MessageDigest.isEqual(hash(path),image.digest))throw new IOException("Owned reclamation file changed");}
    private static byte[] hash(Path path)throws IOException {try{var digest=MessageDigest.getInstance("SHA-256");try(var channel=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){var bytes=ByteBuffer.allocate(64*1024);while(channel.read(bytes)>=0){bytes.flip();digest.update(bytes);bytes.clear();}}return digest.digest();}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private byte[] encode(Manifest manifest)throws IOException {var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);out.writeInt(MAGIC);out.write(binding);out.writeLong(manifest.operation.sessionId().getMostSignificantBits());out.writeLong(manifest.operation.sessionId().getLeastSignificantBits());out.writeLong(manifest.operation.epoch());out.writeLong(manifest.operation.sequence());out.writeInt(manifest.files.size());for(var file:manifest.files){out.writeUTF(file.name);out.writeLong(file.bytes);out.write(file.digest);}out.flush();return bytes.toByteArray();}
    private Manifest decode(byte[] body)throws IOException {try{var in=new DataInputStream(new ByteArrayInputStream(body));if(in.readInt()!=MAGIC || !Arrays.equals(in.readNBytes(32),binding))throw new IOException("Reclamation binding changed");var operation=new OperationId(new UUID(in.readLong(),in.readLong()),in.readLong(),in.readLong());requireOperation(operation);int count=in.readInt();if(count<2 || count>MAX_FILES)throw new IOException("Invalid reclamation file count");var files=new ArrayList<FileImage>();String previous="";for(int n=0;n<count;n++){String name=in.readUTF();long size=in.readLong();byte[] digest=in.readNBytes(32);if(name.length()>96 || name.compareTo(previous)<=0 || size<0 || size>bound(name) || digest.length!=32 || !safeName(name))throw new IOException("Invalid reclamation file image");files.add(new FileImage(name,size,digest));previous=name;}var manifest=new Manifest(operation,List.copyOf(files));if(in.available()!=0 || !Arrays.equals(body,encode(manifest)))throw new IOException("Noncanonical reclamation manifest");return manifest;}catch(RuntimeException malformed){throw new IOException("Invalid reclamation manifest",malformed);}}
    private boolean safeName(String name){if(name.startsWith("bindings/")){var leaf=name.substring(9);if(leaf.endsWith(".lock"))leaf=leaf.substring(0,leaf.length()-5);return leaf.equals("home-business.bin") || leaf.equals("home-control.bin") || leaf.matches("[0-9a-f-]{36}-(true|false)\\.bin");}return allowed(name,Set.of());}
}
