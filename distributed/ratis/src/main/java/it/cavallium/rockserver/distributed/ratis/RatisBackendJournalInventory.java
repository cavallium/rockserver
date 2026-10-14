package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/** Complete cold inventory precedes opening any generation or issuing another native identity. */
final class RatisBackendJournalInventory {
    private RatisBackendJournalInventory(){}
    static void inventory(Path root,RatisRequestBudget budget)throws IOException {
        if(!Files.exists(root,LinkOption.NOFOLLOW_LINKS))return;
        int maximum=budget.limits().maximumJournalPaths();
        try(var traversal=budget.work(65536L+32768L*Math.min(64,maximum))){
            Files.walkFileTree(root,java.util.EnumSet.noneOf(FileVisitOption.class),64,new SimpleFileVisitor<>(){
                int directories,files,payloads;
                @Override public FileVisitResult preVisitDirectory(Path directory,BasicFileAttributes attributes)throws IOException {
                    if(++directories>maximum || directory.toString().length()>4096)throw new IOException("Backend journal directory inventory exceeds bound");return FileVisitResult.CONTINUE;
                }
                @Override public FileVisitResult visitFile(Path path,BasicFileAttributes attributes)throws IOException {
                    if(!attributes.isRegularFile())throw new IOException("Foreign backend journal object");
                    var relative=root.relativize(path);String leaf=path.getFileName().toString();if(relative.getNameCount()>1 && relative.getName(0).toString().equals("exports") && leaf.matches("[0-9a-f]{64}-[0-9a-f-]{36}-[0-9a-f-]{36}-[0-9]+\\.sst(\\.(download|building))?")){if(++payloads>3L*maximum)throw new IOException("Backend export payload entry inventory exceeds bound");return FileVisitResult.CONTINUE;}
                    if(++files>2L*maximum)throw new IOException("Backend journal file inventory exceeds bound");
                    String name=path.getFileName().toString();Path main=name.endsWith(".pending")?path.resolveSibling(name.substring(0,name.length()-8)):path;
                    budget.register(main,Integer.MAX_VALUE);return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}
