package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** One immutable issued grant and its exclusive consumers. Unknown outcomes reopen exact issuer/binding/window state. */
final class RatisBackendSessions {
    record Target(RatisRoutedClient routed,UUID group,RangeDirectory directory){Target{Objects.requireNonNull(routed);Objects.requireNonNull(group);Objects.requireNonNull(directory);if(directory.ranges().stream().noneMatch(range->range.groupId().equals(group)&&range.topologyState()==RangeDescriptor.TopologyState.ACTIVE))throw new IllegalArgumentException("Binding target absent from exact frozen directory");}}
    private RatisBackendSessions(){}
    static RatisGrantIssuer.Issued issue(Path root,RatisSessionEpochs epochs,long lifetime,RatisJournalAccess access)throws IOException{
        return issueBound(root,epochs,lifetime,List.of(),access);
    }
    static RatisGrantIssuer.Issued issueBound(Path root,RatisSessionEpochs epochs,long lifetime,List<Target> targets,RatisJournalAccess access)throws IOException{
        access.preflight(bindingReservations(root,targets,epochs));
        Files.createDirectories(root);Path file=root.resolve("issuer.bin");
        boolean exists=Files.exists(file,LinkOption.NOFOLLOW_LINKS);
        if(!exists && Files.exists(root.resolve("issuer.bin.lock"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used grant issuer state is missing");
        long expires=0;if(!exists){var ledger=epochs.read();if(ledger==null)throw new IOException("Native session catalogue is not initialized");try{expires=Math.addExact(ledger.logicalTimeMillis(),lifetime);}catch(ArithmeticException overflow){throw new IOException("Native session expiry overflows",overflow);}}
        try(var issuer=exists?RatisGrantIssuer.reopenOwned(file,epochs,access):RatisGrantIssuer.createOwned(file,epochs,expires,access)){
            issuer.beforeIssue(next->access.preflight(bindingReservations(root,targets,next)));
            for(int visits=0;visits<32;visits++){var phase=issuer.step();if(phase==RatisGrantIssuer.Phase.ISSUED)return issuer.issued();if(phase==RatisGrantIssuer.Phase.EXPIRED || phase==RatisGrantIssuer.Phase.EXHAUSTED)throw new IOException("Issued session allocation cannot advance");}
            throw new IOException("Grant allocation remains retained; reopen exact issuer");
        }
    }
    static void bind(Path root,RatisSessionEpochs epochs,RatisGrantIssuer.Issued issued,int window,List<Target> targets,RatisJournalAccess access)throws IOException{
        for(var target:targets){Path path=root.resolve("binding-"+target.group());Path identity=path.resolveSibling(path.getFileName()+".identity");var grant=issued.grant();
            byte[] expected=java.nio.ByteBuffer.allocate(128).putInt(0x424e4447).putLong(grant.epoch()).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits()).putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).putInt(window).putLong(target.group().getMostSignificantBits()).putLong(target.group().getLeastSignificantBits()).put(target.routed().sessionIdentityDigest()).put(target.directory().payloadDigest()).array();
            if(Files.exists(identity,LinkOption.NOFOLLOW_LINKS)){if(!Arrays.equals(expected,access.read(identity,192)))throw new IOException("Retained binding grant/window/context identity changed");}
            else {if(Files.exists(path,LinkOption.NOFOLLOW_LINKS) || Files.exists(path.resolveSibling(path.getFileName()+".lock"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used binding identity is missing");access.write(identity,expected,192);}
            try(var maintenance=RatisSessionMaintenance.frozenBinding(path,epochs,target.routed(),target.group(),target.directory(),access)){
            if(maintenance.pendingCommand()!=null){var pending=maintenance.pendingCommand().sessionControl();if(pending==null || !issued.grant().equals(pending.grant()) || pending.outstandingWindow()!=window)throw new IOException("Retained session binding differs from exact grant/window");}
            else if(maintenance.propagated()!=null){continue;}else maintenance.prepareBinding(issued.grant(),window);
            var result=maintenance.execute();if(result.status()!=CommandResult.Status.SUCCESS)throw new IOException("Native session binding rejected: "+result.status());
        }}
    }
    static RatisDurableSessionClient window(Path root,RatisRoutedClient routed,RatisSessionEpochs epochs,long lifetime,RatisSessionClient.Limits limits,List<Target> targets,RatisJournalAccess access)throws IOException{
        var mandatory=reservations(root,targets,epochs);access.preflight(mandatory);
        var issued=issueBound(root,epochs,lifetime,targets,access);Path file=root.resolve("window.bin");
        boolean exists=Files.exists(file,LinkOption.NOFOLLOW_LINKS);
        if(!exists && Files.exists(root.resolve("window.bin.lock"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used durable session window is missing");
        bind(root,epochs,issued,limits.outstanding(),targets,access);
        var client=exists?RatisDurableSessionClient.reopenOwned(file,routed,access):RatisDurableSessionClient.createOwned(file,routed,issued.issuance(),issued.grant(),limits,access);
        try{if(!client.grant().equals(issued.grant()) || !client.limits().equals(limits))throw new IOException("Durable window differs from frozen issuer/limits");access.fulfilled(mandatory);return client;}catch(IOException|RuntimeException failure){try{client.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    static List<RatisJournalAccess.Reservation> reservations(Path root,List<Target> targets,RatisSessionEpochs epochs)throws IOException {
        var paths=new ArrayList<>(bindingReservations(root,targets,epochs));paths.add(new RatisJournalAccess.Reservation(root.resolve("window.bin"),SessionWindowJournal.MAX_BYTES,2*(121+32)));paths.add(new RatisJournalAccess.Reservation(root.resolve("window.bin.lock"),32,0));return List.copyOf(paths);
    }
    static List<RatisJournalAccess.Reservation> bindingReservations(Path root,List<Target> targets,RatisSessionEpochs epochs)throws IOException {
        var ledger=epochs.read();if(ledger==null)throw new IOException("Native session catalogue is not initialized");return bindingReservations(root,targets,ledger);
    }
    private static List<RatisJournalAccess.Reservation> bindingReservations(Path root,List<Target> targets,SessionEpochLedger ledger)throws IOException {
        long ledgerBytes=72L+40L*Math.min(SessionEpochLedger.MAX_GRANTS,ledger.grants().size()+1);
        var paths=new ArrayList<RatisJournalAccess.Reservation>();
        paths.add(new RatisJournalAccess.Reservation(root.resolve("issuer.bin"),RatisGrantIssuer.MAX_BYTES,2*(2*ledgerBytes+4096)));
        paths.add(new RatisJournalAccess.Reservation(root.resolve("issuer.bin.lock"),32,0));
        for(var target:targets){Path file=root.resolve("binding-"+target.group());long geometry=RangeDirectoryCodec.encodedSize(target.directory())+2*ledgerBytes+16384;
            paths.add(new RatisJournalAccess.Reservation(file,RatisSessionMaintenance.MAX_JOURNAL,2*Math.min(RatisSessionMaintenance.MAX_JOURNAL,geometry)));
            paths.add(new RatisJournalAccess.Reservation(file.resolveSibling(file.getFileName()+".lock"),32,0));
            paths.add(new RatisJournalAccess.Reservation(file.resolveSibling(file.getFileName()+".identity"),192,2*(128+32)));
        }return List.copyOf(paths);
    }

}
