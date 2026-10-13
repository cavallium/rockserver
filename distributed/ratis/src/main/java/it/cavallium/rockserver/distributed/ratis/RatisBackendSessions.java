package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** One immutable issued grant and its exclusive consumers. Unknown outcomes reopen exact issuer/binding/window state. */
final class RatisBackendSessions {
    record Target(RatisRoutedClient routed,UUID group,RangeDirectory directory){Target{Objects.requireNonNull(routed);Objects.requireNonNull(group);Objects.requireNonNull(directory);if(directory.ranges().stream().noneMatch(range->range.groupId().equals(group)&&range.topologyState()==RangeDescriptor.TopologyState.ACTIVE))throw new IllegalArgumentException("Binding target absent from exact frozen directory");}}
    private RatisBackendSessions(){}
    static RatisGrantIssuer.Issued issue(Path root,RatisSessionEpochs epochs,long lifetime)throws IOException{
        Files.createDirectories(root);Path file=root.resolve("issuer.bin");
        boolean exists=Files.exists(file,LinkOption.NOFOLLOW_LINKS);
        if(!exists && Files.exists(root.resolve("issuer.bin.lock"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used grant issuer state is missing");
        long expires=0;if(!exists){var ledger=epochs.read();if(ledger==null)throw new IOException("Native session catalogue is not initialized");try{expires=Math.addExact(ledger.logicalTimeMillis(),lifetime);}catch(ArithmeticException overflow){throw new IOException("Native session expiry overflows",overflow);}}
        try(var issuer=exists?RatisGrantIssuer.reopen(file,epochs):RatisGrantIssuer.create(file,epochs,expires)){
            for(int visits=0;visits<32;visits++){var phase=issuer.step();if(phase==RatisGrantIssuer.Phase.ISSUED)return issuer.issued();if(phase==RatisGrantIssuer.Phase.EXPIRED || phase==RatisGrantIssuer.Phase.EXHAUSTED)throw new IOException("Issued session allocation cannot advance");}
            throw new IOException("Grant allocation remains retained; reopen exact issuer");
        }
    }
    static void bind(Path root,RatisSessionEpochs epochs,RatisGrantIssuer.Issued issued,int window,List<Target> targets)throws IOException{
        for(var target:targets){Path path=root.resolve("binding-"+target.group());Path identity=path.resolveSibling(path.getFileName()+".identity");var grant=issued.grant();
            byte[] expected=java.nio.ByteBuffer.allocate(96).putInt(0x424e4447).putLong(grant.epoch()).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits()).putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).putInt(window).putLong(target.group().getMostSignificantBits()).putLong(target.group().getLeastSignificantBits()).put(target.routed().sessionIdentityDigest()).array();
            if(Files.exists(identity,LinkOption.NOFOLLOW_LINKS)){if(!Arrays.equals(expected,SessionJournalFiles.read(identity,128)))throw new IOException("Retained binding grant/window/context identity changed");}
            else {if(Files.exists(path,LinkOption.NOFOLLOW_LINKS) || Files.exists(path.resolveSibling(path.getFileName()+".lock"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used binding identity is missing");SessionJournalFiles.write(identity,expected,128);}
            try(var maintenance=RatisSessionMaintenance.frozenBinding(path,epochs,target.routed(),target.group(),target.directory())){
            if(maintenance.pendingCommand()!=null){var pending=maintenance.pendingCommand().sessionControl();if(pending==null || !issued.grant().equals(pending.grant()) || pending.outstandingWindow()!=window)throw new IOException("Retained session binding differs from exact grant/window");}
            else if(maintenance.propagated()!=null){continue;}else maintenance.prepareBinding(issued.grant(),window);
            var result=maintenance.execute();if(result.status()!=CommandResult.Status.SUCCESS)throw new IOException("Native session binding rejected: "+result.status());
        }}
    }
    static RatisDurableSessionClient window(Path root,RatisRoutedClient routed,RatisSessionEpochs epochs,long lifetime,RatisSessionClient.Limits limits,List<Target> targets)throws IOException{
        var issued=issue(root,epochs,lifetime);Path file=root.resolve("window.bin");
        boolean exists=Files.exists(file,LinkOption.NOFOLLOW_LINKS);
        if(!exists && Files.exists(root.resolve("window.bin.lock"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used durable session window is missing");
        bind(root,epochs,issued,limits.outstanding(),targets);
        var client=exists?RatisDurableSessionClient.reopen(file,routed):RatisDurableSessionClient.create(file,routed,issued.issuance(),issued.grant(),limits);
        if(!client.grant().equals(issued.grant()) || !client.limits().equals(limits)){client.close();throw new IOException("Durable window differs from frozen issuer/limits");}return client;
    }
}
