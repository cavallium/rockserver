package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import java.io.IOException;
import java.util.*;

/** Owns control journals/adapters only. Borrowed routing clients, homes, scheduler and native actors stay open. */
public final class RatisDatabaseBackupOperation implements AutoCloseable {
    private final RatisDatabaseBackupDriver driver;
    private final RatisDatabaseBackupJournal journal;
    private final List<AutoCloseable> owners;
    private final Runnable refund;
    private boolean active,closed;
    private RatisDatabaseBackupDriver.Progress progress;
    RatisDatabaseBackupOperation(RatisDatabaseBackupDriver driver,RatisDatabaseBackupJournal journal,List<AutoCloseable> owners,Runnable refund){this.driver=driver;this.journal=journal;this.owners=List.copyOf(owners);this.refund=refund;}
    public RatisDatabaseBackupDriver.Progress step(RequestContext context)throws IOException{
        synchronized(this){if(closed || active)throw new IOException("Backup operation closed or actual work active");active=true;}
        try{var next=driver.step(context);synchronized(this){progress=next;}return next;}finally{synchronized(this){active=false;}}
    }
    public void cancel(){driver.cancel();}
    public synchronized RatisDatabaseBackupJournal.Snapshot snapshot()throws IOException{if(closed || active)throw new IOException("Backup operation closed or actual work active");return journal.snapshot();}
    /** Cancellation/loss is settled only after repeated steps deliver every native cleanup proof. */
    @Override public void close()throws IOException{
        synchronized(this){if(closed)return;if(active || progress==null || progress.cleanupPending() || !progress.complete() && progress.phase()!=RatisDatabaseBackupJournal.Phase.CANCELLED && progress.phase()!=RatisDatabaseBackupJournal.Phase.LOST)throw new IOException("Backup cleanup remains owned; cancel/step until actual settlement");active=true;}
        IOException failure=null;
        try{driver.close();for(int i=owners.size()-1;i>=0;i--)try{owners.get(i).close();}catch(Exception problem){if(failure==null)failure=new IOException("Backup owned resource close failed",problem);else failure.addSuppressed(problem);}if(failure!=null)throw failure;synchronized(this){closed=true;}refund.run();}
        finally{synchronized(this){active=false;}}
    }
}
