package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Borrowed authoritative roster reader. A complete view is rechecked against concurrent domain creation. */
public final class RatisDatabaseDomainRoster {
    private static final byte TAG=(byte)0x87;
    public record View(DatabaseDomainRoster roster,Map<UUID,RangeDirectory> directories) { public View { Objects.requireNonNull(roster); directories=Map.copyOf(directories); if(!directories.keySet().equals(Set.copyOf(roster.domainIds()))) throw new IllegalArgumentException("Incomplete database roster view"); for(var entry:directories.entrySet()) { var directory=entry.getValue(); if(!entry.getKey().equals(directory.domainId()) || !roster.clusterId().equals(directory.clusterId()) || !roster.databaseId().equals(directory.databaseId())) throw new IllegalArgumentException("Foreign database roster directory view"); } } }
    private final RaftClient client; private final UUID cluster,database,group,column; private final RatisCatalogue catalogue;
    public RatisDatabaseDomainRoster(RaftClient client,UUID cluster,UUID database,RatisNodePlan.Catalogue binding) { this(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()); }
    public RatisDatabaseDomainRoster(RaftClient client,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) { this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column); catalogue=new RatisCatalogue(client,cluster,database,group,epoch,schema,column); }
    public DatabaseDomainRoster read() throws IOException {
        var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.pointRequest(column,key(database))))));
        if(reply==null || !reply.isSuccess() || !org.apache.ratis.protocol.RaftGroupId.valueOf(group).equals(reply.getRaftGroupId()) || reply.getMessage()==null || reply.getMessage().getContent().size()>ScopedTransactionRpc.HEADER_BYTES+4+DatabaseDomainRoster.MAX_BYTES) throw new IOException("Database roster read failed/exceeded bound");
        try { byte[] bytes=ScopedTransactionRpc.pointValue(reply.getMessage().getContent().toByteArray(),DatabaseDomainRoster.MAX_BYTES); if(bytes==null) return null; var roster=DatabaseDomainRoster.decode(bytes); requireNamespace(roster,cluster,database); return roster; } catch(IllegalArgumentException invalid) { throw new IOException("Invalid authoritative database roster",invalid); }
    }
    public View readComplete() throws IOException {
        var before=read(); if(before==null) throw new IOException("Database roster has not been published"); var directories=new LinkedHashMap<UUID,RangeDirectory>();
        for(var domain:before.domainIds()) { var directory=RatisRoutedClient.rpc(()->catalogue.read(domain)); if(directory==null) throw new IOException("Advertised database domain directory is incomplete: "+domain); directories.put(domain,directory); }
        requireUnchanged(before); return new View(before,directories);
    }
    public void requireUnchanged(DatabaseDomainRoster expected) throws IOException { if(!Objects.requireNonNull(expected).equals(read())) throw new IOException("Authoritative database domain roster changed during observation"); }
    static void requireNamespace(DatabaseDomainRoster roster,UUID cluster,UUID database) { if(!roster.clusterId().equals(cluster) || !roster.databaseId().equals(database)) throw new IllegalArgumentException("Foreign database roster namespace"); }
    public static byte[] key(UUID database) { return ByteBuffer.allocate(17).put(TAG).putLong(database.getMostSignificantBits()).putLong(database.getLeastSignificantBits()).array(); }
    static void requirePublication(KeyPredicate guard,Mutation mutation,RangeDirectory directory) {
        if(guard.bucket() || !Arrays.equals(guard.key(),key(directory.databaseId()))) throw new IllegalArgumentException("Invalid database roster guard");
        var previous=guard.expectedValue()==null ? null : DatabaseDomainRoster.decode(guard.expectedValue()); if(previous!=null) requireNamespace(previous,directory.clusterId(),directory.databaseId());
        var next=mutation==null ? previous : DatabaseDomainRoster.decode(mutation.value()); if(next==null) throw new IllegalArgumentException("Initial directory lacks an authoritative database roster"); requireNamespace(next,directory.clusterId(),directory.databaseId());
        if(!next.domainIds().contains(directory.domainId())) throw new IllegalArgumentException("Published directory is not in its database roster");
        if(mutation!=null) { if(mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(guard.columnId()) || !Arrays.equals(mutation.key(),guard.key())) throw new IllegalArgumentException("Invalid database roster mutation"); if(previous==null) { if(next.generation()!=0) throw new IllegalArgumentException("Initial roster generation must be zero"); } else next.requireSuccessorOf(previous); }
    }
}
