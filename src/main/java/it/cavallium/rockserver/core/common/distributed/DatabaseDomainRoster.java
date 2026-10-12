package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Replicated add-only DATA-domain identities. Advertising a domain does not prove its directory or owners are ready. */
public record DatabaseDomainRoster(UUID clusterId,UUID databaseId,long generation,List<UUID> domainIds) {
    public static final int MAX_DOMAINS=64,HEADER_BYTES=44,MAX_BYTES=HEADER_BYTES+16*MAX_DOMAINS;
    public DatabaseDomainRoster {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainIds);
        if(generation<0 || domainIds.isEmpty() || domainIds.size()>MAX_DOMAINS) throw new IllegalArgumentException("Invalid bounded database domain roster");
        var unique=new TreeSet<UUID>(RangeDirectoryCodec.UUID_ORDER); for(var domain:domainIds) if(!unique.add(Objects.requireNonNull(domain))) throw new IllegalArgumentException("Duplicate database domain identity"); domainIds=List.copyOf(unique);
    }
    public static DatabaseDomainRoster initial(UUID cluster,UUID database,Collection<UUID> domains) { return new DatabaseDomainRoster(cluster,database,0,List.copyOf(domains)); }
    /** Domain identity is issued once. Retired and incomplete identities remain in the roster. */
    public DatabaseDomainRoster add(Collection<UUID> additions) {
        Objects.requireNonNull(additions); if(additions.isEmpty() || generation==Long.MAX_VALUE) throw new IllegalArgumentException("Roster growth requires new identities and available generation");
        var next=new ArrayList<>(domainIds); for(var domain:additions) { if(next.contains(domain)) throw new IllegalArgumentException("Database domain identity is already issued"); next.add(Objects.requireNonNull(domain)); } return new DatabaseDomainRoster(clusterId,databaseId,generation+1,next);
    }
    public void requireSuccessorOf(DatabaseDomainRoster previous) {
        if(previous==null || !clusterId.equals(previous.clusterId) || !databaseId.equals(previous.databaseId) || previous.generation==Long.MAX_VALUE || generation!=previous.generation+1 || domainIds.size()<=previous.domainIds.size() || !domainIds.containsAll(previous.domainIds)) throw new IllegalArgumentException("Database roster must grow monotonically without identity reuse");
    }
    public byte[] encode() { var out=ByteBuffer.allocate(HEADER_BYTES+16*domainIds.size()); uuid(out,clusterId); uuid(out,databaseId); out.putLong(generation).putInt(domainIds.size()); domainIds.forEach(id->uuid(out,id)); return out.array(); }
    public static DatabaseDomainRoster decode(byte[] bytes) {
        if(bytes==null || bytes.length<HEADER_BYTES+16 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid database domain roster size"); var in=ByteBuffer.wrap(bytes); UUID cluster=uuid(in),database=uuid(in); long generation=in.getLong(); int count=in.getInt();
        if(count<1 || count>MAX_DOMAINS || in.remaining()!=16*count) throw new IllegalArgumentException("Invalid database domain roster count"); var domains=new ArrayList<UUID>(count); for(int i=0;i<count;i++) domains.add(uuid(in)); var result=new DatabaseDomainRoster(cluster,database,generation,domains); if(!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical database domain roster"); return result;
    }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
}
