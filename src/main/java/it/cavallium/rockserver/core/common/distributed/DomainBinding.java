package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Immutable placement and schema authority for one domain of a global snapshot/transaction. */
public record DomainBinding(UUID domainId,long directoryGeneration,byte[] directoryDigest,long schemaEpoch,byte[] schemaDigest) {
    public static final int BYTES=96,MAX_DOMAINS=64;
    public DomainBinding {
        Objects.requireNonNull(domainId);
        if(directoryGeneration<0 || schemaEpoch<0 || directoryDigest.length!=32 || schemaDigest.length!=32)throw new IllegalArgumentException("Invalid domain binding");
        directoryDigest=directoryDigest.clone();schemaDigest=schemaDigest.clone();
    }
    public static DomainBinding of(RangeDirectory directory){return new DomainBinding(directory.domainId(),directory.generation(),directory.payloadDigest(),directory.schemaEpoch(),directory.schemaDigest());}
    @Override public byte[] directoryDigest(){return directoryDigest.clone();}
    @Override public byte[] schemaDigest(){return schemaDigest.clone();}
    public void require(RangeDirectory directory){if(!equals(of(directory)))throw new IllegalArgumentException("Domain directory/schema authority changed");}
    @Override public boolean equals(Object other){return other instanceof DomainBinding b && domainId.equals(b.domainId) && directoryGeneration==b.directoryGeneration && schemaEpoch==b.schemaEpoch && Arrays.equals(directoryDigest,b.directoryDigest) && Arrays.equals(schemaDigest,b.schemaDigest);}
    @Override public int hashCode(){return Objects.hash(domainId,directoryGeneration,Arrays.hashCode(directoryDigest),schemaEpoch,Arrays.hashCode(schemaDigest));}
    public static List<DomainBinding> canonical(List<DomainBinding> domains){
        Objects.requireNonNull(domains);if(domains.isEmpty() || domains.size()>MAX_DOMAINS)throw new IllegalArgumentException("Domain binding count exceeds bound");
        var ordered=domains.stream().sorted(Comparator.comparing(DomainBinding::domainId,SnapshotFence.UUID_ORDER)).toList();
        for(int n=1;n<ordered.size();n++)if(ordered.get(n-1).domainId.equals(ordered.get(n).domainId))throw new IllegalArgumentException("Duplicate domain binding");
        return ordered;
    }
    public static DomainBinding require(List<DomainBinding> domains,UUID id){return domains.stream().filter(domain->domain.domainId.equals(id)).findFirst().orElseThrow(()->new IllegalArgumentException("Unregistered participant domain"));}
    static void write(ByteBuffer b,DomainBinding domain){SnapshotFence.uuid(b,domain.domainId);b.putLong(domain.directoryGeneration).put(domain.directoryDigest).putLong(domain.schemaEpoch).put(domain.schemaDigest);}
    static DomainBinding read(ByteBuffer b){SnapshotFence.require(b,BYTES);var id=SnapshotFence.uuid(b);long generation=b.getLong();var directory=new byte[32];b.get(directory);long epoch=b.getLong();var schema=new byte[32];b.get(schema);return new DomainBinding(id,generation,directory,epoch,schema);}
}
