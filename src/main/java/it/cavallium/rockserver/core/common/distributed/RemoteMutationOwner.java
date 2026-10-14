package it.cavallium.rockserver.core.common.distributed;

import java.util.UUID;
import java.util.Arrays;
import java.util.Objects;

/** Discovery of an installed durable owner before preparing caller-selected request IDs. */
public record RemoteMutationOwner(UUID clusterId,UUID databaseId,UUID domainId,UUID ownerId,long ownerEpoch,long nextRequestSequence,RemoteMutationToken.Kind kind,byte[] schemaDigest,byte[] directoryDigest) {
    public RemoteMutationOwner { Objects.requireNonNull(clusterId);Objects.requireNonNull(databaseId);Objects.requireNonNull(domainId);Objects.requireNonNull(ownerId);Objects.requireNonNull(kind);if(nextRequestSequence<0 || ownerEpoch<0 || schemaDigest.length!=32 || directoryDigest.length!=32)throw new IllegalArgumentException("Invalid owner authority");schemaDigest=schemaDigest.clone();directoryDigest=directoryDigest.clone(); }
    @Override public byte[] schemaDigest(){return schemaDigest.clone();}
    @Override public byte[] directoryDigest(){return directoryDigest.clone();}
    public boolean sameAuthority(RemoteMutationOwner owner) {return owner!=null && clusterId.equals(owner.clusterId) && databaseId.equals(owner.databaseId) && domainId.equals(owner.domainId) && ownerId.equals(owner.ownerId) && ownerEpoch==owner.ownerEpoch && kind==owner.kind && Arrays.equals(schemaDigest,owner.schemaDigest) && Arrays.equals(directoryDigest,owner.directoryDigest);}
    @Override public boolean equals(Object other){return other instanceof RemoteMutationOwner owner && nextRequestSequence==owner.nextRequestSequence && sameAuthority(owner);}
    @Override public int hashCode(){return Objects.hash(clusterId,databaseId,domainId,ownerId,ownerEpoch,nextRequestSequence,kind,Arrays.hashCode(schemaDigest),Arrays.hashCode(directoryDigest));}
}
