package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** Discovery of an installed durable owner and its complete current domain authority. */
public record RemoteMutationOwner(UUID clusterId, UUID databaseId, UUID ownerId, long ownerEpoch,
        long nextRequestSequence, RemoteMutationToken.Kind kind, List<DomainBinding> domains) {
    public RemoteMutationOwner {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(ownerId); Objects.requireNonNull(kind);
        if (nextRequestSequence < 0 || ownerEpoch < 0) throw new IllegalArgumentException("Invalid owner authority");
        domains = DomainBinding.canonical(domains);
        if (kind != RemoteMutationToken.Kind.ATOMIC && domains.size() != 1) throw new IllegalArgumentException("Single-domain owner requires one binding");
    }
    public boolean sameAuthority(RemoteMutationOwner owner) {
        return owner != null && clusterId.equals(owner.clusterId) && databaseId.equals(owner.databaseId)
                && ownerId.equals(owner.ownerId) && ownerEpoch == owner.ownerEpoch && kind == owner.kind && domains.equals(owner.domains);
    }
}
