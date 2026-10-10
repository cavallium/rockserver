package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/** Borrowed client and native reader. Changed bounds require fresh DATA cursor witnesses. */
final class RatisCdcDeliveryAuthority {
    @FunctionalInterface interface DirectoryReader { RangeDirectory read(UUID domain) throws IOException; }
    private final RatisRoutedClient client;
    private final DirectoryReader directories;
    RatisCdcDeliveryAuthority(RatisRoutedClient client,DirectoryReader directories) { this.client=Objects.requireNonNull(client); this.directories=Objects.requireNonNull(directories); }
    void validate(DistributedCommand command) throws IOException {
        if(!RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command)) throw new IOException("Expected new CDC delivery bounds");
        var next=DistributedCdcSubscription.decode(command.mutations().getFirst().value());
        var old=DistributedCdcSubscription.decode(command.predicates().getFirst().expectedValue()); next.requireSuccessorOf(old);
        var definition=next.definition(); var directory=directories.read(definition.domainId());
        if(directory==null || !directory.clusterId().equals(definition.clusterId()) || !directory.databaseId().equals(definition.databaseId()) || !directory.domainId().equals(definition.domainId())) throw new IOException("CDC delivery directory is absent/foreign");
        for(var entry:next.delivered().cursors().entrySet()) {
            if(entry.getValue().equals(old.delivered().cursors().get(entry.getKey()))) continue;
            var result=client.pollCdcDirectory(directory,entry.getKey(),entry.getValue(),1,RatisCdcPollRpc.REPLY_BASE_BYTES+1);
            if(result.outcome()==RatisCdcPollRpc.Outcome.GAP || !result.fromVerified()) throw new IOException("CDC delivery cursor has no retained/current DATA witness");
            if(!old.delivered().cursors().get(entry.getKey()).covers(result.floor())) throw new IOException("CDC delivery crosses an undelivered retention gap");
        }
    }
}
