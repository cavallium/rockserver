package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisCatalogueValidationTest {
    final UUID cluster = UUID.randomUUID(), database = UUID.randomUUID(), domain = UUID.randomUUID(),
            group = UUID.randomUUID(), column = UUID.randomUUID(), data = UUID.randomUUID();
    final byte[] key = ByteBuffer.allocate(16).putLong(domain.getMostSignificantBits()).putLong(domain.getLeastSignificantBits()).array();
    final java.util.function.Consumer<DistributedCommand> validate = RatisCatalogue.validator(cluster, database, group, 1, new byte[32], column);
    RangeDirectory directory(UUID cluster, long generation) {
        var replica = new ReplicaDescriptor(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "node", "127.0.0.1:12345", ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(cluster, database, domain, 0, new byte[32], generation, Set.of(data), 0,
                List.of(new RangeDescriptor(UUID.randomUUID(), UUID.randomUUID(), 1, 0, null, null, RangeDescriptor.TopologyState.ACTIVE, List.of(replica))));
    }
    DistributedCommand command(byte[] expected, byte[] value) {
        return new DistributedCommand(group, 1, new byte[32], new OperationId(UUID.randomUUID(), 1, 1),
                List.of(new Mutation(Mutation.Kind.PUT, column, key, value)), List.of(new KeyPredicate(column, key, expected),
                new KeyPredicate(column,RatisSchemaAdministration.headKey(domain),null)));
    }
    @Test void acknowledgementOnlyDoesNotRequireOrWeakenDirectoryReplacement() {
        var operation = new OperationId(UUID.randomUUID(),1,2);
        var ack = new DistributedCommand(group,1,new byte[32],operation,List.of(),List.of(),1);
        assertDoesNotThrow(() -> validate.accept(ack));
        assertThrows(IllegalArgumentException.class,() -> validate.accept(new DistributedCommand(UUID.randomUUID(),1,new byte[32],
                operation,List.of(),List.of(),1)));
        var initial = RangeDirectoryCodec.encode(directory(cluster,0));
        var original = command(null,initial);
        var piggyback = new DistributedCommand(group,1,new byte[32],operation,original.mutations(),original.predicates(),1);
        assertDoesNotThrow(() -> validate.accept(piggyback));
        assertThrows(IllegalArgumentException.class,() -> validate.accept(new DistributedCommand(group,1,new byte[32],operation,
                original.mutations(),List.of(),1)));
    }

    @Test void creationAndSingleGenerationAdvanceAreAccepted() {
        byte[] initial = RangeDirectoryCodec.encode(directory(cluster, 0));
        assertDoesNotThrow(() -> validate.accept(command(null, initial)));
        assertDoesNotThrow(() -> validate.accept(command(initial, RangeDirectoryCodec.encode(directory(cluster, 1)))));
    }
    @Test void acceptedUserPlansCannotBypassCataloguePredicates() {
        var base=command(null,RangeDirectoryCodec.encode(directory(cluster,0)));
        var audit=new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,1),
                new TransactionScope(List.of(TransactionScope.Span.point(column,key),TransactionScope.Span.point(column,RatisSchemaAdministration.headKey(domain)))),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode());
        var accepted=DistributedCommand.acceptedTransaction(base,audit);
        assertThrows(IllegalArgumentException.class,() -> validate.accept(accepted));
        var epochs=RatisSessionEpochs.validator(cluster,database,group,1,new byte[32],column);
        assertThrows(IllegalArgumentException.class,() -> epochs.accept(accepted));
    }
    @Test void foreignIdentityGenerationSkipsAndSchemaChangesAreRejected() {
        byte[] initial = RangeDirectoryCodec.encode(directory(cluster, 0));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(command(null, RangeDirectoryCodec.encode(directory(UUID.randomUUID(), 0)))));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(command(null, RangeDirectoryCodec.encode(directory(cluster, 1)))));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(command(initial, RangeDirectoryCodec.encode(directory(cluster, 2)))));
        var next = directory(cluster, 1);
        var schemaChanged = new RangeDirectory(cluster, database, domain, 1, new byte[32], 1, Set.of(data), 0, next.ranges());
        assertThrows(IllegalArgumentException.class, () -> validate.accept(command(initial, RangeDirectoryCodec.encode(schemaChanged))));
        var atLimit = RangeDirectoryCodec.encode(directory(cluster, Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(command(atLimit, initial)));
    }
    @Test void unconditionalAndMisaddressedEnvelopesAreRejected() {
        byte[] initial = RangeDirectoryCodec.encode(directory(cluster, 0));
        var valid = command(null, initial);
        assertThrows(IllegalArgumentException.class, () -> validate.accept(new DistributedCommand(group, 1, new byte[32], valid.operationId(), valid.mutations())));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(new DistributedCommand(UUID.randomUUID(), 1, new byte[32], valid.operationId(), valid.mutations(), valid.predicates())));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(new DistributedCommand(group, 2, new byte[32], valid.operationId(), valid.mutations(), valid.predicates())));
        var mismatch = new KeyPredicate(column, new byte[16], null);
        assertThrows(IllegalArgumentException.class, () -> validate.accept(new DistributedCommand(group, 1, new byte[32], valid.operationId(), valid.mutations(), List.of(mismatch))));
        assertThrows(IllegalArgumentException.class, () -> validate.accept(command(null, new byte[10])));
    }
}
