package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;

/** Deterministic aggregate return overflow before any accepted transaction is published. */
final class RatisStagedResultTooLargeException extends IOException {
    RatisStagedResultTooLargeException(){super("Aggregate staged result exceeds transaction result budget; original prefix remains owned");}
}
