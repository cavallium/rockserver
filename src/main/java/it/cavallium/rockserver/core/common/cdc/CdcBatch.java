package it.cavallium.rockserver.core.common.cdc;

import java.util.List;
import org.jetbrains.annotations.NotNull;

public record CdcBatch(@NotNull List<CDCEvent> events, long nextSeq) {
}
