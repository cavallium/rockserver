package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetColumnId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.OpenIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Subsequent;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RocksDBSyncAPIRequestHandler {

	default <R> R requestSync(RocksDBAPICommand<R> req) {
		throw new UnsupportedOperationException("Unsupported request type: " + req);
	}
}
