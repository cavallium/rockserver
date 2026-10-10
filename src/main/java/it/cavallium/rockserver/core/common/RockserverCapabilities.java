package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.Objects;

/** Effective backend features available through this connection's transport. */
public record RockserverCapabilities(BackendMode backendMode, RawScanMode rawScanMode,
		int maximumRangePageKeys, long maximumRangePageBytes, CdcMode cdcMode) {
	public enum CdcMode { UNSUPPORTED, SCALAR, VECTOR_SUBSCRIPTIONS }
	/** Both typed carriers must fit every legal projected batch before advertising vector support. */
	public static final int VECTOR_CDC_TRANSPORT_BYTES=32*1024*1024;
	public enum BackendMode { STANDALONE, DISTRIBUTED }
	public enum RawScanMode { UNSUPPORTED, PHYSICAL_SST, IMMUTABLE_EXPORT_JOBS }
	public static final RockserverCapabilities STANDALONE = new RockserverCapabilities(
			BackendMode.STANDALONE, RawScanMode.PHYSICAL_SST,
			RangeBudget.DEFAULT_MAX_ITEMS, RangeBudget.DEFAULT_MAX_BYTES,CdcMode.SCALAR);

	public RockserverCapabilities {
		Objects.requireNonNull(backendMode); Objects.requireNonNull(rawScanMode); Objects.requireNonNull(cdcMode);
		if(backendMode==BackendMode.DISTRIBUTED && cdcMode==CdcMode.SCALAR || backendMode==BackendMode.STANDALONE && cdcMode==CdcMode.VECTOR_SUBSCRIPTIONS) throw new IllegalArgumentException("CDC mode differs from backend");
		if (maximumRangePageKeys <= 0 || maximumRangePageBytes <= 0)
			throw new IllegalArgumentException("Positive bounded range page limits are required");
		if (backendMode == BackendMode.DISTRIBUTED && rawScanMode == RawScanMode.PHYSICAL_SST)
			throw new IllegalArgumentException("Distributed raw scans require immutable export jobs");
	}

	public RockserverCapabilities withoutRawScan() {
		return new RockserverCapabilities(backendMode, RawScanMode.UNSUPPORTED,
				maximumRangePageKeys, maximumRangePageBytes,cdcMode);
	}
	/** A page repeats its final key as the exclusive cursor; reserve its second copy and the envelope. */
	public RockserverCapabilities withTransportLimit(long maximumMessageBytes) {
		long available = (maximumMessageBytes - 64L * 1024) / 2;
		if (maximumMessageBytes <= 64L * 1024 || available < 1)
			throw new IllegalArgumentException("Transport cannot fit a bounded range page");
		return new RockserverCapabilities(backendMode, rawScanMode, maximumRangePageKeys,
				Math.min(maximumRangePageBytes, available),cdcMode==CdcMode.VECTOR_SUBSCRIPTIONS && maximumMessageBytes<VECTOR_CDC_TRANSPORT_BYTES ? CdcMode.UNSUPPORTED : cdcMode);
	}
    public int maximumVectorCdcRecords() { return cdcMode==CdcMode.VECTOR_SUBSCRIPTIONS ? it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions.MAX_RECORDS : 0; }
    public int maximumVectorCdcSourceBytes() { return cdcMode==CdcMode.VECTOR_SUBSCRIPTIONS ? it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions.MAX_POLL_BYTES : 0; }
    public int maximumVectorCdcBatchBytes() { return cdcMode==CdcMode.VECTOR_SUBSCRIPTIONS ? it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions.MAX_BATCH_BYTES : 0; }
	public void requireScalarCdc() {
		if(cdcMode!=CdcMode.SCALAR) throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,"Scalar CDC is unavailable through this connection: "+cdcMode);
	}
	public void requireVectorCdc() {
		if(cdcMode!=CdcMode.VECTOR_SUBSCRIPTIONS) throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,"Distributed CDC subscriptions are unavailable through this connection: "+cdcMode);
	}
	public void requirePhysicalSst() {
		if (rawScanMode != RawScanMode.PHYSICAL_SST)
			throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
					"Physical SST scans are unavailable through this connection: " + rawScanMode);
	}
	public void requireRangeBudget(RangeBudget budget) {
		if (budget.maxItems() > maximumRangePageKeys || budget.maxBytes() > maximumRangePageBytes)
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Range page budget exceeds the connected backend limits");
	}
}
