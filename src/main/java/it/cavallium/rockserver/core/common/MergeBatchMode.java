package it.cavallium.rockserver.core.common;

public enum MergeBatchMode {
	MERGE_WRITE_BATCH,
	MERGE_WRITE_BATCH_NO_WAL,
	MERGE_SST_INGESTION,
	MERGE_SST_INGEST_BEHIND
}