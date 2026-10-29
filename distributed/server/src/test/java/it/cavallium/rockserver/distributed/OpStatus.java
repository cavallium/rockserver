package it.cavallium.rockserver.distributed;

/** What a client knows about the effect of one operation it sent to the cluster. */
public enum OpStatus {
	/** Acknowledged: the operation took effect once, between its invocation and its completion. */
	OK,
	/** The operation certainly had no effect. */
	FAILED,
	/** The operation may or may not take effect, at any time after its invocation. */
	UNKNOWN
}
