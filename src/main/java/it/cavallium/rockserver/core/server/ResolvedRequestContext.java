package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.*;


/** A request context and its deadline on this server's monotonic clock. */
record ResolvedRequestContext(
		it.cavallium.rockserver.core.common.RequestContext value,
		long localMonotonicDeadlineNanos) {

	WorkloadProfile profile() {
		return value.profile();
	}

}
