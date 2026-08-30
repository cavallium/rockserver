package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

class SchedulerStateModelFuzzTest {

	@FuzzTest(maxDuration = "20s")
	void arbitraryByteActionsPreserveOwnershipAndTerminalConservation(FuzzedDataProvider data) {
		SchedulerStateTraceRunner.run(data.consumeRemainingAsBytes());
	}
}
