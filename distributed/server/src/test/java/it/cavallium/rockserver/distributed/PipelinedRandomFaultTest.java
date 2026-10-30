package it.cavallium.rockserver.distributed;

import java.util.List;

/** {@link RandomFaultTest} with pipelined updates on, so that its hot counters chain through the faults. */
class PipelinedRandomFaultTest extends RandomFaultTest {

	@Override
	List<String> serverArgs() {
		return ProcessCluster.pipelined(super.serverArgs());
	}
}
