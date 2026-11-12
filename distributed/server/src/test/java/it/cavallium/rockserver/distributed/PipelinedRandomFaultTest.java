package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import java.util.List;

/** {@link RandomFaultTest} with pipelined updates on, so that its hot counters chain through the faults. */
@Tag("slow")
class PipelinedRandomFaultTest extends RandomFaultTest {

	@Override
	List<String> serverArgs() {
		return ProcessCluster.pipelined(super.serverArgs());
	}
}
