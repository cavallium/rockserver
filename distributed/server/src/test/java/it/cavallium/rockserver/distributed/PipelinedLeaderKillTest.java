package it.cavallium.rockserver.distributed;

import java.nio.file.Path;
import java.util.List;

/** {@link LeaderKillTest} with pipelined updates on, so that its hot counters chain through the kills. */
class PipelinedLeaderKillTest extends LeaderKillTest {

	@Override
	List<String> serverArgs(Path rockserverConfig) {
		return ProcessCluster.pipelined(super.serverArgs(rockserverConfig));
	}
}
