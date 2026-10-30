package it.cavallium.rockserver.distributed;

import java.util.List;

/** {@link PartitionTest} with pipelined updates on, so that its hot counters chain through the partitions. */
class PipelinedPartitionTest extends PartitionTest {

	@Override
	List<String> serverArgs() {
		return ProcessCluster.pipelined(super.serverArgs());
	}
}
