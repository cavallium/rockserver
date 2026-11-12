package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import java.util.List;

/** {@link PartitionTest} with pipelined updates on, so that its hot counters chain through the partitions. */
@Tag("slow")
class PipelinedPartitionTest extends PartitionTest {

	@Override
	List<String> serverArgs() {
		return ProcessCluster.pipelined(super.serverArgs());
	}
}
