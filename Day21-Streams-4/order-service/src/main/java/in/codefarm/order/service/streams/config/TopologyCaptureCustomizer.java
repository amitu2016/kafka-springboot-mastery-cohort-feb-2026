package in.codefarm.order.service.streams.config;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;

public class TopologyCaptureCustomizer implements KafkaStreamsInfrastructureCustomizer {

	private final TopologyDescriptionHolder holder;

	public TopologyCaptureCustomizer(TopologyDescriptionHolder holder) {
		this.holder = holder;
	}

	@Override
	public void configureTopology(Topology topology) {
		holder.set(topology.describe());
	}
}
