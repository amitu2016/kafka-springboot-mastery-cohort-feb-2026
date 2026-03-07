package in.codefarm.order.service.streams.config;

import org.apache.kafka.streams.TopologyDescription;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the topology description once built (via KafkaStreamsInfrastructureCustomizer)
 * for visualization endpoints. KafkaStreams does not expose getTopology() in all versions.
 */
public class TopologyDescriptionHolder {

	private final AtomicReference<TopologyDescription> description = new AtomicReference<>();

	public void set(TopologyDescription desc) {
		description.set(desc);
	}

	public TopologyDescription get() {
		return description.get();
	}
}
