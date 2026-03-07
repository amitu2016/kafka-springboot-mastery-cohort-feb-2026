package in.codefarm.order.service.streams.config;

import org.apache.kafka.streams.TopologyDescription;

import java.util.concurrent.atomic.AtomicReference;

public class TopologyDescriptionHolder {

	private final AtomicReference<TopologyDescription> description = new AtomicReference<>();

	public void set(TopologyDescription desc) {
		description.set(desc);
	}

	public TopologyDescription get() {
		return description.get();
	}
}
