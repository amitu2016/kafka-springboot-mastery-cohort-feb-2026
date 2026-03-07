package in.codefarm.order.service.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Day 1 demo topology: read → transform → write, with simple KStream and KTable.
 * Realistic order-service setup: orders → process → orders-processed; KTable for customer reference.
 */
public class OrderStreamsTopology {

	private static final Logger log = LoggerFactory.getLogger(OrderStreamsTopology.class);

	private final String inputTopic;
	private final String outputTopic;
	private final String referenceTopic;

	public OrderStreamsTopology(String inputTopic, String outputTopic, String referenceTopic) {
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
		this.referenceTopic = referenceTopic;
	}

	/**
	 * Builds the topology on the given StreamsBuilder.
	 * Returns the source KStream so Spring binds the pipeline lifecycle.
	 */
	public KStream<String, String> build(StreamsBuilder builder) {
		// --- KStream: orders → transform → orders-processed ---
		KStream<String, String> orders = builder.stream(
				inputTopic,
				Consumed.with(Serdes.String(), Serdes.String())
		);

		KStream<String, String> processed = ((KStream<String, String>) orders)
				.mapValues(value -> {
					if (value == null) return null;
					return "[processed] " + value.toUpperCase();
				})
				.peek((key, value) -> log.info("Order event processed: key={}, value={}", key, value));

		processed.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

		// --- KTable: customer reference (changelog, latest value per key) ---
		KTable<String, String> customerReference = builder.table(
				referenceTopic,
				Consumed.with(Serdes.String(), Serdes.String()),
				Materialized.as("customer-reference-store")
		);
		customerReference
				.toStream()
				.peek((k, v) -> log.info("Customer reference update: key={}, value={}", k, v));

		return orders;
	}
}
