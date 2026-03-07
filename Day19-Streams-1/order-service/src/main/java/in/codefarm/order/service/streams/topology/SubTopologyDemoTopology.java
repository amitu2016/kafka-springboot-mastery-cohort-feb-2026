package in.codefarm.order.service.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demo topology that produces multiple sub-topologies to illustrate
 * how Kafka Streams decides where to split.
 *
 * <p>Expected sub-topologies:
 * <ul>
 *   <li><b>Sub-topology 0</b> — Pipeline A (stateless: filter → map → sink) +
 *       Pipeline B (groupByKey → count). Both share the same source ("orders")
 *       and groupByKey does NOT change the key, so no repartition → same sub-topology.
 *       Pipeline C's groupBy (re-key) also originates here but writes to an internal
 *       repartition topic (the SINK side of the repartition boundary).</li>
 *   <li><b>Sub-topology 1</b> — Pipeline C aggregation side. Reads from the internal
 *       repartition topic created by groupBy → count. The topic boundary forced the split.</li>
 *   <li><b>Sub-topology 2</b> — Pipeline D. Independent KTable from "customer-reference"
 *       with no processor edges to the orders pipelines → separate sub-topology.</li>
 * </ul>
 *
 * <p>Activate this topology by setting {@code app.kafka.topology=subtopology-demo}
 * in application.yml, then visit {@code /api/streams/topology/visual} to see the result.
 */
public class SubTopologyDemoTopology {

	private static final Logger log = LoggerFactory.getLogger(SubTopologyDemoTopology.class);

	private final String inputTopic;
	private final String outputTopic;
	private final String referenceTopic;

	public SubTopologyDemoTopology(String inputTopic, String outputTopic, String referenceTopic) {
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
		this.referenceTopic = referenceTopic;
	}

	public KStream<String, String> build(StreamsBuilder builder) {

		KStream<String, String> orders = builder.stream(
				inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

		// ── Pipeline A: Stateless — filter → transform → sink ──────────────
		// All processors connected by direct edges → stays in sub-topology 0.
		orders
				.filter((key, value) -> value != null && !value.isBlank())
				.mapValues(value -> "[processed] " + value.toUpperCase())
				.peek((key, value) -> log.info("Pipeline A (stateless): key={}", key))
				.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

		// ── Pipeline B: groupByKey → count ─────────────────────────────────
		// Key is NOT changed → no repartition needed → stays in sub-topology 0.
		orders
				.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-count-by-key")
						.withKeySerde(Serdes.String())
						.withValueSerde(Serdes.Long()))
				.toStream()
				.peek((key, value) -> log.info("Pipeline B (groupByKey): key={}, count={}", key, value))
				.to("order-key-counts", Produced.with(Serdes.String(), Serdes.Long()));

		// ── Pipeline C: groupBy(new key) → count ───────────────────────────
		// Key IS changed → internal repartition topic created → NEW sub-topology.
		orders
				.groupBy((key, value) -> extractCategory(value),
						Grouped.with(Serdes.String(), Serdes.String()))
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-count-by-category")
						.withKeySerde(Serdes.String())
						.withValueSerde(Serdes.Long()))
				.toStream()
				.peek((key, value) -> log.info("Pipeline C (groupBy new key): category={}, count={}", key, value))
				.to("order-category-counts", Produced.with(Serdes.String(), Serdes.Long()));

		// ── Pipeline D: Independent KTable ─────────────────────────────────
		// No processor edges connecting to orders pipelines → separate sub-topology.
		KTable<String, String> customerReference = builder.table(
				referenceTopic,
				Consumed.with(Serdes.String(), Serdes.String()),
				Materialized.as("customer-reference-store"));
		customerReference.toStream()
				.peek((k, v) -> log.info("Pipeline D (independent KTable): key={}", k));

		return orders;
	}

	private static String extractCategory(String value) {
		if (value == null) return "unknown";
		String lower = value.toLowerCase();
		if (lower.contains("electronics")) return "electronics";
		if (lower.contains("clothing")) return "clothing";
		if (lower.contains("food")) return "food";
		if (lower.contains("books")) return "books";
		return "general";
	}
}
