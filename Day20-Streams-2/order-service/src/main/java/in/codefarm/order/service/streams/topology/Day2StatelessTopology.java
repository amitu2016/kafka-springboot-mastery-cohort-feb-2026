package in.codefarm.order.service.streams.topology;

import tools.jackson.databind.ObjectMapper;
import in.codefarm.order.service.api.dto.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Day 2 demo: stateless transformations — mapValues (parse), filter, selectKey, branch(), to topics.
 * Handles malformed data by dropping invalid/missing fields; branches by high-value vs normal.
 */
public class Day2StatelessTopology {

	private static final Logger log = LoggerFactory.getLogger(Day2StatelessTopology.class);
	private static final double HIGH_VALUE_THRESHOLD = 1000.0;

	private final String inputTopic;
	private final String highValueTopic;
	private final String normalTopic;
	private final ObjectMapper objectMapper;

	public Day2StatelessTopology(String inputTopic, String highValueTopic, String normalTopic, ObjectMapper objectMapper) {
		this.inputTopic = inputTopic;
		this.highValueTopic = highValueTopic;
		this.normalTopic = normalTopic;
		this.objectMapper = objectMapper;
	}

	public KStream<String, String> build(StreamsBuilder builder) {
		KStream<String, String> source = builder.stream(
				inputTopic,
				Consumed.with(Serdes.String(), Serdes.String())
		);

		// Filter: drop null or empty
		KStream<String, String> nonEmpty = source.filter((key, value) -> value != null && !value.isBlank());

		// mapValues: parse JSON; return null to drop malformed (handles malformed data)
		KStream<String, String> parsed = nonEmpty.mapValues(value -> {
			try {
				OrderEvent event = objectMapper.readValue(value, OrderEvent.class);
				return event != null ? value : null;
			} catch (Exception e) {
				log.info("Dropping malformed order event: {}", e.getMessage());
				return null;
			}
		});

		// Filter: drop null (parse failed or null event)
		KStream<String, String> valid = parsed.filter((key, value) -> value != null && parseOrderEvent(value) != null && parseOrderEvent(value).isValid());

		// selectKey: re-key by customerId for downstream (e.g. future joins)
		KStream<String, String> byCustomer = valid.selectKey((key, value) -> {
			OrderEvent o = parseOrderEvent(value);
			return o != null ? o.customerId() : key;
		});

		// peek: side effect (logging)
		byCustomer.peek((key, value) -> log.info("Valid order, key={}: {}", key, value));

		// split/branch: high-value vs normal (first match wins; defaultBranch catches rest)
		// Use withConsumer so we don't depend on Map keys (which can vary by Kafka Streams version)
		Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
		byCustomer
				.split()
				.branch((key, value) -> isHighValue(value, objectMapper), Branched.withConsumer(ks -> ks.to(highValueTopic, produced)))
				.defaultBranch(Branched.withConsumer(ks -> ks.to(normalTopic, produced)));

		return source;
	}

	private OrderEvent parseOrderEvent(String value) {
		try {
			return objectMapper.readValue(value, OrderEvent.class);
		} catch (Exception e) {
			return null;
		}
	}

	private static boolean isHighValue(String value, ObjectMapper mapper) {
		try {
			OrderEvent o = mapper.readValue(value, OrderEvent.class);
			return o != null && o.isHighValue(HIGH_VALUE_THRESHOLD);
		} catch (Exception e) {
			return false;
		}
	}
}
