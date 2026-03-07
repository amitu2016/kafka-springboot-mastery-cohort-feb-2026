package in.codefarm.order.service.streams.topology;

import in.codefarm.order.service.api.dto.OrderEvent;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

/**
 * Day 4 demo: stateful operations — running order total per customer (aggregate), order count per category (count).
 * Reads from orders, parses OrderEvent, then two pipelines: groupBy customerId → aggregate(total), groupBy categoryId → count().
 */
public class Day4StatefulTopology {

	private static final Logger log = LoggerFactory.getLogger(Day4StatefulTopology.class);

	private final String inputTopic;
	private final String customerTotalsTopic;
	private final String ordersPerCategoryTopic;
	private final ObjectMapper objectMapper;

	public Day4StatefulTopology(String inputTopic, String customerTotalsTopic, String ordersPerCategoryTopic,
			ObjectMapper objectMapper) {
		this.inputTopic = inputTopic;
		this.customerTotalsTopic = customerTotalsTopic;
		this.ordersPerCategoryTopic = ordersPerCategoryTopic;
		this.objectMapper = objectMapper;
	}

	public KStream<String, String> build(StreamsBuilder builder) {
		KStream<String, String> source = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

		KStream<String, String> valid = source
				.filter((key, value) -> value != null && !value.isBlank())
				.mapValues(value -> {
					try {
						OrderEvent e = objectMapper.readValue(value, OrderEvent.class);
						return e != null && e.isValid() ? value : null;
					} catch (Exception ex) {
						log.info("Dropping malformed order: {}", ex.getMessage());
						return null;
					}
				})
				.filter((key, value) -> value != null);

		// Pipeline 1: running total per customer (groupBy customerId → aggregate)
		valid
				.groupBy(
						(key, value) -> parseOrder(value).customerId(),
						Grouped.with(Serdes.String(), Serdes.String()))
				.aggregate(
						() -> Double.valueOf(0.0),
						(key, value, total) -> {
							OrderEvent o = parseOrder(value);
							return total + (o != null && o.totalAmount() != null ? o.totalAmount() : 0.0);
						},
						Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("customer-totals-store")
								.withValueSerde(Serdes.Double()))
				.toStream()
				.peek((k, v) -> log.info("Customer total: {} -> {}", k, v))
				.to(customerTotalsTopic, Produced.with(Serdes.String(), Serdes.Double()));

		// Pipeline 2: count per category (groupBy categoryId → count)
		valid
				.groupBy(
						(key, value) -> parseOrder(value).effectiveCategoryId(),
						Grouped.with(Serdes.String(), Serdes.String()))
				.count(Materialized.as("orders-per-category-store"))
				.toStream()
				.peek((k, v) -> log.info("Category count: {} -> {}", k, v))
				.to(ordersPerCategoryTopic, Produced.with(Serdes.String(), Serdes.Long()));

		return source;
	}

	private OrderEvent parseOrder(String value) {
		try {
			return objectMapper.readValue(value, OrderEvent.class);
		} catch (Exception e) {
			return null;
		}
	}
}
