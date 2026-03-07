package in.codefarm.order.service.streams.topology;

//import tools.jackson.databind.ObjectMapper;
import in.codefarm.order.service.api.dto.Customer;
import in.codefarm.order.service.api.dto.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Day 5 demo: stream-table join — orders (stream) + customers (table) → enriched-orders.
 * Re-keys orders by customerId, joins with customers KTable, produces JSON with order + customer fields.
 */
public class Day5EnrichmentTopology {

	private static final Logger log = LoggerFactory.getLogger(Day5EnrichmentTopology.class);

	private final String ordersTopic;
	private final String customersTopic;
	private final String enrichedOrdersTopic;
	private final ObjectMapper objectMapper;

	public Day5EnrichmentTopology(String ordersTopic, String customersTopic, String enrichedOrdersTopic,
			ObjectMapper objectMapper) {
		this.ordersTopic = ordersTopic;
		this.customersTopic = customersTopic;
		this.enrichedOrdersTopic = enrichedOrdersTopic;
		this.objectMapper = objectMapper;
	}

	public KStream<String, String> build(StreamsBuilder builder) {
		KStream<String, String> orders = builder.stream(ordersTopic, Consumed.with(Serdes.String(), Serdes.String()));
		KTable<String, String> customers = builder.table(customersTopic, Consumed.with(Serdes.String(), Serdes.String()));

		KStream<String, String> validOrders = orders
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

		// Re-key by customerId for co-partitioning with customers table
		validOrders
				.selectKey((key, value) -> parseOrder(value).customerId())
				.join(
						customers,
						(orderJson, customerJson) -> toEnrichedJson(orderJson, customerJson),
						Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
				.peek((customerId, enriched) -> log.info("Enriched order for customer {}: {}", customerId, enriched))
				.to(enrichedOrdersTopic, Produced.with(Serdes.String(), Serdes.String()));

		return orders;
	}

	private OrderEvent parseOrder(String value) {
		try {
			return objectMapper.readValue(value, OrderEvent.class);
		} catch (Exception e) {
			return null;
		}
	}

	private Customer parseCustomer(String value) {
		if (value == null || value.isBlank()) return Customer.unknown();
		try {
			Customer c = objectMapper.readValue(value, Customer.class);
			return c != null ? c : Customer.unknown();
		} catch (Exception e) {
			return Customer.unknown();
		}
	}

	private String toEnrichedJson(String orderJson, String customerJson) {
		OrderEvent order = parseOrder(orderJson);
		Customer customer = parseCustomer(customerJson);
		try {
			return objectMapper.writeValueAsString(Map.of(
					"orderId", order != null && order.orderId() != null ? order.orderId() : "",
					"customerId", order != null && order.customerId() != null ? order.customerId() : "",
					"totalAmount", order != null && order.totalAmount() != null ? order.totalAmount() : 0.0,
					"categoryId", order != null ? order.effectiveCategoryId() : "default",
					"customerName", customer.name() != null ? customer.name() : "unknown",
					"customerTier", customer.tier() != null ? customer.tier() : "-"));
		} catch (Exception e) {
			return "{\"customerName\":\"" + customer.name() + "\",\"customerTier\":\"" + customer.tier() + "\"}";
		}
	}
}
