package in.codefarm.order.service.dashboard;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes from enriched-orders to update the dashboard with processed results (order + customer).
 */
@Component
public class DashboardConsumer {

	private static final Logger log = LoggerFactory.getLogger(DashboardConsumer.class);

	private final DashboardEventService dashboardEventService;
	private final ObjectMapper objectMapper;

	public DashboardConsumer(DashboardEventService dashboardEventService, ObjectMapper objectMapper) {
		this.dashboardEventService = dashboardEventService;
		this.objectMapper = objectMapper;
	}

	@KafkaListener(
			topics = "${app.kafka.enriched-orders-topic:enriched-orders}",
			containerFactory = "enrichedOrdersListenerContainerFactory")
	public void onEnrichedOrder(ConsumerRecord<String, String> record) {
		String value = record.value();
		if (value == null || value.isBlank()) return;
		try {
			JsonNode node = objectMapper.readTree(value);
			String customerId = node.has("customerId") ? node.get("customerId").asText("") : "";
			String orderId = node.has("orderId") ? node.get("orderId").asText("") : "";
			double totalAmount = node.has("totalAmount") ? node.get("totalAmount").asDouble(0) : 0;
			String customerName = node.has("customerName") ? node.get("customerName").asText("unknown") : "unknown";
			String customerTier = node.has("customerTier") ? node.get("customerTier").asText("-") : "-";
			log.debug("Dashboard: enriched order customerId={} orderId={}", customerId, orderId);
			dashboardEventService.recordEnrichedOrder(customerId, orderId, totalAmount, customerName, customerTier);
		} catch (Exception e) {
			log.warn("Failed to parse enriched order: {}", e.getMessage());
		}
	}
}
