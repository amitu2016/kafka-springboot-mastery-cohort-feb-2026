package in.codefarm.order.service.api;

import in.codefarm.order.service.api.dto.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * REST API to place orders for Day 2 demo scenarios. Publishes to the orders topic for stream processing.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

	private static final Logger log = LoggerFactory.getLogger(OrderController.class);
	private static final String ORDERS_TOPIC = "orders";
	private static final double HIGH_VALUE_THRESHOLD = 1000.0;

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;

	public OrderController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	/**
	 * Place an order. Sends the payload to the orders topic for stream processing.
	 * Use scenario query param or request body to drive different flows (valid, high-value, normal, malformed).
	 */
	@PostMapping
	public ResponseEntity<Map<String, Object>> placeOrder(
			@RequestBody OrderRequest body,
			@RequestParam(required = false) String scenario) {
		String effectiveScenario = scenario != null ? scenario : (body != null && body.scenario() != null ? body.scenario() : OrderRequest.SCENARIO_VALID);

		String key = body.orderId() != null ? body.orderId() : java.util.UUID.randomUUID().toString();
		String value;

		OrderRequest bodySafe = body != null ? body : new OrderRequest(null, null, null, null, null);

		switch (effectiveScenario.toLowerCase()) {
			case OrderRequest.SCENARIO_MALFORMED -> {
				// Send invalid JSON so the stream's mapValues(parse) will drop it (malformed data handling)
				value = "not-valid-json {";
				log.info("Placing order with scenario=malformed (invalid JSON)");
			}
			case OrderRequest.SCENARIO_HIGH_VALUE -> {
				OrderRequest highValue = new OrderRequest(
						bodySafe.orderId() != null ? bodySafe.orderId() : "ord-high-" + System.currentTimeMillis(),
						bodySafe.customerId() != null ? bodySafe.customerId() : "customer-1",
						bodySafe.totalAmount() != null && bodySafe.totalAmount() >= HIGH_VALUE_THRESHOLD ? bodySafe.totalAmount() : HIGH_VALUE_THRESHOLD,
						bodySafe.status() != null ? bodySafe.status() : "PLACED",
						null
				);
				value = toJson(highValue);
				log.info("Placing high-value order: amount={}", highValue.totalAmount());
			}
			case OrderRequest.SCENARIO_NORMAL -> {
				OrderRequest normal = new OrderRequest(
						bodySafe.orderId() != null ? bodySafe.orderId() : "ord-normal-" + System.currentTimeMillis(),
						bodySafe.customerId() != null ? bodySafe.customerId() : "customer-1",
						bodySafe.totalAmount() != null && bodySafe.totalAmount() < HIGH_VALUE_THRESHOLD ? bodySafe.totalAmount() : 99.99,
						bodySafe.status() != null ? bodySafe.status() : "PLACED",
						null
				);
				value = toJson(normal);
				log.info("Placing normal order: amount={}", normal.totalAmount());
			}
			default -> {
				// valid
				OrderRequest valid = new OrderRequest(
						bodySafe.orderId() != null ? bodySafe.orderId() : "ord-" + System.currentTimeMillis(),
						bodySafe.customerId() != null ? bodySafe.customerId() : "customer-1",
						bodySafe.totalAmount() != null ? bodySafe.totalAmount() : 150.0,
						bodySafe.status() != null ? bodySafe.status() : "PLACED",
						null
				);
				value = toJson(valid);
				log.info("Placing valid order: orderId={}", valid.orderId());
			}
		}

		kafkaTemplate.send(ORDERS_TOPIC, key, value);

		return ResponseEntity.accepted().body(Map.of(
				"message", "Order submitted for processing",
				"scenario", effectiveScenario,
				"key", key
		));
	}

	private String toJson(OrderRequest r) {
		try {
			return objectMapper.writeValueAsString(Map.of(
					"orderId", r.orderId() != null ? r.orderId() : "",
					"customerId", r.customerId() != null ? r.customerId() : "",
					"totalAmount", r.totalAmount() != null ? r.totalAmount() : 0,
					"status", r.status() != null ? r.status() : "PLACED"
			));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
