package in.codefarm.order.service.api;

import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Day 5 demo: POST order events to the orders topic. Body should include customerId, totalAmount, optional orderId and categoryId.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

	private static final Logger log = LoggerFactory.getLogger(OrderController.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String ordersTopic;

	public OrderController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper,
			@org.springframework.beans.factory.annotation.Value("${app.kafka.input-topic:orders}") String ordersTopic) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.ordersTopic = ordersTopic;
	}

	@PostMapping
	public ResponseEntity<Map<String, Object>> placeOrder(
			@RequestBody(required = false) Map<String, Object> body,
			@RequestParam(required = false) String key) {
		String recordKey = (key != null && !key.isBlank()) ? key : "order-" + System.currentTimeMillis();
		String value = toJson(body);
		kafkaTemplate.send(ordersTopic, recordKey, value);
		log.info("Sent order event key={}", recordKey);
		return ResponseEntity.accepted().body(Map.of(
				"message", "Order event sent",
				"key", recordKey
		));
	}

	private String toJson(Map<String, Object> body) {
		if (body == null || body.isEmpty()) {
			String orderId = "order-" + System.currentTimeMillis();
			return "{\"orderId\":\"" + orderId + "\",\"customerId\":\"default\",\"totalAmount\":0.0,\"categoryId\":\"default\"}";
		}
		try {
			return objectMapper.writeValueAsString(body);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid JSON body", e);
		}
	}
}
