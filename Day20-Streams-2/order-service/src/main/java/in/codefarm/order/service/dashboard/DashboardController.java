package in.codefarm.order.service.dashboard;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

/**
 * Serves the Day 2 demo UI.
 * Simulation (POST /orders, /orders/bulk): client only — sends events to Kafka; no access to processing result.
 * Dashboard (GET /): data from processed results only (consumed from output topics); alerts from processing only.
 */
@Controller
public class DashboardController {

	private static final double HIGH_VALUE_AMOUNT = 1000.0;

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final DashboardEventService dashboardEventService;
	private final String ordersTopic;
	private final ObjectMapper objectMapper;

	public DashboardController(KafkaTemplate<String, String> kafkaTemplate,
			DashboardEventService dashboardEventService,
			@Value("${app.kafka.input-topic:orders}") String ordersTopic,
			ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.dashboardEventService = dashboardEventService;
		this.ordersTopic = ordersTopic;
		this.objectMapper = objectMapper;
	}

	@GetMapping("/")
	public String dashboard(Model model) {
		model.addAttribute("highValueCount", dashboardEventService.getHighValueCount());
		model.addAttribute("normalCount", dashboardEventService.getNormalCount());
		model.addAttribute("recentHighValue", dashboardEventService.getRecentHighValue());
		model.addAttribute("recentNormal", dashboardEventService.getRecentNormal());
		model.addAttribute("alerts", dashboardEventService.getAlerts());
		return "dashboard";
	}

	/**
	 * Simulate a single order (client). Sends to input topic only; no result awareness.
	 */
	@PostMapping("/orders")
	public String placeOrder(
			@RequestParam(required = false) String orderId,
			@RequestParam(required = false) String customerId,
			@RequestParam(required = false) Double totalAmount,
			@RequestParam(defaultValue = "valid") String scenario,
			RedirectAttributes redirectAttributes) {
		String key = orderId != null && !orderId.isBlank() ? orderId : UUID.randomUUID().toString();
		String value = buildPayload(orderId, customerId, totalAmount, scenario);
		kafkaTemplate.send(ordersTopic, key, value);
		redirectAttributes.addFlashAttribute("success", "Order event submitted to topic (key: " + key + "). Result will appear in dashboard after stream processing.");
		return "redirect:/";
	}

	/**
	 * Simulate bulk order events (client). Sends N events to input topic; no result awareness.
	 */
	@PostMapping("/orders/bulk")
	public String placeOrdersBulk(
			@RequestParam(name = "count", defaultValue = "10") int count,
			@RequestParam(defaultValue = "valid") String scenario,
			RedirectAttributes redirectAttributes) {
		if (count < 1) count = 1;
		if (count > 500) count = 500;
		for (int i = 0; i < count; i++) {
			String key = "ord-bulk-" + System.currentTimeMillis() + "-" + i;
			String value = buildPayload(null, "customer-" + (i % 5), null, scenario);
			kafkaTemplate.send(ordersTopic, key, value);
		}
		redirectAttributes.addFlashAttribute("success", count + " order events submitted to topic. Results will appear in dashboard after stream processing.");
		return "redirect:/";
	}

	private String buildPayload(String orderId, String customerId, Double totalAmount, String scenario) {
		String oid = orderId != null && !orderId.isBlank() ? orderId : "ord-" + System.currentTimeMillis();
		String cid = customerId != null && !customerId.isBlank() ? customerId : "customer-1";
		Double amount = totalAmount != null ? totalAmount : switch (scenario.toLowerCase()) {
			case "high-value" -> HIGH_VALUE_AMOUNT;
			case "normal" -> 99.99;
			default -> 150.0;
		};
		if ("malformed".equalsIgnoreCase(scenario)) {
			return "not-valid-json {";
		}
		try {
			return objectMapper.writeValueAsString(Map.of(
					"orderId", oid,
					"customerId", cid,
					"totalAmount", amount,
					"status", "PLACED"
			));
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to build order payload", e);
		}
	}
}
