package in.codefarm.order.service.dashboard;

import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Map;

/**
 * Serves the Day 5 demo UI.
 * Simulation: send orders (client) and add customers (to customers table). Dashboard shows enriched orders (from enriched-orders topic).
 */
@Controller
public class DashboardController {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final DashboardEventService dashboardEventService;
	private final ObjectMapper objectMapper;
	private final String ordersTopic;
	private final String customersTopic;

	private static final String[] CATEGORIES = { "electronics", "books", "home", "sports", "toys" };

	public DashboardController(KafkaTemplate<String, String> kafkaTemplate,
			DashboardEventService dashboardEventService,
			ObjectMapper objectMapper,
			@Value("${app.kafka.input-topic:orders}") String ordersTopic,
			@Value("${app.kafka.customers-topic:customers}") String customersTopic) {
		this.kafkaTemplate = kafkaTemplate;
		this.dashboardEventService = dashboardEventService;
		this.objectMapper = objectMapper;
		this.ordersTopic = ordersTopic;
		this.customersTopic = customersTopic;
	}

	@GetMapping("/")
	public String dashboard(Model model) {
		model.addAttribute("recentEnrichedOrders", dashboardEventService.getRecentEnrichedOrders());
		return "dashboard";
	}

	@PostMapping("/orders")
	public String sendOrder(
			@RequestParam(required = false) String customerId,
			@RequestParam(required = false) String totalAmount,
			@RequestParam(required = false) String categoryId,
			RedirectAttributes redirectAttributes) {
		String customer = (customerId != null && !customerId.isBlank()) ? customerId : "customer-default";
		double amount = parseAmount(totalAmount, 50.0);
		String category = (categoryId != null && !categoryId.isBlank()) ? categoryId : "default";
		String orderId = "order-" + System.currentTimeMillis();
		String value = toOrderJson(orderId, customer, amount, category);
		kafkaTemplate.send(ordersTopic, orderId, value);
		redirectAttributes.addFlashAttribute("success", "Order submitted (orderId=" + orderId + ", customer=" + customer + "). Add the customer to the table first if needed; enriched result will appear after stream processing.");
		return "redirect:/";
	}

	@PostMapping("/orders/bulk")
	public String sendOrdersBulk(
			@RequestParam(name = "count", defaultValue = "10") int count,
			@RequestParam(required = false) String customerPrefix,
			RedirectAttributes redirectAttributes) {
		if (count < 1) count = 1;
		if (count > 500) count = 500;
		String prefix = (customerPrefix != null && !customerPrefix.isBlank()) ? customerPrefix : "customer";
		for (int i = 0; i < count; i++) {
			String customer = prefix + "-" + (i % 10);
			double amount = 10.0 + (i % 5) * 25.0;
			String category = CATEGORIES[i % CATEGORIES.length];
			String orderId = "order-" + System.currentTimeMillis() + "-" + i;
			String value = toOrderJson(orderId, customer, amount, category);
			kafkaTemplate.send(ordersTopic, orderId, value);
		}
		redirectAttributes.addFlashAttribute("success", count + " orders submitted. Enriched orders will appear in dashboard after stream processing (ensure customers exist in the table).");
		return "redirect:/";
	}

	/**
	 * Add or update a customer in the customers table (key = customerId).
	 */
	@PostMapping("/customers")
	public String addCustomer(
			@RequestParam String customerId,
			@RequestParam(required = false) String name,
			@RequestParam(required = false) String tier,
			RedirectAttributes redirectAttributes) {
		if (customerId == null || customerId.isBlank()) {
			redirectAttributes.addFlashAttribute("success", "Customer ID is required.");
			return "redirect:/";
		}
		String value = toCustomerJson(customerId, name != null ? name : "Customer-" + customerId, tier != null ? tier : "standard");
		kafkaTemplate.send(customersTopic, customerId, value);
		redirectAttributes.addFlashAttribute("success", "Customer " + customerId + " added/updated. Orders for this customer will now be enriched with this data.");
		return "redirect:/";
	}

	private String toOrderJson(String orderId, String customerId, double totalAmount, String categoryId) {
		try {
			return objectMapper.writeValueAsString(Map.of(
					"orderId", orderId,
					"customerId", customerId,
					"totalAmount", totalAmount,
					"categoryId", categoryId));
		} catch (Exception e) {
			return "{\"orderId\":\"" + orderId + "\",\"customerId\":\"" + customerId + "\",\"totalAmount\":" + totalAmount + ",\"categoryId\":\"" + categoryId + "\"}";
		}
	}

	private String toCustomerJson(String customerId, String name, String tier) {
		try {
			return objectMapper.writeValueAsString(Map.of(
					"customerId", customerId,
					"name", name,
					"tier", tier));
		} catch (Exception e) {
			return "{\"customerId\":\"" + customerId + "\",\"name\":\"" + name + "\",\"tier\":\"" + tier + "\"}";
		}
	}

	private static double parseAmount(String s, double defaultVal) {
		if (s == null || s.isBlank()) return defaultVal;
		try {
			return Double.parseDouble(s.trim());
		} catch (NumberFormatException e) {
			return defaultVal;
		}
	}
}
