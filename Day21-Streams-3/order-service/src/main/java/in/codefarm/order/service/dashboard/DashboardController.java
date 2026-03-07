package in.codefarm.order.service.dashboard;

import in.codefarm.order.service.streams.store.StoreQueryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.time.Instant;
import java.util.List;

/**
 * Serves the Day 3 demo UI.
 * Simulation (POST /events, /events/bulk): client only — sends events to Kafka; no access to processing result.
 * Dashboard (GET /): data from processed results only (consumed from order-counts-hourly); alerts from processing only.
 * Also exposes data from state store via Interactive Queries (same data, queried from RocksDB).
 */
@Controller
public class DashboardController {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final DashboardEventService dashboardEventService;
	private final StoreQueryService storeQueryService;
	private final String ordersTopic;

	public DashboardController(KafkaTemplate<String, String> kafkaTemplate,
			DashboardEventService dashboardEventService,
			StoreQueryService storeQueryService,
			@Value("${app.kafka.input-topic:orders}") String ordersTopic) {
		this.kafkaTemplate = kafkaTemplate;
		this.dashboardEventService = dashboardEventService;
		this.storeQueryService = storeQueryService;
		this.ordersTopic = ordersTopic;
	}

	@GetMapping("/")
	public String dashboard(Model model) {
		model.addAttribute("recentWindowedCounts", dashboardEventService.getRecentWindowedCounts());
		model.addAttribute("alerts", dashboardEventService.getAlerts());
		// Data from state store (Interactive Queries) — same semantics as output topic
		boolean storeReady = storeQueryService.isStoreReady();
		model.addAttribute("storeReady", storeReady);
		List<StoreQueryService.WindowedCountEntry> storeCounts = storeReady
				? storeQueryService.fetchAll(Instant.now().minusSeconds(24 * 3600L), Instant.now())
				: List.of();
		model.addAttribute("storeWindowedCounts", storeCounts);
		return "dashboard";
	}

	/**
	 * Simulate a single order event (client). Sends to input topic only; no result awareness.
	 */
	@PostMapping("/events")
	public String sendEvent(
			@RequestParam(required = false) String key,
			@RequestParam(required = false) String customerId,
			RedirectAttributes redirectAttributes) {
		String effectiveCustomerId = (customerId != null && !customerId.isBlank()) ? customerId : "default";
		String recordKey = (key != null && !key.isBlank()) ? key : "customer-" + effectiveCustomerId;
		String value = "{\"customerId\":\"" + recordKey + "\",\"at\":\"" + Instant.now() + "\"}";
		kafkaTemplate.send(ordersTopic, recordKey, value);
		redirectAttributes.addFlashAttribute("success", "Event submitted to topic (key: " + recordKey + "). Windowed counts will appear in dashboard after stream processing.");
		return "redirect:/";
	}

	/**
	 * Simulate bulk order events (client). Sends N events to input topic; no result awareness.
	 */
	@PostMapping("/events/bulk")
	public String sendEventsBulk(
			@RequestParam(name = "count", defaultValue = "10") int count,
			@RequestParam(required = false) String keyPrefix,
			RedirectAttributes redirectAttributes) {
		if (count < 1) count = 1;
		if (count > 500) count = 500;
		String prefix = (keyPrefix != null && !keyPrefix.isBlank()) ? keyPrefix : "customer";
		for (int i = 0; i < count; i++) {
			String recordKey = prefix + "-" + (i % 10);
			String value = "{\"customerId\":\"" + recordKey + "\",\"at\":\"" + Instant.now() + "\"}";
			kafkaTemplate.send(ordersTopic, recordKey, value);
		}
		redirectAttributes.addFlashAttribute("success", count + " events submitted to topic. Windowed counts will appear in dashboard after stream processing.");
		return "redirect:/";
	}
}
