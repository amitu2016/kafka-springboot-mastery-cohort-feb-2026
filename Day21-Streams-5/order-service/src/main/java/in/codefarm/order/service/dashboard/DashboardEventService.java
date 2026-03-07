package in.codefarm.order.service.dashboard;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds processed results only: data consumed from enriched-orders (stream-table join output).
 */
@Service
public class DashboardEventService {

	private static final int MAX_RECENT = 50;

	private final List<EnrichedOrderView> recentEnrichedOrders = Collections.synchronizedList(new ArrayList<>());

	public void recordEnrichedOrder(String customerId, String orderId, Double totalAmount, String customerName, String customerTier) {
		synchronized (recentEnrichedOrders) {
			recentEnrichedOrders.add(0, new EnrichedOrderView(
					customerId != null ? customerId : "",
					orderId != null ? orderId : "",
					totalAmount != null ? totalAmount : 0.0,
					customerName != null ? customerName : "unknown",
					customerTier != null ? customerTier : "-",
					Instant.now()));
			while (recentEnrichedOrders.size() > MAX_RECENT) recentEnrichedOrders.remove(recentEnrichedOrders.size() - 1);
		}
	}

	public List<EnrichedOrderView> getRecentEnrichedOrders() {
		return List.copyOf(recentEnrichedOrders);
	}

	public record EnrichedOrderView(String customerId, String orderId, double totalAmount, String customerName, String customerTier, Instant at) {}
}
