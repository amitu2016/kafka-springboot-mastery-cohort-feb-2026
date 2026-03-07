package in.codefarm.order.service.dashboard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds processed results only: data consumed from customer-order-totals and orders-per-category.
 * Alerts are derived from processed results only (e.g. total or count crosses threshold).
 */
@Service
public class DashboardEventService {

	private static final int MAX_RECENT = 50;
	private static final int MAX_ALERTS = 30;

	private final double totalThreshold;
	private final long categoryCountThreshold;

	private final List<CustomerTotal> recentCustomerTotals = Collections.synchronizedList(new ArrayList<>());
	private final List<CategoryCount> recentCategoryCounts = Collections.synchronizedList(new ArrayList<>());
	private final List<Alert> alerts = Collections.synchronizedList(new ArrayList<>());

	public DashboardEventService(
			@Value("${app.dashboard.alert.total-threshold:500}") double totalThreshold,
			@Value("${app.dashboard.alert.category-count-threshold:10}") long categoryCountThreshold) {
		this.totalThreshold = totalThreshold;
		this.categoryCountThreshold = categoryCountThreshold;
	}

	public void recordCustomerTotal(String customerId, double total) {
		trimAndAdd(recentCustomerTotals, new CustomerTotal(customerId, total, Instant.now()), MAX_RECENT);
		if (total >= totalThreshold) {
			trimAndAddAlert(new Alert(AlertType.THRESHOLD, "Customer " + customerId + " total ≥ " + totalThreshold + " (current: " + total + ")", Instant.now()));
		}
	}

	public void recordCategoryCount(String categoryId, long count) {
		trimAndAdd(recentCategoryCounts, new CategoryCount(categoryId, count, Instant.now()), MAX_RECENT);
		if (count >= categoryCountThreshold) {
			trimAndAddAlert(new Alert(AlertType.THRESHOLD, "Category " + categoryId + " count ≥ " + categoryCountThreshold + " (current: " + count + ")", Instant.now()));
		}
	}

	public List<CustomerTotal> getRecentCustomerTotals() {
		return List.copyOf(recentCustomerTotals);
	}

	public List<CategoryCount> getRecentCategoryCounts() {
		return List.copyOf(recentCategoryCounts);
	}

	public List<Alert> getAlerts() {
		return List.copyOf(alerts);
	}

	private <T> void trimAndAdd(List<T> list, T item, int max) {
		synchronized (list) {
			list.add(0, item);
			while (list.size() > max) list.remove(list.size() - 1);
		}
	}

	private void trimAndAddAlert(Alert alert) {
		synchronized (alerts) {
			alerts.add(0, alert);
			while (alerts.size() > MAX_ALERTS) alerts.remove(alerts.size() - 1);
		}
	}

	public record CustomerTotal(String customerId, double total, Instant at) {}
	public record CategoryCount(String categoryId, long count, Instant at) {}
	public record Alert(AlertType type, String message, Instant at) {}
	public enum AlertType { THRESHOLD }
}
