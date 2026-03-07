package in.codefarm.order.service.dashboard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds processed results only: data consumed from stream output topic (order-counts-hourly).
 * Alerts and thresholds are derived from processed windowed counts only, not from client simulation.
 */
@Service
public class DashboardEventService {

	private static final int MAX_WINDOWED = 50;
	private static final int MAX_ALERTS = 30;

	private final long countThreshold;

	private final List<WindowedCount> recentWindowedCounts = Collections.synchronizedList(new ArrayList<>());
	private final List<Alert> alerts = Collections.synchronizedList(new ArrayList<>());

	public DashboardEventService(
			@Value("${app.dashboard.alert.count-threshold:5}") long countThreshold) {
		this.countThreshold = countThreshold;
	}

	/**
	 * Called when a record is consumed from order-counts-hourly (processed by stream).
	 */
	public void recordWindowedCount(String key, long windowStartMs, long windowEndMs, long count) {
		trimAndAddWindowed(new WindowedCount(key, windowStartMs, windowEndMs, count, Instant.now()));
		if (count >= countThreshold) {
			trimAndAddAlert(new Alert(AlertType.THRESHOLD, "Hourly count for " + key + " = " + count + " (≥ " + countThreshold + ")", Instant.now()));
		}
	}

	public List<WindowedCount> getRecentWindowedCounts() {
		return List.copyOf(recentWindowedCounts);
	}

	public List<Alert> getAlerts() {
		return List.copyOf(alerts);
	}

	private void trimAndAddWindowed(WindowedCount item) {
		synchronized (recentWindowedCounts) {
			recentWindowedCounts.add(0, item);
			while (recentWindowedCounts.size() > MAX_WINDOWED) recentWindowedCounts.remove(recentWindowedCounts.size() - 1);
		}
	}

	private void trimAndAddAlert(Alert alert) {
		synchronized (alerts) {
			alerts.add(0, alert);
			while (alerts.size() > MAX_ALERTS) alerts.remove(alerts.size() - 1);
		}
	}

	public record WindowedCount(String key, long windowStartMs, long windowEndMs, long count, Instant at) {}
	public record Alert(AlertType type, String message, Instant at) {}
	public enum AlertType { THRESHOLD }
}
