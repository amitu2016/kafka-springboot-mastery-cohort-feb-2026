package in.codefarm.order.service.dashboard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds processed results only: data consumed from stream output topics (high-value-orders, normal-orders).
 * Alerts and thresholds are derived from processed events only, not from client simulation.
 */
@Service
public class DashboardEventService {

	private static final int MAX_RECENT = 50;
	private static final int MAX_ALERTS = 30;

	private final long highValueCountThreshold;
	private final long normalCountThreshold;

	private final AtomicLong highValueCount = new AtomicLong(0);
	private final AtomicLong normalCount = new AtomicLong(0);

	private final List<ProcessedOrder> recentHighValue = Collections.synchronizedList(new ArrayList<>());
	private final List<ProcessedOrder> recentNormal = Collections.synchronizedList(new ArrayList<>());
	private final List<Alert> alerts = Collections.synchronizedList(new ArrayList<>());

	public DashboardEventService(
			@Value("${app.dashboard.alert.high-value-count-threshold:5}") long highValueCountThreshold,
			@Value("${app.dashboard.alert.normal-count-threshold:10}") long normalCountThreshold) {
		this.highValueCountThreshold = highValueCountThreshold;
		this.normalCountThreshold = normalCountThreshold;
	}

	/**
	 * Called when a record is consumed from high-value-orders (processed by stream).
	 */
	public void recordHighValueRouted(String key, String value) {
		long count = highValueCount.incrementAndGet();
		trimAndAddHighValue(new ProcessedOrder(key, value, Instant.now()));
		trimAndAddAlert(new Alert(AlertType.HIGH_VALUE_ROUTED, "Processed: order routed to high-value-orders (amount ≥ 1000)", Instant.now()));
		if (count == highValueCountThreshold) {
			trimAndAddAlert(new Alert(AlertType.THRESHOLD, "High-value count reached threshold: " + count, Instant.now()));
		}
	}

	/**
	 * Called when a record is consumed from normal-orders (processed by stream).
	 */
	public void recordNormalRouted(String key, String value) {
		long count = normalCount.incrementAndGet();
		trimAndAddNormal(new ProcessedOrder(key, value, Instant.now()));
		if (count == normalCountThreshold) {
			trimAndAddAlert(new Alert(AlertType.THRESHOLD, "Normal count reached threshold: " + count, Instant.now()));
		}
	}

	public long getHighValueCount() {
		return highValueCount.get();
	}

	public long getNormalCount() {
		return normalCount.get();
	}

	public List<ProcessedOrder> getRecentHighValue() {
		return List.copyOf(recentHighValue);
	}

	public List<ProcessedOrder> getRecentNormal() {
		return List.copyOf(recentNormal);
	}

	public List<Alert> getAlerts() {
		return List.copyOf(alerts);
	}

	private void trimAndAddHighValue(ProcessedOrder item) {
		synchronized (recentHighValue) {
			recentHighValue.add(0, item);
			while (recentHighValue.size() > MAX_RECENT) recentHighValue.remove(recentHighValue.size() - 1);
		}
	}

	private void trimAndAddNormal(ProcessedOrder item) {
		synchronized (recentNormal) {
			recentNormal.add(0, item);
			while (recentNormal.size() > MAX_RECENT) recentNormal.remove(recentNormal.size() - 1);
		}
	}

	private void trimAndAddAlert(Alert alert) {
		synchronized (alerts) {
			alerts.add(0, alert);
			while (alerts.size() > MAX_ALERTS) alerts.remove(alerts.size() - 1);
		}
	}

	public record ProcessedOrder(String key, String valuePreview, Instant at) {}
	public record Alert(AlertType type, String message, Instant at) {}
	public enum AlertType { HIGH_VALUE_ROUTED, THRESHOLD }
}
