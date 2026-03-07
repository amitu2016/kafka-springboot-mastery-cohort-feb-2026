package in.codefarm.order.service.dashboard;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes from customer-order-totals and orders-per-category to update the dashboard with processed results.
 */
@Component
public class DashboardConsumer {

	private static final Logger log = LoggerFactory.getLogger(DashboardConsumer.class);

	private final DashboardEventService dashboardEventService;

	public DashboardConsumer(DashboardEventService dashboardEventService) {
		this.dashboardEventService = dashboardEventService;
	}

	@KafkaListener(
			topics = "${app.kafka.customer-totals-topic:customer-order-totals}",
			containerFactory = "customerTotalsListenerContainerFactory")
	public void onCustomerTotal(ConsumerRecord<String, Double> record) {
		String key = record.key();
		Double total = record.value();
		log.debug("Dashboard: customer total key={} total={}", key, total);
		dashboardEventService.recordCustomerTotal(key != null ? key : "unknown", total != null ? total : 0.0);
	}

	@KafkaListener(
			topics = "${app.kafka.orders-per-category-topic:orders-per-category}",
			containerFactory = "categoryCountListenerContainerFactory")
	public void onCategoryCount(ConsumerRecord<String, Long> record) {
		String key = record.key();
		Long count = record.value();
		log.debug("Dashboard: category count key={} count={}", key, count);
		dashboardEventService.recordCategoryCount(key != null ? key : "unknown", count != null ? count : 0L);
	}
}
