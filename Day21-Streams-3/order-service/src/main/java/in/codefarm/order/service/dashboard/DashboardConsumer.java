package in.codefarm.order.service.dashboard;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes from order-counts-hourly to update the dashboard with windowed counts.
 */
@Component
public class DashboardConsumer {

	private static final Logger log = LoggerFactory.getLogger(DashboardConsumer.class);

	private final DashboardEventService dashboardEventService;

	public DashboardConsumer(DashboardEventService dashboardEventService) {
		this.dashboardEventService = dashboardEventService;
	}

	@KafkaListener(
			topics = "${app.kafka.output-topic:order-counts-hourly}",
			groupId = "day3-dashboard-consumer",
			containerFactory = "dashboardListenerContainerFactory"
	)
	public void onWindowedCount(ConsumerRecord<Windowed<String>, Long> record) {
		Windowed<String> wk = record.key();
		Long count = record.value();
		log.debug("Dashboard: windowed count key={} window=[{} - {}] count={}", wk.key(), wk.window().start(), wk.window().end(), count);
		dashboardEventService.recordWindowedCount(wk.key(), wk.window().start(), wk.window().end(), count != null ? count : 0L);
	}
}
