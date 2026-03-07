package in.codefarm.order.service.dashboard;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Consumes from stream output topics to update the dashboard. Uses a separate group id
 * so it does not interfere with the Kafka Streams application.
 */
@Component
public class DashboardConsumer {

	private static final Logger log = LoggerFactory.getLogger(DashboardConsumer.class);

	private final DashboardEventService dashboardEventService;

	public DashboardConsumer(DashboardEventService dashboardEventService) {
		this.dashboardEventService = dashboardEventService;
	}

	@KafkaListener(
			topics = "${app.kafka.high-value-topic:high-value-orders}",
			groupId = "day2-dashboard-consumer",
			containerFactory = "kafkaListenerContainerFactory"
	)
	public void onHighValueOrder(ConsumerRecord<String, String> record,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
		log.debug("Dashboard: received from {} key={}", topic, record.key());
		String preview = record.value() != null && record.value().length() > 80
				? record.value().substring(0, 80) + "…"
				: record.value();
		dashboardEventService.recordHighValueRouted(record.key(), preview);
	}

	@KafkaListener(
			topics = "${app.kafka.normal-topic:normal-orders}",
			groupId = "day2-dashboard-consumer",
			containerFactory = "kafkaListenerContainerFactory"
	)
	public void onNormalOrder(ConsumerRecord<String, String> record,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
		log.debug("Dashboard: received from {} key={}", topic, record.key());
		String preview = record.value() != null && record.value().length() > 80
				? record.value().substring(0, 80) + "…"
				: record.value();
		dashboardEventService.recordNormalRouted(record.key(), preview);
	}
}
