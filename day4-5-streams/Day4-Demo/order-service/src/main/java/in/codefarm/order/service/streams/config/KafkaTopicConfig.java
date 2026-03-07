package in.codefarm.order.service.streams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Day 4 demo: orders (input), customer-order-totals, orders-per-category (outputs).
 */
@Configuration
public class KafkaTopicConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.customer-totals-topic:customer-order-totals}")
	private String customerTotalsTopic;

	@Value("${app.kafka.orders-per-category-topic:orders-per-category}")
	private String ordersPerCategoryTopic;

	@Value("${app.kafka.topic.partitions:2}")
	private int partitions;

	@Value("${app.kafka.topic.replication-factor:1}")
	private short replicationFactor;

	@Bean
	public NewTopic ordersTopic() {
		return TopicBuilder.name(inputTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}

	@Bean
	public NewTopic customerOrderTotalsTopic() {
		return TopicBuilder.name(customerTotalsTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}

	@Bean
	public NewTopic ordersPerCategoryTopic() {
		return TopicBuilder.name(ordersPerCategoryTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}
}
