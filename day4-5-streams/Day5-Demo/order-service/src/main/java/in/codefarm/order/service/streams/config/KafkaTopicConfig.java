package in.codefarm.order.service.streams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Day 5 demo: orders (input stream), customers (table), enriched-orders (output).
 */
@Configuration
public class KafkaTopicConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.customers-topic:customers}")
	private String customersTopic;

	@Value("${app.kafka.enriched-orders-topic:enriched-orders}")
	private String enrichedOrdersTopic;

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
	public NewTopic customersTopic() {
		return TopicBuilder.name(customersTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}

	@Bean
	public NewTopic enrichedOrdersTopic() {
		return TopicBuilder.name(enrichedOrdersTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}
}
