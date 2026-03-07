package in.codefarm.order.service.streams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates required Kafka topics on application startup.
 * Uses the same topic names as the streams topology (app.kafka.*).
 */
@Configuration
public class KafkaTopicConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.output-topic:orders-processed}")
	private String outputTopic;

	@Value("${app.kafka.reference-topic:customer-reference}")
	private String referenceTopic;

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
	public NewTopic ordersProcessedTopic() {
		return TopicBuilder.name(outputTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}

	@Bean
	public NewTopic customerReferenceTopic() {
		return TopicBuilder.name(referenceTopic)
				.partitions(1)
				.replicas(replicationFactor)
				.build();
	}

	@Bean
	public NewTopic orderKeyCountsTopic() {
		return TopicBuilder.name("order-key-counts")
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}

	@Bean
	public NewTopic orderCategoryCountsTopic() {
		return TopicBuilder.name("order-category-counts")
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}
}
