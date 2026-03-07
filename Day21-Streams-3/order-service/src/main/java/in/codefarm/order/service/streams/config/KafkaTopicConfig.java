package in.codefarm.order.service.streams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Day 3 demo: orders (input), order-counts-hourly (windowed output).
 */
@Configuration
public class KafkaTopicConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.output-topic:order-counts-hourly}")
	private String outputTopic;

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
	public NewTopic orderCountsHourlyTopic() {
		return TopicBuilder.name(outputTopic)
				.partitions(partitions)
				.replicas(replicationFactor)
				.build();
	}
}
