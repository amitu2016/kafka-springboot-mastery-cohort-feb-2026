package in.codefarm.order.service.dashboard;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer config for dashboard: read from enriched-orders (String, String).
 */
@Configuration
public class DashboardConsumerConfig {

	@Value("${spring.kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;

	private Map<String, Object> baseConsumerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "day5-dashboard");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> enrichedOrdersListenerContainerFactory() {
		ConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(baseConsumerProps(),
				new StringDeserializer(),
				new StringDeserializer());
		ConcurrentKafkaListenerContainerFactory<String, String> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		containerFactory.setConsumerFactory(factory);
		return containerFactory;
	}
}
