package in.codefarm.order.service.dashboard;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
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
 * Consumer config for dashboard: read from customer-order-totals (String, Double) and orders-per-category (String, Long).
 */
@Configuration
public class DashboardConsumerConfig {

	@Value("${spring.kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;

	@Value("${app.kafka.customer-totals-topic:customer-order-totals}")
	private String customerTotalsTopic;

	@Value("${app.kafka.orders-per-category-topic:orders-per-category}")
	private String ordersPerCategoryTopic;

	private Map<String, Object> baseConsumerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "day4-dashboard");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Double> customerTotalsListenerContainerFactory() {
		Map<String, Object> props = new HashMap<>(baseConsumerProps());
		ConsumerFactory<String, Double> factory = new DefaultKafkaConsumerFactory<>(props,
				new StringDeserializer(),
				new DoubleDeserializer());
		ConcurrentKafkaListenerContainerFactory<String, Double> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		containerFactory.setConsumerFactory(factory);
		return containerFactory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Long> categoryCountListenerContainerFactory() {
		Map<String, Object> props = new HashMap<>(baseConsumerProps());
		ConsumerFactory<String, Long> factory = new DefaultKafkaConsumerFactory<>(props,
				new StringDeserializer(),
				new LongDeserializer());
		ConcurrentKafkaListenerContainerFactory<String, Long> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		containerFactory.setConsumerFactory(factory);
		return containerFactory;
	}
}
