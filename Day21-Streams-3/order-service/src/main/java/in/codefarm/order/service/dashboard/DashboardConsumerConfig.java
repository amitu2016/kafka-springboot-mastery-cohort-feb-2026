package in.codefarm.order.service.dashboard;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer config for the dashboard: reads from order-counts-hourly (Windowed&lt;String&gt;, Long).
 */
@Configuration
public class DashboardConsumerConfig {

	@Value("${spring.kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;

	@Value("${app.windowing.size-hours:1}")
	private int windowSizeHours;

	@Bean("dashboardListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<Windowed<String>, Long> dashboardListenerContainerFactory() {
		long windowSizeMs = windowSizeHours * 3600L * 1000;
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "day3-dashboard-consumer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		Deserializer<Windowed<String>> keyDeserializer = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSizeMs).deserializer();

		ConsumerFactory<Windowed<String>, Long> factory = new DefaultKafkaConsumerFactory<>(
				props,
				keyDeserializer,
				new LongDeserializer()
		);

		ConcurrentKafkaListenerContainerFactory<Windowed<String>, Long> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		containerFactory.setConsumerFactory(factory);
		containerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
		return containerFactory;
	}
}
