package in.codefarm.order.service.streams.config;

import tools.jackson.databind.ObjectMapper;
import in.codefarm.order.service.streams.topology.Day4StatefulTopology;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;

/**
 * Day 4 demo: stateful operations — order totals by customer, count per category.
 */
@Configuration
@EnableKafkaStreams
public class StreamsTopologyConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.customer-totals-topic:customer-order-totals}")
	private String customerTotalsTopic;

	@Value("${app.kafka.orders-per-category-topic:orders-per-category}")
	private String ordersPerCategoryTopic;

	@Bean
	public TopologyDescriptionHolder topologyDescriptionHolder() {
		return new TopologyDescriptionHolder();
	}

	@Bean
	public BeanPostProcessor streamsBuilderFactoryBeanCustomizer(TopologyDescriptionHolder holder, MeterRegistry meterRegistry) {
		return new BeanPostProcessor() {
			@Override
			public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
				if (bean instanceof StreamsBuilderFactoryBean factoryBean) {
					factoryBean.setInfrastructureCustomizer(new TopologyCaptureCustomizer(holder));
					factoryBean.addListener(new KafkaStreamsMicrometerListener(meterRegistry, java.util.List.of()));
				}
				return bean;
			}
		};
	}

	@Bean
	public KStream<String, String> orderStream(StreamsBuilder streamsBuilder, ObjectMapper objectMapper) {
		Day4StatefulTopology topology = new Day4StatefulTopology(
				inputTopic,
				customerTotalsTopic,
				ordersPerCategoryTopic,
				objectMapper);
		return topology.build(streamsBuilder);
	}
}
