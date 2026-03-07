package in.codefarm.order.service.streams.config;

import tools.jackson.databind.ObjectMapper;
import in.codefarm.order.service.streams.topology.Day2StatelessTopology;
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
 * Day 2 demo: stateless transformations — mapValues, filter, selectKey, branch.
 */
@Configuration
@EnableKafkaStreams
public class StreamsTopologyConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.high-value-topic:high-value-orders}")
	private String highValueTopic;

	@Value("${app.kafka.normal-topic:normal-orders}")
	private String normalTopic;

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
					// Register Kafka Streams metrics with Micrometer (lag, commit rate, records, etc.)
					factoryBean.addListener(new KafkaStreamsMicrometerListener(meterRegistry, java.util.List.of()));
				}
				return bean;
			}
		};
	}

	@Bean
	public KStream<String, String> orderStream(StreamsBuilder streamsBuilder, ObjectMapper objectMapper) {
		Day2StatelessTopology topology = new Day2StatelessTopology(inputTopic, highValueTopic, normalTopic, objectMapper);
		return topology.build(streamsBuilder);
	}
}
