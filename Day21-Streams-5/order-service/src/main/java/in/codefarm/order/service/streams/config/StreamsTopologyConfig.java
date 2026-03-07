package in.codefarm.order.service.streams.config;

import tools.jackson.databind.ObjectMapper;
import in.codefarm.order.service.streams.topology.Day5EnrichmentTopology;
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
 * Day 5 demo: stream-table join — order enrichment with customer data.
 */
@Configuration
@EnableKafkaStreams
public class StreamsTopologyConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String ordersTopic;

	@Value("${app.kafka.customers-topic:customers}")
	private String customersTopic;

	@Value("${app.kafka.enriched-orders-topic:enriched-orders}")
	private String enrichedOrdersTopic;

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
		Day5EnrichmentTopology topology = new Day5EnrichmentTopology(
				ordersTopic,
				customersTopic,
				enrichedOrdersTopic,
				objectMapper);
		return topology.build(streamsBuilder);
	}
}
