package in.codefarm.order.service.streams.config;

import in.codefarm.order.service.streams.topology.OrderStreamsTopology;
import in.codefarm.order.service.streams.topology.SubTopologyDemoTopology;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;

/**
 * Day 1 demo: configures Kafka Streams topology (read → transform → write)
 * and KStream/KTable examples for the order-service.
 */
@Configuration
@EnableKafkaStreams
public class StreamsTopologyConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.output-topic:orders-processed}")
	private String outputTopic;

	@Value("${app.kafka.reference-topic:customer-reference}")
	private String referenceTopic;

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
	@ConditionalOnProperty(name = "app.kafka.topology", havingValue = "default", matchIfMissing = true)
	public KStream<String, String> orderStream(StreamsBuilder streamsBuilder) {
		OrderStreamsTopology topology = new OrderStreamsTopology(inputTopic, outputTopic, referenceTopic);
		return topology.build(streamsBuilder);
	}

	/**
	 * Activate with app.kafka.topology=subtopology-demo to see how Kafka Streams
	 * splits a topology into sub-topologies. Visit /api/streams/topology/visual.
	 */
	@Bean
	@ConditionalOnProperty(name = "app.kafka.topology", havingValue = "subtopology-demo")
	public KStream<String, String> subTopologyDemoStream(StreamsBuilder streamsBuilder) {
		SubTopologyDemoTopology topology = new SubTopologyDemoTopology(inputTopic, outputTopic, referenceTopic);
		return topology.build(streamsBuilder);
	}
}
