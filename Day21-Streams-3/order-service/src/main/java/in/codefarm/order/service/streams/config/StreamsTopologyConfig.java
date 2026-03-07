package in.codefarm.order.service.streams.config;

import in.codefarm.order.service.streams.topology.Day3WindowingTopology;
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

import java.time.Duration;

/**
 * Day 3 demo: windowing — tumbling windows, hourly order counts, grace period.
 */
@Configuration
@EnableKafkaStreams
public class StreamsTopologyConfig {

	@Value("${app.kafka.input-topic:orders}")
	private String inputTopic;

	@Value("${app.kafka.output-topic:order-counts-hourly}")
	private String outputTopic;

	@Value("${app.windowing.size-hours:1}")
	private int windowSizeHours;

	@Value("${app.windowing.grace-minutes:5}")
	private int graceMinutes;

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
	public KStream<String, String> orderStream(StreamsBuilder streamsBuilder) {
		Day3WindowingTopology topology = new Day3WindowingTopology(
				inputTopic,
				outputTopic,
				Duration.ofHours(windowSizeHours),
				Duration.ofMinutes(graceMinutes));
		return topology.build(streamsBuilder);
	}
}
