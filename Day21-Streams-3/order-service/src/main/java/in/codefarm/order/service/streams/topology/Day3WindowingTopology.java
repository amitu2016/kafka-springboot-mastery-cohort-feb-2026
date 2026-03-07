package in.codefarm.order.service.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Day 3 demo: tumbling windows — hourly order counts with grace period for late events.
 * Reads from "orders", groups by key, counts per 1-hour window, writes to "order-counts-hourly".
 */
public class Day3WindowingTopology {

	private static final Logger log = LoggerFactory.getLogger(Day3WindowingTopology.class);

	private final String inputTopic;
	private final String outputTopic;
	private final Duration windowSize;
	private final Duration gracePeriod;

	public Day3WindowingTopology(String inputTopic, String outputTopic, Duration windowSize, Duration gracePeriod) {
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
		this.windowSize = windowSize;
		this.gracePeriod = gracePeriod;
	}

	/**
	 * Builds the windowed topology and returns the source stream for Spring lifecycle binding.
	 */
	public KStream<String, String> build(StreamsBuilder builder) {
		KStream<String, String> source = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
		source.filter((key, value) -> value != null && !value.isBlank())
				.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
				.windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gracePeriod))
				.count(Materialized.as("hourly-order-counts"))
				.toStream()
				.peek((windowedKey, count) -> log.info("Window [{} - {}] key={} count={}",
						windowedKey.window().start(), windowedKey.window().end(), windowedKey.key(), count))
				.to(outputTopic, Produced.with(
						WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis()),
						Serdes.Long()));
		return source;
	}
}
