package in.codefarm.order.service.streams.store;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Queries the Kafka Streams state store (RocksDB-backed) via Interactive Queries.
 * Used by REST API and dashboard to read windowed counts directly from the store.
 */
@Service
public class StoreQueryService {

	private static final String STORE_NAME = "hourly-order-counts";
	private static final Logger log = LoggerFactory.getLogger(StoreQueryService.class);

	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	public StoreQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
	}

	/**
	 * Whether the streams instance is running and the store can be queried.
	 */
	public boolean isStoreReady() {
		KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
		return kafkaStreams != null && kafkaStreams.state() == KafkaStreams.State.RUNNING;
	}

	/**
	 * Fetch windowed counts for a single key in the given time range (window start times).
	 */
	public List<WindowedCountEntry> fetchByKey(String key, Instant from, Instant to) {
		List<WindowedCountEntry> result = new ArrayList<>();
		KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
		if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
			return result;
		}
		try {
			ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
					StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));
			try (WindowStoreIterator<Long> it = store.fetch(key, from, to)) {
				while (it.hasNext()) {
					var next = it.next();
					long windowStart = next.key;   // KeyValue<Long, V>: key = window start ms
					long windowEnd = windowStart + 3600_000L; // 1h in ms
					result.add(new WindowedCountEntry(key, windowStart, windowEnd, next.value));
				}
			}
		} catch (Exception e) {
			log.warn("Failed to query store by key key={} from={} to={}", key, from, to, e);
		}
		return result;
	}

	/**
	 * Fetch all windowed counts in the given time range (window start times).
	 */
	public List<WindowedCountEntry> fetchAll(Instant from, Instant to) {
		List<WindowedCountEntry> result = new ArrayList<>();
		KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
		if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
			return result;
		}
		try {
			ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
					StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));
			try (var it = store.fetchAll(from, to)) {
				while (it.hasNext()) {
					var next = it.next();
					var wk = next.key;
					long windowStart = wk.window().start();
					long windowEnd = wk.window().end();
					Long count = next.value;
					if (count != null) {
						result.add(new WindowedCountEntry(wk.key(), windowStart, windowEnd, count));
					}
				}
			}
		} catch (Exception e) {
			log.warn("Failed to query store fetchAll from={} to={}", from, to, e);
		}
		return result;
	}

	public record WindowedCountEntry(String key, long windowStartMs, long windowEndMs, long count) {}
}
