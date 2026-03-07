package in.codefarm.order.service.streams.controller;

import in.codefarm.order.service.streams.store.StoreQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * REST API for Interactive Queries: reads windowed counts directly from the
 * Kafka Streams state store (RocksDB-backed) instead of consuming the output topic.
 */
@RestController
@RequestMapping("/api/store")
public class InteractiveQueryController {

	private final StoreQueryService storeQueryService;

	public InteractiveQueryController(StoreQueryService storeQueryService) {
		this.storeQueryService = storeQueryService;
	}

	/**
	 * Get windowed counts from the state store.
	 * Optional: key (filter by key), from/to as epoch seconds (default: last 24 hours).
	 */
	@GetMapping("/hourly-counts")
	public ResponseEntity<Map<String, Object>> getHourlyCounts(
			@RequestParam(required = false) String key,
			@RequestParam(required = false) Long from,
			@RequestParam(required = false) Long to) {
		if (!storeQueryService.isStoreReady()) {
			return ResponseEntity.status(503).body(Map.of(
					"status", "store_not_ready",
					"message", "Kafka Streams is not in RUNNING state. Wait for the application to be ready."
			));
		}
		long nowSec = Instant.now().getEpochSecond();
		long defaultRangeSec = 24 * 3600L;
		Instant fromInstant = from != null ? Instant.ofEpochSecond(from) : Instant.ofEpochSecond(nowSec - defaultRangeSec);
		Instant toInstant = to != null ? Instant.ofEpochSecond(to) : Instant.now();

		List<StoreQueryService.WindowedCountEntry> entries;
		if (key != null && !key.isBlank()) {
			entries = storeQueryService.fetchByKey(key.trim(), fromInstant, toInstant);
		} else {
			entries = storeQueryService.fetchAll(fromInstant, toInstant);
		}
		List<Map<String, Object>> items = entries.stream()
				.map(e -> Map.<String, Object>of(
						"key", e.key(),
						"windowStartMs", e.windowStartMs(),
						"windowEndMs", e.windowEndMs(),
						"count", e.count()))
				.toList();
		return ResponseEntity.ok(Map.of(
				"source", "state_store",
				"storeName", "hourly-order-counts",
				"entries", items));
	}
}
