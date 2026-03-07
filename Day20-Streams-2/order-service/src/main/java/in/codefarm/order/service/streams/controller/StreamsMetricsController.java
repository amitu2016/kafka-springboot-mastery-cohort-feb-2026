package in.codefarm.order.service.streams.controller;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Exposes stream-related and JVM metrics for the in-app dashboard.
 */
@RestController
@RequestMapping("/api/streams/metrics")
public class StreamsMetricsController {

	private static final String KAFKA_PREFIX = "kafka.";
	private static final String JVM_PREFIX = "jvm.";

	private final MeterRegistry meterRegistry;

	public StreamsMetricsController(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	/**
	 * Returns a summary of metrics (name, value, tags) for kafka.* and optionally jvm.*.
	 * Used by the metrics visual page.
	 */
	@GetMapping("/summary")
	public ResponseEntity<Map<String, Object>> summary(
			@RequestParam(defaultValue = "true") boolean kafka,
			@RequestParam(defaultValue = "false") boolean jvm) {
		List<Map<String, Object>> kafkaMetrics = kafka ? collectMetrics(KAFKA_PREFIX) : List.of();
		List<Map<String, Object>> jvmMetrics = jvm ? collectMetrics(JVM_PREFIX) : List.of();
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("kafka", kafkaMetrics);
		body.put("jvm", jvmMetrics);
		body.put("refreshedAt", java.time.Instant.now().toString());
		return ResponseEntity.ok(body);
	}

	/**
	 * Returns an HTML page that fetches /summary and displays metrics in tables with auto-refresh.
	 */
	@GetMapping(value = "/visual", produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<String> visual() {
		String html = """
			<!DOCTYPE html>
			<html>
			<head>
			  <meta charset="UTF-8">
			  <title>Streams Metrics Dashboard</title>
			  <style>
			    body { font-family: system-ui, sans-serif; margin: 1rem 2rem; background: #f5f5f5; }
			    h1 { color: #333; }
			    h2 { color: #555; margin-top: 1.5rem; font-size: 1.1rem; }
			    table { border-collapse: collapse; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }
			    th, td { padding: 0.5rem 1rem; text-align: left; }
			    th { background: #333; color: white; font-weight: 600; }
			    tr:nth-child(even) { background: #f9f9f9; }
			    .value { font-variant-numeric: tabular-nums; }
			    .tags { font-size: 0.85rem; color: #666; max-width: 280px; overflow: hidden; text-overflow: ellipsis; }
			    .meta { color: #666; font-size: 0.9rem; margin-bottom: 1rem; }
			    a { color: #0066cc; }
			    .refresh { margin-left: 1rem; }
			  </style>
			</head>
			<body>
			  <h1>Streams Metrics Dashboard</h1>
			  <p class="meta">Last updated: <span id="updated">-</span>
			    <span class="refresh"><label><input type="checkbox" id="autoRefresh" checked> Auto-refresh every 10s</label></span>
			    <span class="refresh"><label><input type="checkbox" id="includeJvm"> Include JVM metrics</label></span>
			  </p>
			  <h2>Kafka / Streams metrics</h2>
			  <div id="kafkaTable"></div>
			  <h2 id="jvmHeading" style="display:none;">JVM metrics</h2>
			  <div id="jvmTable"></div>
			  <h2>Links</h2>
			  <p><a href="/api/streams/topology/visual">Topology diagram</a> | <a href="/actuator/metrics">All actuator metrics</a></p>
			  <script>
			    function formatValue(v) {
			      if (v === undefined || v === null) return '-';
			      if (Number.isFinite(v)) return Number.isInteger(v) ? v : v.toFixed(4);
			      return String(v);
			    }
			    function renderTable(metrics, id) {
			      const el = document.getElementById(id);
			      if (!metrics || metrics.length === 0) { el.innerHTML = '<p>No metrics found.</p>'; return; }
			      let html = '<table><thead><tr><th>Metric</th><th>Value</th><th>Tags</th></tr></thead><tbody>';
			      metrics.forEach(m => {
			        const tags = m.tags && Object.keys(m.tags).length ? JSON.stringify(m.tags) : '';
			        html += '<tr><td>' + (m.name || '') + '</td><td class="value">' + formatValue(m.value) + '</td><td class="tags">' + tags + '</td></tr>';
			      });
			      html += '</tbody></table>';
			      el.innerHTML = html;
			    }
			    function fetchSummary() {
			      const jvm = document.getElementById('includeJvm').checked;
			      fetch('/api/streams/metrics/summary?kafka=true&jvm=' + jvm)
			        .then(r => r.json())
			        .then(data => {
			          document.getElementById('updated').textContent = data.refreshedAt || '-';
			          renderTable(data.kafka || [], 'kafkaTable');
			          renderTable(data.jvm || [], 'jvmTable');
			          document.getElementById('jvmHeading').style.display = jvm ? 'block' : 'none';
			        })
			        .catch(e => { document.getElementById('kafkaTable').innerHTML = '<p>Error loading metrics: ' + e.message + '</p>'; });
			    }
			    document.getElementById('includeJvm').addEventListener('change', fetchSummary);
			    fetchSummary();
			    setInterval(() => { if (document.getElementById('autoRefresh').checked) fetchSummary(); }, 10000);
			  </script>
			</body>
			</html>
			""";
		return ResponseEntity.ok(html);
	}

	private List<Map<String, Object>> collectMetrics(String namePrefix) {
		List<Map<String, Object>> out = new ArrayList<>();
		StreamSupport.stream(meterRegistry.getMeters().spliterator(), false)
				.filter(m -> m.getId().getName().startsWith(namePrefix))
				.sorted((a, b) -> a.getId().getName().compareTo(b.getId().getName()))
				.forEach(meter -> {
					double value = 0;
					for (Measurement meas : meter.measure()) {
						value += meas.getValue();
					}
					Map<String, Object> entry = new LinkedHashMap<>();
					entry.put("name", meter.getId().getName());
					entry.put("value", value);
					Map<String, String> tags = new LinkedHashMap<>();
					meter.getId().getTags().forEach(t -> tags.put(t.getKey(), t.getValue()));
					entry.put("tags", tags);
					out.add(entry);
				});
		return out;
	}
}
