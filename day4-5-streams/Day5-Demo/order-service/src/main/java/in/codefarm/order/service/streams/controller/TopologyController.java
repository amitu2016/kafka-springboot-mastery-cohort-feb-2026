package in.codefarm.order.service.streams.controller;

import in.codefarm.order.service.streams.TopologyMermaidGenerator;
import in.codefarm.order.service.streams.config.TopologyDescriptionHolder;
import org.apache.kafka.streams.TopologyDescription;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/streams")
public class TopologyController {

	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
	private final TopologyDescriptionHolder topologyDescriptionHolder;

	public TopologyController(StreamsBuilderFactoryBean streamsBuilderFactoryBean,
			TopologyDescriptionHolder topologyDescriptionHolder) {
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
		this.topologyDescriptionHolder = topologyDescriptionHolder;
	}

	@GetMapping("/topology")
	public ResponseEntity<Map<String, Object>> describeTopology() {
		TopologyDescription description = topologyDescriptionHolder.get();
		if (description == null) {
			return ResponseEntity.ok(Map.of(
					"status", "Streams not started or topology not yet built",
					"topologyDescription", "",
					"subtopologies", List.of()
			));
		}
		Map<String, Object> body = new HashMap<>();
		body.put("status", streamsBuilderFactoryBean.getKafkaStreams() != null ? "RUNNING" : "STARTING");
		body.put("applicationId", streamsBuilderFactoryBean.getStreamsConfiguration().getProperty("application.id"));
		body.put("topologyDescription", description.toString());
		body.put("mermaid", TopologyMermaidGenerator.toMermaid(description));
		body.put("subtopologies", description.subtopologies().stream()
				.map(st -> {
					Map<String, Object> sub = new HashMap<>();
					sub.put("id", st.id());
					sub.put("nodes", st.nodes().stream().map(TopologyDescription.Node::name).collect(Collectors.toList()));
					return sub;
				})
				.collect(Collectors.toList()));
		return ResponseEntity.ok(body);
	}

	@GetMapping(value = "/topology/mermaid", produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> topologyMermaid() {
		TopologyDescription description = topologyDescriptionHolder.get();
		return ResponseEntity.ok(description != null ? TopologyMermaidGenerator.toMermaid(description) : "Topology not available.");
	}

	@GetMapping(value = "/topology/visual", produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<String> topologyVisual() {
		TopologyDescription description = topologyDescriptionHolder.get();
		String mermaid = description != null ? TopologyMermaidGenerator.toMermaid(description) : "flowchart LR\n  empty[Topology not available]";
		String applicationId = streamsBuilderFactoryBean.getStreamsConfiguration().getProperty("application.id");
		String html = """
			<!DOCTYPE html>
			<html>
			<head>
			  <meta charset="UTF-8">
			  <title>Kafka Streams Topology - %s</title>
			  <script type="module">
			    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
			    mermaid.initialize({ startOnLoad: true, flowchart: { useMaxWidth: true } });
			  </script>
			</head>
			<body>
			  <h1>Topology: %s</h1>
			  <div class="mermaid">%s</div>
			  <p><a href="/api/streams/topology">JSON</a> | <a href="/api/streams/topology/mermaid">Mermaid source</a></p>
			</body>
			</html>
			""".formatted(applicationId, applicationId, mermaid.replace("\n", "\n  "));
		return ResponseEntity.ok(html);
	}
}
