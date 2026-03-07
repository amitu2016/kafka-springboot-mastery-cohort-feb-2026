package in.codefarm.order.service.streams.service;

import org.apache.kafka.streams.TopologyDescription;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts a Kafka Streams TopologyDescription to Mermaid flowchart syntax for visualization.
 */
public final class TopologyMermaidGenerator {

	private TopologyMermaidGenerator() {
	}

	/**
	 * Generates Mermaid flowchart diagram from the topology description.
	 * Sub-topologies become subgraphs; nodes and edges are derived from predecessors/successors.
	 */
	public static String toMermaid(TopologyDescription description) {
		if (description == null) {
			return "flowchart LR\n  empty[Topology not available]";
		}

		StringBuilder sb = new StringBuilder();
		sb.append("flowchart LR\n");

		for (TopologyDescription.Subtopology sub : description.subtopologies().stream().sorted((a, b) -> Integer.compare(a.id(), b.id())).toList()) {
			sb.append("  subgraph sub").append(sub.id()).append("[\"Sub-topology ").append(sub.id()).append("\"]\n");
			Map<String, String> nodeIds = new LinkedHashMap<>();
			int idx = 0;
			for (TopologyDescription.Node node : sub.nodes().stream().sorted((a, b) -> a.name().compareTo(b.name())).toList()) {
				String id = "n" + sub.id() + "_" + idx++;
				nodeIds.put(node.name(), id);
			}
			for (Map.Entry<String, String> e : nodeIds.entrySet()) {
				String label = e.getKey().replace("\"", "'");
				sb.append("    ").append(e.getValue()).append("[\"").append(label).append("\"]\n");
			}
			for (TopologyDescription.Node node : sub.nodes()) {
				String fromId = nodeIds.get(node.name());
				if (fromId == null) continue;
				for (TopologyDescription.Node succ : node.successors()) {
					String toId = nodeIds.get(succ.name());
					if (toId != null) {
						sb.append("    ").append(fromId).append(" --> ").append(toId).append("\n");
					}
				}
			}
			sb.append("  end\n");
		}

		return sb.toString();
	}
}
