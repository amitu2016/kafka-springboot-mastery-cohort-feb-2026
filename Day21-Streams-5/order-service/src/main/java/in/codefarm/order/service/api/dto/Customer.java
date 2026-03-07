package in.codefarm.order.service.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Customer profile for Day 5 stream-table join. Topic "customers" is keyed by customerId; value is this JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Customer(
		String customerId,
		String name,
		String tier
) {
	public static Customer unknown() {
		return new Customer(null, "unknown", "-");
	}
}
