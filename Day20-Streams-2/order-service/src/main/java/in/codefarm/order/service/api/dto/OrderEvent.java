package in.codefarm.order.service.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Parsed order event (used in streams). Must be parseable from JSON in the topology.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderEvent(
		String orderId,
		String customerId,
		Double totalAmount,
		String status
) {
	public boolean isValid() {
		return orderId != null && !orderId.isBlank()
				&& customerId != null && !customerId.isBlank()
				&& totalAmount != null && totalAmount >= 0;
	}

	public boolean isHighValue(double threshold) {
		return totalAmount != null && totalAmount >= threshold;
	}
}
