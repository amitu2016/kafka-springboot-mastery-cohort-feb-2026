package in.codefarm.order.service.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Request body for placing an order. Used by REST API and sent as JSON to the orders topic.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderRequest(
		String orderId,
		String customerId,
		Double totalAmount,
		String status,
		String scenario
) {
	/** Scenario: valid order, normal amount */
	public static final String SCENARIO_VALID = "valid";
	/** Scenario: high-value order (e.g. amount >= 1000) for branch() demo */
	public static final String SCENARIO_HIGH_VALUE = "high-value";
	/** Scenario: normal value order */
	public static final String SCENARIO_NORMAL = "normal";
	/** Scenario: send malformed payload to topic to test stream error handling */
	public static final String SCENARIO_MALFORMED = "malformed";
}
