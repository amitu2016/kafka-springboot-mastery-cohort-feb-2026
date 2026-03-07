package in.codefarm.order.service.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Order event for Day 5 join/enrichment. Must have customerId, totalAmount; orderId and categoryId optional.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderEvent(
		String orderId,
		String customerId,
		Double totalAmount,
		String status,
		String categoryId
) {
	public boolean isValid() {
		return customerId != null && !customerId.isBlank()
				&& totalAmount != null && totalAmount >= 0;
	}

	/** Category for count-per-category; default if missing. */
	public String effectiveCategoryId() {
		return (categoryId != null && !categoryId.isBlank()) ? categoryId : "default";
	}
}
