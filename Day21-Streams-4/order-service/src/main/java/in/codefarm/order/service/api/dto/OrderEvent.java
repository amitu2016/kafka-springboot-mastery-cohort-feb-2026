package in.codefarm.order.service.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Order event for Day 4 stateful aggregations. Must have customerId, totalAmount; categoryId optional (default "default").
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
