package com.example.consumer.model;
import java.math.BigDecimal;import java.time.OffsetDateTime;
public record AggregateMessage(String aggregationId,OffsetDateTime windowStart,OffsetDateTime windowEnd,
 String aggregationType,String dimensionType,String dimensionValue,long transactionCount,BigDecimal totalAmount,
 BigDecimal averageAmount,BigDecimal minAmount,BigDecimal maxAmount,long approvedCount,long declinedCount){}
