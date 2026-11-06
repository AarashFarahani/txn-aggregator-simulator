package com.example.simulator.model;
import java.math.BigDecimal;
import java.time.Instant;
public record TransactionEvent(String transactionId,String accountId,String merchantId,BigDecimal amount,
 String currency,String country,String transactionType,String status,Instant eventTime) {}
