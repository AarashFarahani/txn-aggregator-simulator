package com.example.simulator.model;
import jakarta.validation.constraints.*;
public record SimulationRequest(@Min(1) @Max(10000000) long totalTransactions,
 @Min(1) @Max(100000) int batchSize,@Min(1) @Max(32) int producerConcurrency,
 @Min(0) @Max(60000) long batchIntervalMs) {}
