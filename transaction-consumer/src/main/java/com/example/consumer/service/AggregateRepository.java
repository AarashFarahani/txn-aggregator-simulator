package com.example.consumer.service;
import com.example.consumer.model.AggregateMessage;import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;import reactor.core.publisher.Mono;
@Repository public class AggregateRepository{
 private final DatabaseClient db;public AggregateRepository(DatabaseClient db){this.db=db;}
 public Mono<Void> upsert(AggregateMessage a){
  return db.sql("INSERT INTO transaction_aggregates(aggregation_id,window_start,window_end,aggregation_type,dimension_type,dimension_value,transaction_count,total_amount,average_amount,min_amount,max_amount,approved_count,declined_count) VALUES(:id,:ws,:we,:type,:dtype,:dv,:cnt,:total,:avg,:min,:max,:ok,:no) ON CONFLICT(aggregation_id) DO UPDATE SET transaction_count=EXCLUDED.transaction_count,total_amount=EXCLUDED.total_amount,average_amount=EXCLUDED.average_amount,min_amount=EXCLUDED.min_amount,max_amount=EXCLUDED.max_amount,approved_count=EXCLUDED.approved_count,declined_count=EXCLUDED.declined_count")
   .bind("id",a.aggregationId()).bind("ws",a.windowStart()).bind("we",a.windowEnd()).bind("type",a.aggregationType())
   .bind("dtype",a.dimensionType()).bind("dv",a.dimensionValue()).bind("cnt",a.transactionCount()).bind("total",a.totalAmount())
   .bind("avg",a.averageAmount()).bind("min",a.minAmount()).bind("max",a.maxAmount()).bind("ok",a.approvedCount())
   .bind("no",a.declinedCount()).fetch().rowsUpdated().then();
 }
}
