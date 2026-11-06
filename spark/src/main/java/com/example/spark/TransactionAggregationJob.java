package com.example.spark;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class TransactionAggregationJob {
 public static void main(String[] args)throws Exception{
  String brokers=System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092");
  String input=System.getenv().getOrDefault("RAW_TOPIC","transactions.raw");
  String output=System.getenv().getOrDefault("AGGREGATED_TOPIC","transactions.aggregated");
  String checkpoint=System.getenv().getOrDefault("CHECKPOINT_DIR","/tmp/aggregation");
  SparkSession spark=SparkSession.builder().appName("transaction-window-aggregation").getOrCreate();
  spark.sparkContext().setLogLevel("WARN");
  StructType schema=new StructType().add("transactionId",DataTypes.StringType).add("accountId",DataTypes.StringType)
   .add("merchantId",DataTypes.StringType).add("amount",DataTypes.createDecimalType(20,2))
   .add("currency",DataTypes.StringType).add("country",DataTypes.StringType)
   .add("transactionType",DataTypes.StringType).add("status",DataTypes.StringType).add("eventTime",DataTypes.TimestampType);
  Dataset<Row> kafka=spark.readStream().format("kafka").option("kafka.bootstrap.servers",brokers)
   .option("subscribe",input).option("startingOffsets","earliest").option("failOnDataLoss","false").load();
  Dataset<Row> tx=kafka.select(from_json(col("value").cast("string"),schema).alias("t")).select("t.*")
   .filter(col("transactionId").isNotNull()).withWatermark("eventTime","10 seconds");
  Dataset<Row> all=aggregate(tx,"country","COUNTRY").unionByName(aggregate(tx,"merchantId","MERCHANT"))
   .unionByName(aggregate(tx,"transactionType","TRANSACTION_TYPE"));
  Dataset<Row> out=all.select(col("aggregationId").alias("key"),to_json(struct(
   col("aggregationId"),col("windowStart"),col("windowEnd"),col("aggregationType"),col("dimensionType"),
   col("dimensionValue"),col("transactionCount"),col("totalAmount"),col("averageAmount"),col("minAmount"),
   col("maxAmount"),col("approvedCount"),col("declinedCount"))).alias("value"));
  out.writeStream().format("kafka").option("kafka.bootstrap.servers",brokers).option("topic",output)
   .option("checkpointLocation",checkpoint).outputMode("append").start().awaitTermination();
 }
 static Dataset<Row> aggregate(Dataset<Row> tx,String dimension,String type){
  Dataset<Row> g=tx.groupBy(window(col("eventTime"),"1 minute").alias("w"),col(dimension).alias("dimensionValue"))
   .agg(count(lit(1)).alias("transactionCount"),sum("amount").alias("totalAmount"),avg("amount").alias("averageAmount"),
    min("amount").alias("minAmount"),max("amount").alias("maxAmount"),
    sum(when(upper(col("status")).equalTo("APPROVED"),1).otherwise(0)).alias("approvedCount"),
    sum(when(upper(col("status")).equalTo("DECLINED"),1).otherwise(0)).alias("declinedCount"));
  Column start=col("w.start").cast("string"),end=col("w.end").cast("string");
  return g.select(sha2(concat_ws("|",lit(type),start,end,col("dimensionValue")),256).alias("aggregationId"),
   start.alias("windowStart"),end.alias("windowEnd"),lit(type).alias("aggregationType"),lit(type).alias("dimensionType"),
   coalesce(col("dimensionValue"),lit("UNKNOWN")).alias("dimensionValue"),col("transactionCount"),
   round(col("totalAmount"),2).alias("totalAmount"),round(col("averageAmount"),2).alias("averageAmount"),
   round(col("minAmount"),2).alias("minAmount"),round(col("maxAmount"),2).alias("maxAmount"),
   col("approvedCount"),col("declinedCount"));
 }
}
