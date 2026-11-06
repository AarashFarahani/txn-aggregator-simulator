FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package
FROM apache/spark:4.0.1
COPY --from=build /src/target/transaction-aggregation-1.0.0.jar /opt/spark-app/app.jar
ENTRYPOINT ["/bin/bash","-lc","/opt/spark/bin/spark-submit --master ${SPARK_MASTER:-spark://spark-master:7077} --deploy-mode client --class com.example.spark.TransactionAggregationJob --conf spark.executor.instances=2 --conf spark.executor.cores=2 --conf spark.executor.memory=1500m --conf spark.driver.host=spark-job --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-app/app.jar"]
