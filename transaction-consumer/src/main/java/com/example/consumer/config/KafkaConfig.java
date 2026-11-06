package com.example.consumer.config;
import io.projectreactor.kafka.receiver.*;import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;import java.util.*;
@Configuration public class KafkaConfig{
 @Bean ReceiverOptions<String,String> receiverOptions(@Value("${app.kafka.bootstrap-servers}")String brokers,
 @Value("${app.kafka.aggregated-topic}")String topic,@Value("${app.kafka.group-id}")String group){
  Map<String,Object> p=new HashMap<>();p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);p.put(ConsumerConfig.GROUP_ID_CONFIG,group);
  p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
  p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
  p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500);
  return ReceiverOptions.<String,String>create(p).subscription(List.of(topic));
 }
 @Bean KafkaReceiver<String,String> receiver(ReceiverOptions<String,String> o){return KafkaReceiver.create(o);}
}
