package com.example.consumer.service;
import com.example.consumer.model.AggregateMessage;import com.fasterxml.jackson.databind.ObjectMapper;
import io.projectreactor.kafka.receiver.KafkaReceiver;import jakarta.annotation.PostConstruct;import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;import reactor.core.scheduler.Schedulers;
@Component public class AggregateConsumer{
 private final KafkaReceiver<String,String> receiver;private final AggregateRepository repo;private final ObjectMapper mapper;
 public AggregateConsumer(KafkaReceiver<String,String> r,AggregateRepository repo,ObjectMapper mapper){this.receiver=r;this.repo=repo;this.mapper=mapper;}
 @PostConstruct public void start(){
  receiver.receive().concatMap(record->Mono.fromCallable(()->mapper.findAndRegisterModules().readValue(record.value(),AggregateMessage.class))
   .subscribeOn(Schedulers.parallel()).flatMap(repo::upsert).then(record.receiverOffset().commit())
   .doOnError(e->System.err.println("Persistence failed; offset not committed: "+e)))
   .doOnError(e->System.err.println("Kafka receive stream failed: "+e)).retry().subscribe();
 }
}
