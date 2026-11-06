package com.example.simulator.service;

import com.example.simulator.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.math.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class SimulationService {
 private final KafkaTemplate<String,TransactionEvent> kafka; private final String topic;
 private final AtomicReference<String> state=new AtomicReference<>("IDLE");
 private final AtomicLong generated=new AtomicLong(),sent=new AtomicLong(),failed=new AtomicLong(),batches=new AtomicLong();
 private volatile long target,startedNanos; private volatile String startedAt=""; private volatile SimulationRequest config;
 private static final List<String> COUNTRIES=List.of("CA","US","GB","DE","AU","JP");
 private static final List<String> TYPES=List.of("PURCHASE","REFUND","TRANSFER","CASH_WITHDRAWAL");
 private static final List<String> MERCHANTS=List.of("MER-101","MER-102","MER-103","MER-104","MER-105");
 public SimulationService(KafkaTemplate<String,TransactionEvent> kafka,@Value("${app.kafka.raw-topic}") String topic){this.kafka=kafka;this.topic=topic;}
 public synchronized void start(SimulationRequest c){
  if(state.get().equals("RUNNING")||state.get().equals("PAUSED")) throw new IllegalStateException("Simulation already active");
  config=c;target=c.totalTransactions();generated.set(0);sent.set(0);failed.set(0);batches.set(0);
  startedNanos=System.nanoTime();startedAt=Instant.now().toString();state.set("RUNNING");generate();
 }
 @Async public void generate(){
  ExecutorService pool=Executors.newFixedThreadPool(config.producerConcurrency());
  try{
   while(generated.get()<target&&!state.get().equals("STOPPED")){
    while(state.get().equals("PAUSED"))Thread.sleep(100);
    if(state.get().equals("STOPPED"))break;
    int n=(int)Math.min(config.batchSize(),target-generated.get());CountDownLatch latch=new CountDownLatch(n);
    for(int i=0;i<n;i++){
     TransactionEvent e=event();generated.incrementAndGet();
     pool.submit(()->kafka.send(topic,e.transactionId(),e).whenComplete((r,x)->{if(x==null)sent.incrementAndGet();else failed.incrementAndGet();latch.countDown();}));
    }
    batches.incrementAndGet();latch.await();
    if(config.batchIntervalMs()>0)Thread.sleep(config.batchIntervalMs());
   }
   if(!state.get().equals("STOPPED"))state.set("COMPLETED");
  }catch(InterruptedException e){Thread.currentThread().interrupt();state.set("STOPPED");}
  finally{pool.shutdown();}
 }
 private TransactionEvent event(){
  var r=ThreadLocalRandom.current();var amount=BigDecimal.valueOf(r.nextDouble(1,10000)).setScale(2,RoundingMode.HALF_UP);
  return new TransactionEvent("TXN-"+UUID.randomUUID(),"ACC-"+r.nextInt(1000,9999),MERCHANTS.get(r.nextInt(MERCHANTS.size())),
   amount,"CAD",COUNTRIES.get(r.nextInt(COUNTRIES.size())),TYPES.get(r.nextInt(TYPES.size())),
   r.nextDouble()<.97?"APPROVED":"DECLINED",Instant.now());
 }
 public void pause(){state.compareAndSet("RUNNING","PAUSED");}
 public void resume(){state.compareAndSet("PAUSED","RUNNING");}
 public void stop(){state.set("STOPPED");}
 public Map<String,Object> status(){
  double secs=Math.max(.001,(System.nanoTime()-startedNanos)/1e9);
  return Map.of("state",state.get(),"target",target,"generated",generated.get(),"successful",sent.get(),
   "failed",failed.get(),"batches",batches.get(),"transactionsPerSecond",sent.get()/secs,"startedAt",startedAt);
 }
}
