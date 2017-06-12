package com.kogentix.internal.kafka.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaMultiThreadedConsumer {

  public static class KafkaPartitionConsumer implements Runnable {

    private int tnum;
    private KafkaStream<byte[], byte[]> kfs;

    public KafkaPartitionConsumer(int id, KafkaStream<byte[], byte[]> ks) {
      tnum = id;
      kfs = ks;
    }

    public void run() {
      System.out.println("This is thread - " + tnum);

      ConsumerIterator<byte[], byte[]> it = kfs.iterator();
      while (it.hasNext()) {
        System.out.println(Thread.currentThread().getName() + " ::: " + tnum
            + new String(it.next().message()));
      }
    }
  }

  public static class MultiKafka {
    public void run() {
    }
  }

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "test-consumer-group");
    props.put("zookeeper.session.timeout.ms", "413");
    props.put("zookeeper.sync.time.ms", "203");
    props.put("auto.commit.interval.ms", "1000");
    // props.put("auto.offset.reset", "smallest");

    ConsumerConfig cf = new ConsumerConfig(props);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(cf);

    String topic = "rpal-multi-part";

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(3));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
        .createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    ExecutorService executor = Executors.newFixedThreadPool(3);

    int threadnum = 0;

    for (KafkaStream<byte[], byte[]> stream : streams) {
      executor.execute(new KafkaPartitionConsumer(threadnum, stream));
      ++threadnum;
    }
    // consumer.shutdown();

    // for (KafkaStream<byte[], byte[]> stream : streams) {
    // ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
    // threadnum++;
    //
    // while (consumerIte.hasNext())
    // System.out.println("Message from thread :: " + threadnum + " -- "
    // + new String(consumerIte.next().message()));
    // // executor.execute(new KafkaPartitionConsumer(threadnum, stream));
    // // ++threadnum;
    // }
  }
}