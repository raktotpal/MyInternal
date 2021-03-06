package com.kogentix.internal.kafka.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer {

  public static void main(String[] args) {
    // String group = args[0] ;

    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "test-consumer-group");
    props.put("zookeeper.session.timeout.ms", "413");
    props.put("zookeeper.sync.time.ms", "203");
    props.put("auto.commit.interval.ms", "1000");
    // props.put("auto.offset.reset", "smallest");

    ConsumerConfig cf = new ConsumerConfig(props);

    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(cf);

    String topic = "rpal-01";

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
        .createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    KafkaStream<byte[], byte[]> stream = streams.get(0);

    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    int i = 1;
    while (it.hasNext()) {
      System.out.println(i + ": " + new String(it.next().message()));
      ++i;
    }

    consumer.shutdown();
  }
}
