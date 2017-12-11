package com.kogentix.internal.kafka.test;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.kogentix.internal.kafka.test.KafkaPartitioner");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<String, String>(config);

    // String msg = "Hi I am Raktotpal";
    // KeyedMessage<String, String> data = new KeyedMessage<String, String>(
    // "rpal-01", msg);
    // producer.send(data);

    int i = 0;
    while (true) {
      String msg = "Hi I am Raktotpal";
      KeyedMessage<String, String> data = new KeyedMessage<String, String>("rpal-multi-part",
          msg + " ::: " + i);
      producer.send(data);
      i++;
      Thread.sleep(1000);
    }
  }
}