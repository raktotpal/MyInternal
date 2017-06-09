package com.kogentix.internal.kafka.MultiThreadedConsumerPartition;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerTest {

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1");

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
    // String msg = (String) args[0];
    for (int i = 0;; i++) {
      System.out.println("in loop");
      String msg = "Trying Producer in" + i + System.currentTimeMillis();
      producer.send(new ProducerRecord<String, String>("rpal-multi-part", msg));
      TimeUnit.SECONDS.sleep(1);
    }
    // producer.close();

  }

}
