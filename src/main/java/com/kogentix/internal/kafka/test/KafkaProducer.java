package com.kogentix.internal.kafka.test;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.kogentix.internal.kafka.test.KafkaPartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

			String msg = "";
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"page_visits", msg);
			producer.send(data);

		producer.close();
	}
}