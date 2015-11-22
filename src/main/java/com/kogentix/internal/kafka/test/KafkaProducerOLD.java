package com.kogentix.internal.kafka.test;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerOLD {

	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		String date = "04092014";
		// String topic = "my-replicated-topic" ;
		String topic = "javatopic";

		for (int i = 1; i <= 1000; i++) {
			String msg = date + " This is message " + i;
			System.out.println(msg);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(i), msg);
			producer.send(data);
		}
		producer.close();
	}
}