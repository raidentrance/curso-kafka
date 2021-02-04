package com.devs4j.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Devs4jProducer {

	public static final Logger log = LoggerFactory.getLogger(Devs4jProducer.class);

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // Broker
		props.put("acks", "all"); //
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");
		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			int id = 1;
			for (int i = 0; i < 2; i++) {// 758136139
				producer.send(new ProducerRecord<String, String>("schedule-export", "1",String.format("%d", id)));
				if (i % 1000000 == 0) {
					producer.flush();
				}
				id++;
			}
			producer.flush();
			producer.close();
		}

		log.info("Processing time = {} ms ", (System.currentTimeMillis() - startTime));
	}
}
