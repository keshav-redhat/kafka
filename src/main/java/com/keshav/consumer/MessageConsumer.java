package com.keshav.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MessageConsumer {
	public static void main(String[] args) {
		
		//create connection properties
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grp1_consumers");
		
		//create kafka consumer object
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		//subscribe to topic destination through message broker
		consumer.subscribe(Arrays.asList("keshav-tpc"));
		
		//perform polling to check and read the message
		while(true) {
			//poll and get consumer records
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			
			//read and display messages
			for(ConsumerRecord<String, String> record : records) {
				System.out.println("message is "+record.value());
			}
		}
	}

}
