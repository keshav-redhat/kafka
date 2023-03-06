package com.keshav.producer;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageProducerApp {
    public static void main(String[] args) {
    	//create connection properties as k=v pairs in java.util.properties class obj
    	Properties properties = new Properties();
    	properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    	properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        //create kafka producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        //create producer record object representing the message
        String msg = "Welcome to kafka";
        String topicName = "keshav-tpc";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, msg);
        
        //send message
        producer.send(record);
        
        //flush the message
        producer.flush();
        
        //close the connection with bootstrap-server
        producer.close();
	}
}
