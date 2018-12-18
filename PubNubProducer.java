package com.Pubnub.PubNub;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.google.common.collect.Lists;
import com.Pubnub.PubNub.*;
import com.google.gson.JsonElement;
public class PubNubProducer {

	private static final String topic = "you-topic-name";
    public static  String jobj = null ;
	public static void run() throws InterruptedException {
		 Properties props = new Properties();
		    props.put("bootstrap.servers", "localhost:9092");
		    props.put("acks", "all");
		    props.put("retries", 0);
		    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");		
 KafkaProducer<String, String> producernew = new KafkaProducer<String, String>(props);
 ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, null, jobj);		
 producernew.send(producerRecord);
			producernew.close();	

	}
	public static void main(String[] args) {
		try {
			PubNubProducer.run();
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
