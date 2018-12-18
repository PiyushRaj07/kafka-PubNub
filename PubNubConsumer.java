package com.Pubnub.PubNub;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import java.util.Properties;
import java.util.Arrays;
public class PubNubConsumer {

	 //private final static String TOPIC = "Test-topic";
	    private final static String TOPIC = "kafkatest1";
	    private final static String BOOTSTRAP_SERVERS =
	    "localhost:9092,localhost:9093,localhost:9094";	
	    private static Consumer<Long, String> createConsumer() {
		
		
	      final Properties props = new Properties();
	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	                                  BOOTSTRAP_SERVERS);
	      props.put(ConsumerConfig.GROUP_ID_CONFIG,
	                                  "KafkaExampleConsumer");
	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	              LongDeserializer.class.getName());
	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	        StringDeserializer.class.getName());
		      
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	      
	      consumer.subscribe(Arrays.asList(TOPIC));
	      System.out.println("Subscribed to topic " + TOPIC);
	      int i = 0;
	     // SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Demo");
	    //  JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));


	        System.out.println("Piyush");
	    //  JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkBroker,"CustomerKafkaConsumerThread", topicmap,StorageLevel.MEMORY_ONLY());
	        

	      
	      while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	            for (ConsumerRecord<String, String> record : records)
	               System.out.printf("offset = %d, key = %s, value = %s\n", 
	               record.offset(), record.key(), record.value());	  
	         //   record.value().
	            System.out.println("Subscribed to topicccccccccccccccccccccccccccccccccccc ");
	      } 

	     
	     
	  }
	 public static void main(String args[])
	 {
		 PubNubConsumer obj = new PubNubConsumer();
		 obj.createConsumer();
	 }
	 
}
