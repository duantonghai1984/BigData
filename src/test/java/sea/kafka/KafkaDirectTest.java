package sea.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class KafkaDirectTest {
	
	
	//http://orchome.com/451

	public void produce() {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConsumerConfig.host);
		props.put("acks", "-1");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<>(KafkaConsumerConfig.topic, Integer.toString(i), Integer.toString(i)));
		}

		producer.close();
	}

	public void consumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConsumerConfig.host);
		props.put("group.id", KafkaConsumerConfig.group);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(KafkaConsumerConfig.topic));
		while (true) {
			System.out.println("poll");
			ConsumerRecords<String, String> records = consumer.poll(100);
			//for (ConsumerRecord<String, String> record : records)
				//System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
			
			// consumer.commitSync(); //同步offset
		}
		
	}
	
	
	//自己控制offset
	public void consumer2() {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConsumerConfig.host);
		props.put("group.id", KafkaConsumerConfig.group);
		props.put("enable.auto.commit", "false");   //change
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(KafkaConsumerConfig.topic));
		
		try {
	         while(true) {
	             ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
	             for (TopicPartition partition : records.partitions()) {
	                 List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
	                 for (ConsumerRecord<String, String> record : partitionRecords) {
	                     System.out.println(record.offset() + ": " + record.value());
	                 }
	                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
	                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
	             }
	         }
	     } finally {
	       consumer.close();
	     }
	}

	/*
	 * 
	 * 使用SimpleConsumer的步骤：
	 * 
	  Find an active Broker and find out which Broker is the leader for your
	  topic and partition Determine who the replica Brokers are for your topic
	  and partition Build the request defining what data you are interested in
	  Fetch the data Identify and recover from leader changes
	  
	  首先，你必须知道读哪个topic的哪个partition 
然后，找到负责该partition的broker leader，从而找到存有该partition副本的那个broker 
再者，自己去写request并fetch数据 
最终，还要注意需要识别和处理broker leader的改变
	 * 
	 */
	public void consumerMutiThread(int threadnum) {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConsumerConfig.host);
		props.put("group.id", KafkaConsumerConfig.group);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(KafkaConsumerConfig.topic, new Integer(threadnum));

		// Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
		// consumer.
	}
	
	
	

	public void consumerLow() {

	}
	
	public void consumerStream(){
	
	}
	
	
	public class KafkaConsumerRunner implements Runnable {
	     private final AtomicBoolean closed = new AtomicBoolean(false);
	     private  KafkaConsumer consumer;

	     public void run() {
	         try {
	             consumer.subscribe(Arrays.asList("topic"));
	             while (!closed.get()) {
	                 ConsumerRecords records = consumer.poll(10000);
	                 // Handle new records
	             }
	         } catch (WakeupException e) {
	             // Ignore exception if closing
	             if (!closed.get()) throw e;
	         } finally {
	             consumer.close();
	         }
	     }

	     // Shutdown hook which can be called from a separate thread
	     public void shutdown() {
	         closed.set(true);
	         consumer.wakeup();
	     }
	 }

	public static void main(String[] args) {
		KafkaDirectTest test = new KafkaDirectTest();
		test.produce();
		test.consumer2();

	}

}
