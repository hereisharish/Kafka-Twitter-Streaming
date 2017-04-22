package com.twitter.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaConsumerForStreaming {
	private final KafkaConsumer<String,String> consumer;
	private final String topic;
	private ExecutorService executor;
	private long delay;


	public KafkaConsumerForStreaming(Properties props, String topic) {
		consumer = new KafkaConsumer<String, String>(props);
		this.topic = topic;
	}

	public void shutdown() {
		if (consumer != null)
			consumer.close();
		if (executor != null)
			executor.shutdown();
	}

	public void run() {
		consumer.subscribe(Collections.singletonList(this.topic));

			while(true) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(1000);
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("Received message: (" + ", " + record.value() + ") at offset " + record.offset());
					}
				}catch(Exception ex) {
					ex.printStackTrace();
				}
			}

	}


	private static Properties createConsumerConfig(String brokers, String groupId) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		//props.put("auto.commit.enable", "false");
		
		return props;
	}

	public static void main(String[] args) throws InterruptedException {
		//String brokers = args[0];
		String brokers = "localhost:9092";
		//String groupId = args[1];
		String groupId = "group1";
		//String topic = args[2];
		String topic = "Twitter-Data";
		Properties props = createConsumerConfig(brokers, groupId);
		KafkaConsumerForStreaming example = new KafkaConsumerForStreaming(props, "Twitter-Data");
		example.run();

		//Thread.sleep(24*60*60*1000);
		
		example.shutdown();
	}
}