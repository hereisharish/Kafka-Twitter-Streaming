package com.twitter.stream;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamingKafkaProducer {

	// Creating a topic with below name
	private static final String topic = "Twitter-Data";

	public static void run(String consumerKey, String consumerSecret,
			String token, String secret) throws InterruptedException {

		// Set the Properties
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("client.id","TwitterStreamingPOC");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		// Configuring the Producer		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		//Setting the tracker to track
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		
		// Add the terms to track
		endpoint.trackTerms(Lists.newArrayList("twitterStreaming","#rpsvsrh"));

		//Passing all the keys here for Authentication Config
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,secret);

		// Create a new Client with passed Authentication
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Play with the messages below
		for (int msgRead = 0; msgRead < 500; msgRead++) {
			//printing the message before sending it into the topic*/
			System.out.println(queue.take());
			
			producer.send(new ProducerRecord<String, String>(topic, null, queue.take()));
		}
		
		// close all the connections
		producer.close();
		client.stop();
	}

	public static void main(String[] args) {
		try {
			String consumerKey = args[0];
			String consumerSecret = args[1];
			String token = args[2];
			String secret = args[3]; 
			TwitterStreamingKafkaProducer.run(consumerKey, consumerSecret, token, secret);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
