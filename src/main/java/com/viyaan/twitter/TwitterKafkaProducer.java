package com.viyaan.twitter;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.viyaan.property.PropertiesLoader;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author Viyaan
 *
 */
public class TwitterKafkaProducer {


	public static void run() throws InterruptedException {

		Properties props = PropertiesLoader.getKafkaProperties();

		ProducerConfig producerConfig = new ProducerConfig(props);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Arrays.asList((props.get("twitter.hashtags").toString().split("\\s*,\\s*"))));
		Authentication auth = new OAuth1(props.getProperty("twitter.consumerkey"),
				props.getProperty("twitter.consumer.secret"), props.getProperty("twitter.token"),
				props.getProperty("twitter.token.secret"));

		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();

		// Do whatever needs to be done with messages
		while(true) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(props.getProperty("kafka.topic"), queue.take());
				System.out.println(message.message());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}

	}

	public static void main(String[] args) {

		try {
			TwitterKafkaProducer.run();
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
