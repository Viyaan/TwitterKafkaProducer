package com.viyaan.twitter;

import static com.viyaan.property.TwitterEnum.BROKER_LIST;

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
import com.viyaan.property.TwitterEnum;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * @author Viyaan
 *
 */
public class TwitterKafkaProducer_hbc {
	
	
    private static ProducerConfig getProducerConfig(PropertiesLoader propertyLoader) {
        Properties props = new Properties();
        props.put("metadata.broker.list", propertyLoader.getString(BROKER_LIST.getValue()));
        props.put("serializer.class", propertyLoader.getString(TwitterEnum.SERIALIZER.getValue()));
        props.put("request.required.acks", propertyLoader.getString(TwitterEnum.REQUIRED_ACKS.getValue()));
        return new ProducerConfig(props);
    }


	public static void run() throws Exception {
		
		PropertiesLoader loader = new PropertiesLoader();

		ProducerConfig config = getProducerConfig(loader);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				config);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Arrays.asList((loader.getString(TwitterEnum.HASH_TAGS.getValue()))));
		Authentication auth = new OAuth1(loader.getString(TwitterEnum.CONSUMER_KEY_KEY.getValue()),
				loader.getString(TwitterEnum.CONSUMER_SECRET_KEY.getValue()), loader.getString(TwitterEnum.ACCESS_TOKEN_KEY.getValue()),
						loader.getString(TwitterEnum.ACCESS_TOKEN_SECRET_KEY.getValue()));

		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();

		// Do whatever needs to be done with messages
		while(true) {
			KeyedMessage<String, String> message = null;
			try {
				
				message = new KeyedMessage<String, String>(loader.getString(TwitterEnum.KAFKA_TOPIC.getValue()), queue.take());
				System.out.println(message.message());
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}

	}

	public static void main(String[] args) {

		
			try {
				TwitterKafkaProducer_hbc.run();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
}
