package com.kafka.twitter.apis;

import static com.kafka.twitter.utils.TwitterEnum.BROKER_LIST;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.twitter.utils.PropertiesLoader;
import com.kafka.twitter.utils.TwitterEnum;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


/**
 * Created by Viyaan
 */
public class TwitterKafkaProducer_twitter4j {

    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaProducer_twitter4j.class);

    private TwitterStream twitterStream;

    private ProducerConfig getProducerConfig(PropertiesLoader PropertiesLoader) {
        Properties props = new Properties();
        props.put("metadata.broker.list", PropertiesLoader.getString(BROKER_LIST.getValue()));
        props.put("serializer.class", PropertiesLoader.getString(TwitterEnum.SERIALIZER.getValue()));
        props.put("request.required.acks", PropertiesLoader.getString(TwitterEnum.REQUIRED_ACKS.getValue()));
        return new ProducerConfig(props);
    }

    private void startTwitterStream(final PropertiesLoader propertiesLoader) {
        ProducerConfig config = getProducerConfig(propertiesLoader);
        final Producer<String, String> producer = new Producer(config);
        ConfigurationBuilder cb = getTwitterConfiguration(propertiesLoader);
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        FilterQuery tweetFilterQuery = new FilterQuery(); // See 
        tweetFilterQuery.track(propertiesLoader.getString(TwitterEnum.HASH_TAGS.getValue()));
         // Filter based on keywords
        StatusListener listener = getTwitterStatusListener(propertiesLoader, producer);
        /** Binds the listener **/
        twitterStream.addListener(listener);
        twitterStream.filter(tweetFilterQuery);
        /** Starts listening on random sample of all public statuses. **/
        twitterStream.sample();
    }

    private StatusListener getTwitterStatusListener(final PropertiesLoader PropertiesLoader, final Producer<String, String> producer) {
        /** Twitter listener **/
        return new StatusListener() {
            // The onStatus method is executed every time a new tweet comes
            public void onStatus(Status status) {
                // The EventBuilder is used to build an event using the raw JSON of a tweet
                //logger.info(status.getUser().getScreenName() + ": " + status.getText());
                KeyedMessage<String, String> data = new KeyedMessage(PropertiesLoader.getString(TwitterEnum.KAFKA_TOPIC.getValue())
                        , TwitterObjectFactory.getRawJSON(status));
                producer.send(data);
                System.out.println(status.getText());
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onScrubGeo(long userId, long upToStatusId) {
            }

            public void onException(Exception ex) {
                logger.info("Shutting down Twitter sample stream...");
                twitterStream.shutdown();
            }

            public void onStallWarning(StallWarning warning) {
            }
        };
    }

    private ConfigurationBuilder getTwitterConfiguration(PropertiesLoader PropertiesLoader) {
        //get keys from apps.twitter.com
        String consumerKey = PropertiesLoader.getString(TwitterEnum.CONSUMER_KEY_KEY.getValue());
        String consumerSecret = PropertiesLoader.getString(TwitterEnum.CONSUMER_SECRET_KEY.getValue());
        String accessToken = PropertiesLoader.getString(TwitterEnum.ACCESS_TOKEN_KEY.getValue());
        String accessTokenSecret = PropertiesLoader.getString(TwitterEnum.ACCESS_TOKEN_SECRET_KEY.getValue());
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);
        return cb;
    }


    public static void main(String[] args) {
        try {
        	PropertiesLoader PropertiesLoader = new PropertiesLoader();
            TwitterKafkaProducer_twitter4j tp = new TwitterKafkaProducer_twitter4j();
            tp.startTwitterStream(PropertiesLoader);
            System.out.println("Fetching twitter messages...........");
        } catch (Exception e) {
            logger.info(e.getMessage());
        }

    }
}
