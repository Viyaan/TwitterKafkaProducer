package com.viyaan.property;

/**
 * Created by viyaan
 */
public enum TwitterEnum {


    CONSUMER_KEY_KEY("twitter.consumerkey"),
    CONSUMER_SECRET_KEY("twitter.consumer.secret"),
    ACCESS_TOKEN_KEY("twitter.access.token"),
    ACCESS_TOKEN_SECRET_KEY("twitter.token.secret"),

    BATCH_SIZE_KEY("batchSize"),
    DEFAULT_BATCH_SIZE("1000L"),

    BROKER_LIST("metadata.broker.list"),
    SERIALIZER("serializer.class"),
    REQUIRED_ACKS("request.required.acks"),
    KAFKA_TOPIC("kafka.topic"),
	
	HASH_TAGS("twitter.hashtags");

    private String value;

    TwitterEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
