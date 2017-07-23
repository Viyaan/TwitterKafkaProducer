# Twitter Kafka Producer

This project is used to get live feeds from Twitter and push it to Kafka Topic.

## Getting Started

Run TwitterKafkaProducer_hbc or
Run TwitterKafkaProducer_twitter4j

Both these classes sever the same purpose but uses different library to serve the purpose.

### Prerequisites

Install and Run Zookeeper and Kafka
Create Topic

### Installing


Start Zookeeper:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Start Kafka:
.\bin\windows\kafka-server-start.bat .\config\server.properties


Create topic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter-topic


End with an example of getting some data out of the system or using it for a little demo

## Running the tests





## Deployment


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Twitter4J](http://twitter4j.org/en/) - JTwitter4J is an unofficial Java library for the Twitter API.
* [HorseBirdClient](https://github.com/twitter/hbc) - A Java HTTP client for consuming Twitter's Streaming API

## Contributing


## Versioning



## Authors

* **Viyaan Jhiingade** - *Initial work* - [Viyaan](https://github.com/Viyaan)



## License



## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc
