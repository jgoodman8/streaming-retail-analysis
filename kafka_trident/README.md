# Streaming retail analysis with Apache Storm Trident

## Requierements

- Java 8
- Gradle
- Apache Kafka
- SBT ([check instalation tutorial](https://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html))

## Setup

The *Kafka* topic named 'compras-trident', where the simulator will write, should be created before launch the application.

A topic may be created using the next command:

```
ssh root@node1
kafka-topics.sh —create —topic <topic-name> —partitions 1 —replication 1 —zookeeper <zookeeper-host:port>
```

To check the created topics, the next command may be used:

```
kafka-topics.sh --list --zookeeper <zookeeper-host:port>
```

Also, is needed to remove the csv header from the data source.

```
tail -n +2 resources/online_retail.csv > resources/retail.csv
```

## Build

First, its is needed to compile the generator located at the `spark_straming` folder.

```
sbt assembly
```

Then, we may build this application code:

```
gradle clean build
```

### Run

The application takes as input a list of countries (separated by commas) which will be used as a filter for results. Also, it takes as second argument, the number of iterations (1 per second) the application will perform. An example is provied bellow:

```
java -jar purchases_trident_kafka.jar "France,Portugal,United Kingdom" 10000
```

As output, the application should show by console the top five clients with more purchases and cancelations volume. This lists will be updated every second. Also, the total volumen cancelated/purchased by each client will be showed.
