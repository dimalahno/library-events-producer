## Start up the Zookeeper.

zookeeper-server-start.bat ..\..\config\zookeeper.properties

## Start up the Kafka Broker.

kafka-server-start.bat ..\..\config\server.properties

kafka-server-start.bat ..\..\config\server-1.properties

kafka-server-start.bat ..\..\config\server-2.properties

cd ..\..\kafka_2.12-3.3.1\bin\windows

kafka-topics.bat --create --topic test-topic-replicated -zookeeper localhost:2181 --replication-factor 3 --partitions 3

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic library-events