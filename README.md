# KestirimProject
Direction estimation target finding project.
Linux Ubuntu Used , apache maven and kafka , Eclipse IDE used.
Needs kafka and maven , with commands below should executed before run.

Commands from kafka directory
to start zookeeper; 

bin/zookeeper-server-start.sh config/zookeeper.properties

to start server;

kafka-server-start.sh config/server.properties

//Creating topic

kafka-topics.sh --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --topic ftopic --create
