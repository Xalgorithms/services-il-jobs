#!/bin/bash
echo "> attempting: $1 (version: $2)"
dcos spark run --name "spark" --submit-args="--driver-cores=0.5 --driver-memory=1024M --class org.xalgorithms.jobs.$1 --conf spark.jars.packages=datastax:spark-cassandra-connector:2.0.3-s_2.11,com.typesafe:config:1.3.1,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,net.ceedubs:ficus_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 --conf spark.cassandra.connection.host=node-0-server.cassandra.autoip.dcos.thisdcos.directory https://github.com/Xalgorithms/xadf-jobs/releases/download/v$2/xa-spark-jobs_2.11-1.0.jar"
