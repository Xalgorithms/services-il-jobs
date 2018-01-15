#!/bin/bash
sbt package && sbt package && spark-submit --class "org.xalgorithms.jobs.$1" --packages datastax:spark-cassandra-connector:2.0.3-s_2.11,com.typesafe:config:1.3.1,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,net.ceedubs:ficus_2.11:1.1.1 target/scala-2.11/xa-spark-jobs_2.11-1.0.jar
