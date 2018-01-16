#!/bin/bash
kafka-topics --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 1 --topic xadf.compute.documents
kafka-topics --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 1 --topic xadf.compute.effective
kafka-topics --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 1 --topic xadf.compute.applicable
