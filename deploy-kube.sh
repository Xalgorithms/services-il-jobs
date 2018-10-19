#!/bin/bash
#echo "> forwarding from kubernetes"
#kubectl port-forward svc/spark-master 17077:7077
#KUBE_PID=$!

echo "> attempting: $1"
OPTS=`cat $1.opts.production`
echo ">   additional opts: $OPTS"

# effective
DEPS="https://dl.bintray.com/spark-packages/maven/datastax/spark-cassandra-connector/2.3.2-s_2.11/spark-cassandra-connector-2.3.2-s_2.11.jar,http://central.maven.org/maven2/com/typesafe/config/1.3.1/config-1.3.1.jar,http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.11/2.3.0/spark-streaming-kafka-0-10_2.11-2.3.0.jar,http://central.maven.org/maven2/net/ceedubs/ficus_2.11/1.1.1/ficus_2.11-1.1.1.jar,http://central.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.11/2.3.1/mongo-spark-connector_2.11-2.3.1.jar,http://central.maven.org/maven2/org/gnieh/diffson-play-json_2.11/2.2.1/diffson-play-json_2.11-2.2.1.jar,http://central.maven.org/maven2/org/apache/kafka/kafka-clients/2.0.0/kafka-clients-2.0.0.jar,http://central.maven.org/maven2/com/typesafe/play/play-json_2.11/2.6.10/play-json_2.11-2.6.10.jar,http://central.maven.org/maven2/com/typesafe/play/play-functional_2.11/2.6.10/play-functional_2.11-2.6.10.jar"

#applicable
DEPS="$DEPS,http://central.maven.org/maven2/org/mongodb/mongo-java-driver/3.4.2/mongo-java-driver-3.4.2.jar"

echo $DEPS

spark-submit --class "org.xalgorithms.jobs.$1" --deploy-mode cluster --master "k8s://https://35.196.30.223" --conf spark.kubernetes.container.image=gcr.io/cloud-solutions-images/spark:v2.3.0-gcs --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark --jars $DEPS $OPTS https://github.com/Xalgorithms/services-il-jobs/releases/download/v0.1.2/jobs-spark-il_2.11-0.1.2.jar

#echo "< killing port forwarding ($KUBE_PID)"
#kill $KUBE_PID
