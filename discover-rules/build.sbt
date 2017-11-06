name := "Discover Rules Job"
version := "0.0.1"
scalaVersion := "2.11.11"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"