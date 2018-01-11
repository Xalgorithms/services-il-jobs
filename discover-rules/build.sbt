name := "discover-rules"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.18.0"