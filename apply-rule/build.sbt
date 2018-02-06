name := "apply-rule"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.0"
