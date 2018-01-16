lazy val VERSION_SCALA               = "2.11.11"
lazy val VERSION_SPARK               = "2.2.0"
lazy val VERSION_CASSANDRA_CONNECTOR = "2.0.3"
lazy val VERSION_MONGO_CONNECTOR     = "2.2.0"
lazy val VERSION_NSCALA_TIME         = "2.18.0"
lazy val VERSION_TYPESAFE_CONFIG     = "1.2.1"
lazy val VERSION_FICUS               = "1.1.1"

lazy val settings = Seq(
  name := "xa-spark-jobs",
  version := "1.0",
  organization := "http://xalgorithms.org",
  scalaVersion := VERSION_SCALA
)
lazy val deps = Seq(
  "org.apache.spark"       %% "spark-core"                % VERSION_SPARK,
  "org.apache.spark"       %% "spark-streaming"           % VERSION_SPARK,
  "org.apache.spark"       %% "spark-streaming-kafka-0-8" % VERSION_SPARK,
  "org.apache.spark"       %% "spark-sql"                 % VERSION_SPARK,
  "com.datastax.spark"     %% "spark-cassandra-connector" % VERSION_CASSANDRA_CONNECTOR,
  "org.mongodb.spark"      %% "mongo-spark-connector"     % VERSION_MONGO_CONNECTOR,
  "com.github.nscala-time" %% "nscala-time"               % VERSION_NSCALA_TIME,
  "com.typesafe"           %  "config"                    % VERSION_TYPESAFE_CONFIG,
  "net.ceedubs"            %% "ficus"                     % VERSION_FICUS,
)

lazy val root = (project in file("."))
  .settings(settings)
  .settings(libraryDependencies ++= deps)
