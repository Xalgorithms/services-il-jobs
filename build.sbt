lazy val VERSION_SCALA               = "2.11.11"
lazy val VERSION_SPARK               = "2.2.0"
lazy val VERSION_CASSANDRA_CONNECTOR = "2.0.3"
lazy val VERSION_MONGO_CONNECTOR     = "2.2.0"
lazy val VERSION_NSCALA_TIME         = "2.18.0"
lazy val VERSION_TYPESAFE_CONFIG     = "1.2.1"
lazy val VERSION_FICUS               = "1.1.1"
lazy val VERSION_PLAY_JSON           = "2.6.0"
lazy val VERSION_BETTER_FILES        = "3.4.0"
lazy val VERSION_JACKSON_DATABIND    = "2.6.5"
lazy val VERSION_SCALA_TEST          = "3.0.5"
lazy val VERSION_SCALA_MOCK          = "4.1.0"

lazy val settings = Seq(
  name := "xa-spark-jobs",
  version := "1.0",
  organization := "http://xalgorithms.org",
  scalaVersion := VERSION_SCALA
)

lazy val depOverrides = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind"       % VERSION_JACKSON_DATABIND
)

lazy val deps = Seq(
  "com.typesafe.play"      %% "play-json"                 % VERSION_PLAY_JSON,
  "org.apache.spark"       %% "spark-core"                % VERSION_SPARK,
  "org.apache.spark"       %% "spark-streaming"           % VERSION_SPARK,
  "org.apache.spark"       %% "spark-streaming-kafka-0-8" % VERSION_SPARK,
  "org.apache.spark"       %% "spark-sql"                 % VERSION_SPARK,
  "com.datastax.spark"     %% "spark-cassandra-connector" % VERSION_CASSANDRA_CONNECTOR,
  "org.mongodb.spark"      %% "mongo-spark-connector"     % VERSION_MONGO_CONNECTOR,
  "com.github.nscala-time" %% "nscala-time"               % VERSION_NSCALA_TIME,
  "com.typesafe"           %  "config"                    % VERSION_TYPESAFE_CONFIG,
  "net.ceedubs"            %% "ficus"                     % VERSION_FICUS,
  "org.scalactic"          %% "scalactic"                 % VERSION_SCALA_TEST,
  "com.github.pathikrit"   %% "better-files"              % VERSION_BETTER_FILES,
  "org.scalatest"          %% "scalatest"                 % VERSION_SCALA_TEST % "test",
  "org.scalamock"          %% "scalamock"                 % VERSION_SCALA_MOCK % "test"
)

lazy val root = (project in file("."))
  .settings(settings)
  .settings(dependencyOverrides ++= depOverrides)
  .settings(libraryDependencies ++= deps)
