// Copyright (C) 2018 Don Kelly <karfai@gmail.com>
// Copyright (C) 2018 Hayk Pilosyan <hayk.pilos@gmail.com>

// This file is part of Interlibr, a functional component of an
// Internet of Rules (IoR).

// ACKNOWLEDGEMENTS
// Funds: Xalgorithms Foundation
// Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see
// <http://www.gnu.org/licenses/>.
lazy val VERSION_SCALA               = "2.11.11"
lazy val VERSION_SPARK               = "2.3.0"
lazy val VERSION_CASSANDRA_CONNECTOR = "2.0.3"
// NOTE: Upgrade (don't forget deployment scripts)
lazy val VERSION_MONGO_CONNECTOR     = "2.2.0"
lazy val VERSION_NSCALA_TIME         = "2.18.0"
lazy val VERSION_TYPESAFE_CONFIG     = "1.2.1"
lazy val VERSION_FICUS               = "1.1.1"
lazy val VERSION_PLAY_JSON           = "2.6.0"
lazy val VERSION_JACKSON_DATABIND    = "2.6.5"
lazy val VERSION_SCALA_TEST          = "3.0.5"
lazy val VERSION_SCALA_MOCK          = "4.1.0"

lazy val settings = Seq(
  name := "jobs-spark-il",
  version := "0.1.2",
  organization := "http://xalgorithms.org",
  scalaVersion := VERSION_SCALA
)

lazy val depOverrides = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind"       % VERSION_JACKSON_DATABIND
)

lazy val deps = Seq(
  "com.typesafe.play"      %% "play-json"                  % VERSION_PLAY_JSON,
  "org.apache.spark"       %% "spark-core"                 % VERSION_SPARK,
  "org.apache.spark"       %% "spark-streaming"            % VERSION_SPARK,
  "org.apache.spark"       %% "spark-streaming-kafka-0-10" % VERSION_SPARK,
  "org.apache.spark"       %% "spark-sql"                  % VERSION_SPARK,
  "com.datastax.spark"     %% "spark-cassandra-connector"  % VERSION_CASSANDRA_CONNECTOR,
  "org.mongodb.spark"      %% "mongo-spark-connector"      % VERSION_MONGO_CONNECTOR,
  "com.github.nscala-time" %% "nscala-time"                % VERSION_NSCALA_TIME,
  "com.typesafe"           %  "config"                     % VERSION_TYPESAFE_CONFIG,
  "net.ceedubs"            %% "ficus"                      % VERSION_FICUS,
  "org.scalactic"          %% "scalactic"                  % VERSION_SCALA_TEST,
  "org.scalatest"          %% "scalatest"                  % VERSION_SCALA_TEST % "test",
  "org.scalamock"          %% "scalamock"                  % VERSION_SCALA_MOCK % "test"
)

lazy val root = (project in file("."))
  .settings(settings)
  .settings(dependencyOverrides ++= depOverrides)
  .settings(libraryDependencies ++= deps)
