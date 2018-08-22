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
package org.xalgorithms.apps

import java.lang.management.ManagementFactory

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.bson.Document
import scala.collection.JavaConverters._

import scala.concurrent.duration.FiniteDuration
// The job might be run on one of the executors, hence it should be serializable
abstract class KafkaStreamingApplication(cfg: ApplicationConfig) extends Serializable {
  type T

  def batch_duration: FiniteDuration = cfg.batch_duration
  def checkpoint_dir: String = cfg.checkpoint_dir

  implicit val job_name: String

  def with_context(app_cfg: ApplicationConfig, fn: (SparkContext, StreamingContext, Broadcast[KafkaEventLog], DStream[String]) => DStream[T]): Unit = {
    val isIDE = {
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }
    val cfg = new SparkConf()

    if (isIDE) {
      cfg.setMaster("local[*]")
    }

    val ctx = new SparkContext(cfg)
    val sctx = new StreamingContext(ctx, Seconds(batch_duration.toSeconds))

    // we have to manually copy the Kafka config
    val kafka_cfg = Map[String, String](
      "bootstrap.servers" -> cfg.get("spark.kafka.bootstrap.servers"),
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> s"il-spark-jobs (${job_name})",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )
    val source = KafkaSource(kafka_cfg)
    val input = source.create(sctx, app_cfg.topic_input)

    val events = ctx.broadcast(KafkaEventLog(kafka_cfg))

    // FIXME: Spits out errors, when spark context is accessed from transform
    // sctx.checkpoint(checkpoint_dir)

    val output = fn(ctx, sctx, events, input)

    act(output, kafka_cfg, app_cfg.topic_output, ctx)

    sctx.start()
    sctx.awaitTermination()
  }

  def act(output: DStream[T], cfg: Map[String, String], topic: String, ctx: SparkContext = null): Unit
}

abstract class KafkaSparkStreamingApplication(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg: ApplicationConfig) {
  type T = String

  def act(output: DStream[T], cfg: Map[String, String], topic: String, ctx: SparkContext = null): Unit = {
    import KafkaStreamSink._
    output.send(cfg, topic)
  }
}

abstract class KafkaMongoSparkStreamingApplication(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg: ApplicationConfig) {
  type T = (String, String)

  def act(output: DStream[T], cfg: Map[String, String], topic: String, ctx: SparkContext): Unit = {
    import KafkaMongoSink._
    output.send(cfg, topic, ctx)
  }
}

import com.typesafe.config.Config

case class ApplicationConfig(
  topic_input: String,
  topic_output: String,
  batch_duration: FiniteDuration,
  checkpoint_dir: String,
  job: Config
) extends Serializable

object ApplicationConfig {
  import com.typesafe.config.ConfigFactory
  import net.ceedubs.ficus.Ficus._

  def apply(name: String): ApplicationConfig = apply(name, ConfigFactory.load)

  def apply(name: String, all_cfg: Config): ApplicationConfig = {
    val app_cfg = all_cfg.getConfig(s"$name.application")
    new ApplicationConfig(
      app_cfg.as[String]("topics.input"),
      app_cfg.as[String]("topics.output"),
      app_cfg.as[FiniteDuration]("batch_duration"),
      app_cfg.as[String]("checkpoint_dir"),
      all_cfg.getConfig(s"$name.job")
    )
  }
}

class KafkaSource(cfg: Map[String, String]) {
  def create(ctx: StreamingContext, topic: String): DStream[String] = {
    KafkaUtils.createDirectStream[String, String](
      ctx,
      PreferConsistent,
      Subscribe[String, String](Array(topic), cfg)).map(rec => rec.value)
  }
}

object KafkaSource {
  def apply(cfg: Map[String, String]): KafkaSource = new KafkaSource(cfg)
}

class KafkaStreamSink(@transient private val st: DStream[String]) extends KafkaSink {
  def send(cfg: Map[String, String], topic: String): Unit = {
    st.foreachRDD { rdd =>
      rdd.foreachPartition { recs =>
        val pr = maybeMakeProducer(cfg)

        val meta = recs.map { rec =>
          // since we sling DStream[String] and Producer[String,
          // String], then our records are Strings
          pr.send(new ProducerRecord(topic, rec))
        }.toList

        meta.foreach { d => d.get() }
      }
    }
  }
}

object KafkaStreamSink {
  import scala.language.implicitConversions

  implicit def createKafkaStreamSink(st: DStream[String]): KafkaStreamSink = {
    new KafkaStreamSink(st)
  }
}

class KafkaMongoSink(@transient private val st: DStream[(String, String)]) extends KafkaSink {
  def send(cfg: Map[String, String], topic: String, ctx: SparkContext): Unit = {
    val writeConfig = WriteConfig(Map("collection" -> "revision", "writeConcern.w" -> "majority", "replaceDocument" -> "false"), Some(WriteConfig(ctx)))
    val mongoConnector = MongoConnector(writeConfig.asOptions)

    st.foreachRDD { rdd =>
      rdd.foreachPartition (recs => if(recs.nonEmpty) {
        val pr = maybeMakeProducer(cfg)

        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          recs.grouped(writeConfig.maxBatchSize).foreach({ batch =>
            collection.insertMany(batch.toList.map({rec =>
              val doc = Document.parse(rec._1)
              pr.send(new ProducerRecord(topic, doc.getString("public_id")))
              doc
            }).asJava)
          })
        })
      })
    }
  }
}

object KafkaMongoSink {
  import scala.language.implicitConversions

  implicit def createKafkaMongoSink(st: DStream[(String, String)]): KafkaMongoSink = {
    new KafkaMongoSink(st)
  }
}
