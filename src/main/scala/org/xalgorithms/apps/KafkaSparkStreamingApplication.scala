package org.xalgorithms.apps

import java.lang.management.ManagementFactory

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.bson.Document
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

// The job might be run on one of the executors, hence it should be serializable
abstract class KafkaStreamingApplication(cfg: ApplicationConfig) extends Serializable {
  type T

  def batch_duration: FiniteDuration = cfg.batch_duration
  def checkpoint_dir: String = cfg.checkpoint_dir

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
    val kafka_cfg = Map("bootstrap.servers" -> cfg.get("spark.kafka.bootstrap.servers"))
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

class KafkaSparkStreamingApplication(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg: ApplicationConfig) {
  type T = String

  def act(output: DStream[T], cfg: Map[String, String], topic: String, ctx: SparkContext = null): Unit = {
    import KafkaStreamSink._
    output.send(cfg, topic)
  }
}

class KafkaMongoSparkStreamingApplication(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg: ApplicationConfig) {
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
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ctx, cfg, Set(topic)).map(_._2)
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
