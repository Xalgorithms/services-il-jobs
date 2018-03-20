package org.xalgorithms.apps

import java.lang.management.ManagementFactory

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import kafka.serializer.StringDecoder

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.bson.Document
import scala.collection.JavaConverters._


// The job might be run on one of the executors, hence it should be serializable
abstract class BaseApplication(cfg: ApplicationConfig) extends Serializable {
  type T

  def spark_cfg: Map[String, String] = cfg.spark
  def batch_duration: FiniteDuration = cfg.batch_duration
  def checkpoint_dir: String = cfg.checkpoint_dir

  def with_context(app_cfg: ApplicationConfig, fn: (SparkContext, StreamingContext, DStream[String]) => DStream[T]): Unit = {
    val isIDE = {
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }
    val cfg = new SparkConf()
    spark_cfg.foreach { case (n, v) => cfg.setIfMissing(n, v) }

    if (isIDE) {
      cfg.setMaster("local[*]")
    }

    val ctx = new SparkContext(cfg)
    val sctx = new StreamingContext(ctx, Seconds(batch_duration.toSeconds))
    val kafka_cfg = Map("bootstrap.servers" -> app_cfg.kafka("broker"))
    val source = KafkaSource(kafka_cfg)
    val input = source.create(sctx, app_cfg.topic_input)

    // FIXME: Spits out errors, when spark context is accessed from transform
    // sctx.checkpoint(checkpoint_dir)

    val output = fn(ctx, sctx, input)

    act(output, kafka_cfg, app_cfg.topic_output, ctx)

    sctx.start()
    sctx.awaitTermination()
  }

  def act(output: DStream[T], sink: Map[String, String], topic: String, ctx: SparkContext = null): Unit
}

class KafkaSparkStreamingApplication(cfg: ApplicationConfig) extends BaseApplication(cfg: ApplicationConfig) {
  type T = String

  def act(output: DStream[T], sink: Map[String, String], topic: String, ctx: SparkContext = null): Unit = {
    import KafkaSink._
    output.send(sink, topic)
  }
}

class KafkaMongoSparkStreamingApplication(cfg: ApplicationConfig) extends BaseApplication(cfg: ApplicationConfig) {
  type T = (String, String)

  def act(output: DStream[T], sink: Map[String, String], topic: String, ctx: SparkContext): Unit = {
    import KafkaMongoSink._
    output.send(sink, topic, ctx)
  }
}

import com.typesafe.config.Config

case class ApplicationConfig(
  topic_input: String,
  topic_output: String,
  kafka: Map[String, String],
  spark: Map[String, String],
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
      app_cfg.as[Map[String, String]]("kafka"),
      app_cfg.as[Map[String, String]]("spark"),
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

class KafkaSinkBase[T](@transient private val st: DStream[T]) extends Serializable {
  private val Producers = mutable.Map[Map[String, Object], KafkaProducer[String, String]]()

  def maybeMakeProducer(cfg: Map[String, Object]): KafkaProducer[String, String] = {
    val default_cfg = Map(
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val local_cfg = default_cfg ++ cfg

    Producers.getOrElseUpdate(
      local_cfg, {
        val pr = new KafkaProducer[String, String](local_cfg.asJava)
        sys.addShutdownHook {
          pr.close()
        }

        pr
      })
  }
}

class KafkaSink(@transient private val st: DStream[String]) extends KafkaSinkBase(st) {
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

object KafkaSink {
  import scala.language.implicitConversions

  implicit def createKafkaSink(st: DStream[String]): KafkaSink = {
    new KafkaSink(st)
  }
}

class KafkaMongoSink(@transient private val st: DStream[(String, String)]) extends KafkaSinkBase(st) {
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
